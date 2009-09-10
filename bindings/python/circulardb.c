#include "Python.h"
#include "structmember.h"
// #include "datetime.h"

#include <circulardb.h>
#include <fcntl.h>

#define PY_CDB_WRITE  0
#define PY_CDB_UPDATE 1

#ifndef O_BINARY
#define O_BINARY 0
#endif

static PyTypeObject StorageType;
static PyTypeObject StatisticsType;
static PyTypeObject AggregateType;

typedef struct {
    PyObject_HEAD
    PyObject   *header;
    PyObject   *statistics;
    cdb_t      *cdb;
} StorageObject;

typedef struct {
    PyObject_HEAD
    cdb_range_t *range;
} StatisticsObject;

// Forward.
static void Statistics_init(PyObject*);

/* Helper functions for dealing with CDB Types */
int _cdb_type_from_string(char *type) {
  if (strcmp(type, "gauge") == 0) {
    return CDB_TYPE_GAUGE;
  } else if (strcmp(type, "counter") == 0) {
    return CDB_TYPE_COUNTER;
  }

  return CDB_DEFAULT_DATA_TYPE;
}

const char* _string_from_cdb_type(int type) {
  switch (type) {
    case CDB_TYPE_GAUGE  : return "gauge";
    case CDB_TYPE_COUNTER: return "counter";
    default: return "gauge";
  }
}

void _check_return(int ret) {
  switch (ret) {
    case CDB_SUCCESS : break;
    case CDB_ETMRANGE: PyErr_SetString(PyExc_TypeError, "End time must be >= Start time.");
    case CDB_ESANITY : PyErr_SetString(PyExc_StandardError, "Database is unsynced.");
    case CDB_ENOMEM  : PyErr_SetString(PyExc_MemoryError, "No memory could be allocated.");
    case CDB_ENORECS : PyErr_SetString(PyExc_RuntimeError, "There were no records in the database to be read.");
    case CDB_EINTERPD: PyErr_SetString(PyExc_RuntimeError, "Aggregate driver issue. Possibly no records!");
    case CDB_EINTERPF: PyErr_SetString(PyExc_RuntimeError, "Aggregate follower issue. Possibly no records!");
    default          : PyErr_SetString(PyExc_SystemError, "Unknown Error");
  }
}

cdb_range_t* _new_statistics(PyObject* self) {

  /* Store the range statistics as an ivar. This overwrites any previous
   * value. Statistics are good only for the last read. */
  PyObject* statistics = ((StorageObject*)self)->statistics;

  // Release the old object. There should be a better way of doing this
  if (statistics != Py_None) {
    PyObject_Del(statistics);
  }

  statistics = PyObject_New(PyObject, &StatisticsType);
  Statistics_init(statistics); // Calls calloc() on range member.

  ((StorageObject*)self)->statistics = statistics;

  memset(((StatisticsObject*)statistics)->range, 0, sizeof(cdb_range_t));

  return ((StatisticsObject*)statistics)->range;
}

void _cdb_py_update_header(PyObject* self, cdb_t *cdb) {

  PyObject *header = ((StorageObject*)self)->header;

  /* Update the number of records in the header dict */
  PyDict_SetItemString(header, "num_records", PyInt_FromLong(cdb->header->num_records));
}

static cdb_time_t _parse_time(PyObject* time) {
  return (cdb_time_t)PyInt_AsUnsignedLongMask(time);
}

static void Storage_dealloc(PyObject *self) {
  Py_XDECREF(((StorageObject*)self)->statistics);
  Py_XDECREF(((StorageObject*)self)->header);
  cdb_free(((StorageObject*)self)->cdb);
  self->ob_type->tp_free(self);
}

static PyObject* Storage_new(PyTypeObject *type, PyObject *args, PyObject *kwdict) {

  StorageObject* self = (StorageObject *)type->tp_alloc(type, 0);

  if (self != NULL) {
    self->cdb = cdb_new();

    if (self->cdb == NULL) {
      Py_DECREF(self);
      return NULL;
    }

    self->header = PyDict_New();

    if (self->header == NULL) {
      Py_DECREF(self);
      return NULL;
    }

    self->statistics = Py_None;
  }

  return (PyObject *)self;
}

static int Storage_init(PyObject *self, PyObject *args, PyObject *kwdict) {

  static char *kwlist[] = {
    "filename", "flags", "mode", "name", "desc", "max_records", "type", "units", "min_value", "max_value", "interval", NULL,
  };

  char *filename;
  int flags            = -1;
  int mode             = -1;
  char *name           = NULL;
  char *desc           = "";
  char *type           = (char*)_string_from_cdb_type(CDB_DEFAULT_DATA_TYPE);
  char *units          = CDB_DEFAULT_DATA_UNIT;
  uint64_t max_records = 0;
  int interval         = 0;
  double min_value     = 0;
  double max_value     = 0;

  if (!PyArg_ParseTupleAndKeywords(args, kwdict, "s|iiszlzzlli:new", kwlist,
    &filename, &flags, &mode, &name, &desc, &max_records, &type, &units, &min_value, &max_value, &interval)) {
    return -1;
  }

  PyObject *header = ((StorageObject*)self)->header;
  cdb_t *cdb       = ((StorageObject*)self)->cdb;

  cdb->filename = filename;
  cdb->flags    = flags;
  cdb->mode     = mode;

  // Try the open
  if (cdb_open(cdb) != CDB_SUCCESS) {
    PyErr_SetFromErrno(PyExc_IOError);
    return -1;
  }

  // Try and read a header, if it doesn't exist, create one
  if (name == NULL) {

    if (cdb_read_header(cdb) != CDB_SUCCESS) {
      PyErr_SetFromErrno(PyExc_IOError);
      return -1;
    }

  } else {

    cdb_generate_header(cdb, name, desc, max_records, _cdb_type_from_string(type), units, min_value, max_value, interval);

    if (cdb_write_header(cdb) != CDB_SUCCESS) {
      PyErr_SetFromErrno(PyExc_IOError);
      return -1;
    }
  }

  PyDict_SetItemString(header, "filename", PyString_FromString(filename));
  PyDict_SetItemString(header, "name", PyString_FromString(cdb->header->name));
  PyDict_SetItemString(header, "desc", PyString_FromString(cdb->header->desc));
  PyDict_SetItemString(header, "type", PyString_FromString(_string_from_cdb_type(cdb->header->type)));
  PyDict_SetItemString(header, "units", PyString_FromString(cdb->header->units));
  PyDict_SetItemString(header, "num_records", PyInt_FromLong(cdb->header->num_records));
  PyDict_SetItemString(header, "max_records", PyInt_FromLong(cdb->header->max_records));
  PyDict_SetItemString(header, "min_value", PyInt_FromLong(cdb->header->min_value));
  PyDict_SetItemString(header, "max_value", PyInt_FromLong(cdb->header->max_value));
  PyDict_SetItemString(header, "interval", PyInt_FromLong(cdb->header->interval));

  return 0;
}

static PyObject* Storage_header_get(PyObject *self, void *closure) {
  return PyDict_GetItemString(((StorageObject*)self)->header, (char*)closure);
}

static int Storage_header_set(PyObject *self, PyObject *value, void *closure) {

  PyObject *header = ((StorageObject*)self)->header;
  cdb_t *cdb       = ((StorageObject*)self)->cdb;
  char* key        = (char*)closure;

  if (strcmp(key, "name") == 0) {

    PyDict_SetItemString(header, key, value);
    strncpy(cdb->header->name, PyString_AsString(value), sizeof(cdb->header->name));
    cdb->synced = false;

  } else if (strcmp(key, "min_value") == 0) {

    PyDict_SetItemString(header, key, value);
    cdb->header->min_value = PyInt_AsUnsignedLongLongMask(value);
    cdb->synced = false;

  } else if (strcmp(key, "max_value") == 0) {

    PyDict_SetItemString(header, key, value);
    cdb->header->max_value = PyInt_AsUnsignedLongLongMask(value);
    cdb->synced = false;

  } else if (strcmp(key, "interval") == 0) {

    PyDict_SetItemString(header, key, value);
    cdb->header->interval = PyInt_AsUnsignedLongLongMask(value);
    cdb->synced = false;

  } else if (strcmp(key, "units") == 0) {

    PyDict_SetItemString(header, key, value);
    strncpy(cdb->header->units, PyString_AsString(value), sizeof(cdb->header->units));
    cdb->synced = false;
  }

  return 0;
}

cdb_request_t _parse_cdb_request(cdb_time_t start, cdb_time_t end, int64_t count, int cooked, long step) {

  // cooked defaults to true from cdb_new_request()
  cdb_request_t request = cdb_new_request();

  request.cooked = (bool)cooked;
  request.start  = start;
  request.end    = end;
  request.count  = count;
  request.step   = step;

  return request;
}

static PyObject* cdb_py_read_records(PyObject *self, PyObject *args, PyObject *kwdict) {
  static char *kwlist[] = {
    "start", "end", "count", "cooked", "step",  NULL,
  };

  cdb_time_t start = 0;
  cdb_time_t end   = 0;
  int ret          = 0;
  int cooked       = 1;
  long step        = 0;
  int64_t count    = 0;
  uint64_t i       = 0;
  uint64_t cnt     = 0;

  if (!PyArg_ParseTupleAndKeywords(args, kwdict, "|LLLil:read_records", kwlist, &start, &end, &count, &cooked, &step)) {
    return NULL;
  }

  cdb_t *cdb            = ((StorageObject*)self)->cdb;
  cdb_range_t *range    = _new_statistics(self);
  cdb_record_t *records = NULL;

  cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

  ret = cdb_read_records(cdb, &request, &cnt, &records, range);

  _check_return(ret);

  PyObject *list = PyList_New(cnt);

  for (i = 0; i < cnt; i++) {
    PyObject* record = PyList_New(2);
    PyList_SetItem(record, 0, PyInt_FromLong(records[i].time));
    PyList_SetItem(record, 1, PyFloat_FromDouble(records[i].value));
    PyList_SetItem(list, i, record);
  }

  free(records);

  _cdb_py_update_header(self, cdb);

  return list;
}

// Helper functions for write_* and update_* since they do almost the same thing.
static PyObject* _cdb_write_or_update_records(PyObject* self, PyObject* list, int type) {

  int len = PyList_Size(list);
  uint64_t i   = 0;
  uint64_t cnt = 0;
  bool     ret = false;

  cdb_t *cdb            = ((StorageObject*)self)->cdb;
  cdb_record_t *records = alloca(sizeof(cdb_record_t) * len);

  // Turn any None's into NaN
  for (i = 0; i < len; i++) {
    PyObject* record = PyList_GetItem(list, i);
    PyObject* value  = PyList_GetItem(record, 1);

    records[i].time  = _parse_time(PyList_GetItem(record, 0));
    records[i].value = (value == Py_None ? CDB_NAN : PyFloat_AsDouble(value));
  }

  if (type == PY_CDB_WRITE) {
    ret = cdb_write_records(cdb, records, len, &cnt);
  } else {
    ret = cdb_update_records(cdb, records, len, &cnt);
  }

  if (ret != CDB_SUCCESS) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }

  _cdb_py_update_header(self, cdb);

  return PyInt_FromLong(cnt);
}

static PyObject* _cdb_write_or_update_record(PyObject* self, PyObject* time, PyObject* value, int type) {
  bool ret = false;

  cdb_t *cdb = ((StorageObject*)self)->cdb;

  // Convert None to NAN, which is what the circulardb code expects
  if (value == Py_None) {
      value = PyFloat_FromDouble(CDB_NAN);
  }

  if (type == PY_CDB_WRITE) {
    ret = cdb_write_record(cdb, _parse_time(time), PyFloat_AsDouble(value));
  } else {
    ret = cdb_update_record(cdb, _parse_time(time), PyFloat_AsDouble(value));
  }

  if (ret == false) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }

  _cdb_py_update_header(self, cdb);

  return PyInt_FromLong(ret);
}

static PyObject* cdb_py_write_records(PyObject* self, PyObject* args) {
  PyObject *list;

  if (!PyArg_ParseTuple(args, "O:write_records", &list)) {
    return NULL;
  }

  return _cdb_write_or_update_records(self, list, PY_CDB_WRITE);
}

static PyObject* cdb_py_write_record(PyObject* self, PyObject* args) {
  PyObject *time, *value;

  if (!PyArg_ParseTuple(args, "OO:write_record", &time, &value)) {
    return NULL;
  }

  return _cdb_write_or_update_record(self, time, value, PY_CDB_WRITE);
}

static PyObject* cdb_py_update_records(PyObject* self, PyObject* args) {
  PyObject *list;

  if (!PyArg_ParseTuple(args, "O:update_records", &list)) {
    return NULL;
  }

  return _cdb_write_or_update_records(self, list, PY_CDB_UPDATE);
}

static PyObject* cdb_py_update_record(PyObject* self, PyObject* args) {
  PyObject *time, *value;

  if (!PyArg_ParseTuple(args, "OO:update_record", &time, &value)) {
    return NULL;
  }

  return _cdb_write_or_update_record(self, time, value, PY_CDB_UPDATE);
}

static PyObject* cdb_py_discard_records_in_time_range(PyObject *self, PyObject *args, PyObject *kwdict) {
  static char *kwlist[] = {
    "start", "end", NULL,
  };

  cdb_time_t start, end;
  uint64_t cnt = 0;

  if (!PyArg_ParseTupleAndKeywords(args, kwdict, "LL:discard_records_in_time_range", kwlist, &start, &end)) {
    return NULL;
  }

  cdb_t *cdb = ((StorageObject*)self)->cdb;

  cdb_request_t request = _parse_cdb_request(start, end, 0, 0, 0);

  int ret = cdb_discard_records_in_time_range(cdb, &request, &cnt);

  _check_return(ret);

  return PyInt_FromLong(cnt);
}

static PyObject* cdb_py_open_cdb(PyObject* self) {

  cdb_t *cdb = ((StorageObject*)self)->cdb;

  if (cdb_open(cdb) != CDB_SUCCESS) {
    PyErr_SetFromErrno(PyExc_IOError);
  }

  return self;
}

static PyObject* cdb_py_close_cdb(PyObject* self) {

  cdb_t *cdb = ((StorageObject*)self)->cdb;

  if (cdb_close(cdb) != CDB_SUCCESS) {
    PyErr_SetFromErrno(PyExc_IOError);
  }

  return self;
}

static PyObject* cdb_py_read_header(PyObject* self) {

  cdb_t *cdb = ((StorageObject*)self)->cdb;

  if (cdb_read_header(cdb) != CDB_SUCCESS) {
    PyErr_SetFromErrno(PyExc_IOError);
  }

  _cdb_py_update_header(self, cdb);

  return self;
}

static PyObject* cdb_py_write_header(PyObject* self) {

  cdb_t *cdb = ((StorageObject*)self)->cdb;

  if (cdb_write_header(cdb) != CDB_SUCCESS) {
    PyErr_SetFromErrno(PyExc_IOError);
  }

  return self;
}

static PyObject* cdb_py_print(PyObject* self) {

  cdb_t *cdb = ((StorageObject*)self)->cdb;
  cdb_print(cdb);
  return self;
}

static PyObject* cdb_py_print_header(PyObject* self) {

  cdb_t *cdb = ((StorageObject*)self)->cdb;
  cdb_print_header(cdb);
  return self;
}

static PyObject* cdb_py_print_records(PyObject *self, PyObject *args, PyObject *kwdict) {
  static char *kwlist[] = {
    "start", "end", "count", "cooked", "step", "file_obj", "date_format", NULL,
  };

  cdb_time_t start = 0;
  cdb_time_t end   = 0;
  int cooked       = 1;
  long step        = 0;
  int64_t count    = 0;
  cdb_t *cdb;
  PyObject *file_obj, *date_format;

  if (!PyArg_ParseTupleAndKeywords(args, kwdict, "|LLLOzil:print_records", kwlist, &start, &end, &count, &file_obj, &date_format, &cooked, &step)) {
    return NULL;
  }

  cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

  if (Py_None == date_format) {
    date_format = PyString_FromString("");
  }

  if (Py_None == file_obj || !PyFile_Check(file_obj)) {
    file_obj = PyString_FromString("stderr");
  }

  cdb = ((StorageObject*)self)->cdb;

  cdb_print_records(cdb, &request, PyFile_AsFile(file_obj), PyString_AsString(date_format));

  return self;
}

static PyObject* cdb_py_statistics(PyObject *self, PyObject *args, PyObject *kwdict) {
  PyObject* statistics = ((StorageObject*)self)->statistics;

  // Do a read of the full range to populate
  if (statistics == Py_None) {
    cdb_py_read_records(self, args, kwdict);
    statistics = ((StorageObject*)self)->statistics;
  }

  return statistics;
}

/*
// Aggregate functions
static PyObject* cdb_agg_rb_initialize(PyObject* self, PyObject* name) {

  rb_iv_set(self, "@name", name);
  rb_iv_set(self, "@cdbs", rb_ary_new());
  rb_iv_set(self, "@statistics", Py_None);

  return self;
}

static PyObject* cdb_agg_rb_read_records(int argc, PyObject* *argv, PyObject* self) {

  PyObject* start, end, count, cooked, step, list;
  PyObject* cdb_objects = rb_iv_get(self, "@cdbs");

  uint64_t i   = 0;
  uint64_t cnt = 0;
  int ret      = 0;
  int num_cdbs = RARRAY(cdb_objects)->len;

  // initialize the cdbs list to the size of the cdb_objects list
  cdb_t *cdbs[num_cdbs];
  cdb_record_t *records = NULL;
  cdb_range_t *range    = _new_statistics(self);

  rb_scan_args(argc, argv, "05", &start, &end, &count, &cooked, &step);

  cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

  // First, loop over the incoming list of CircularDB::Storage objects and
  // extract the pointers to the cdb_t structs.
  for (i = 0; i < num_cdbs; i++) {
      Data_Get_Struct(RARRAY(cdb_objects)->ptr[i], cdb_t, cdbs[i]);
  }

  ret = cdb_read_aggregate_records(cdbs, num_cdbs, &request, &cnt, &records, range);

  _check_return(ret);

  list = rb_ary_new2(cnt);

  for (i = 0; i < cnt; i++) {
      rb_ary_store(list, i, rb_ary_new3(2, ULONG2NUM(records[i].time), rb_float_new(records[i].value)));
  }

  free(records);

  return list;
}

static PyObject* cdb_agg_rb_print_records(int argc, PyObject* *argv, PyObject* self) {

  PyObject* start, end, count, file_obj, date_format, cooked, step;
  PyObject* cdb_objects = rb_iv_get(self, "@cdbs");

  uint64_t i   = 0;
  int num_cdbs = RARRAY(cdb_objects)->len;

  // initialize the cdbs list to the size of the cdb_objects list
  cdb_t *cdbs[num_cdbs];

  rb_scan_args(argc, argv, "07", &start, &end, &count, &file_obj, &date_format, &cooked, &step);

  cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

  if (Py_None == date_format)) {
      date_format = rb_str_new2("");
  }

  if (Py_None == file_obj)) {
      file_obj = rb_const_get(cStorage, rb_intern("STDERR"));
  }

  // First, loop over the incoming list of CircularDB::Storage objects and
  // extract the pointers to the cdb_t structs.
  for (i = 0; i < num_cdbs; i++) {
      Data_Get_Struct(RARRAY(cdb_objects)->ptr[i], cdb_t, cdbs[i]);
  }

  cdb_print_aggregate_records(cdbs, num_cdbs, &request, RFILE(file_obj)->fptr->f, StringValuePtr(date_format));

  return self;
}

static PyObject* cdb_agg_rb_statistics(int argc, PyObject* *argv, PyObject* self) {
  PyObject* statistics = rb_iv_get(self, "@statistics");

  // Do a read of the full range to populate
  if (statistics == Py_None) {
      cdb_agg_rb_read_records(argc, argv, self);
      statistics = rb_iv_get(self, "@statistics");
  }

  return statistics;
}
*/

// Statistics functions
static void Statistics_dealloc(PyObject *self) {
  free(((StatisticsObject*)self)->range);
  self->ob_type->tp_free(self);
}

// This should never called by Python, only from Storage & Aggregate objects here.
static void Statistics_init(PyObject *self) {
  ((StatisticsObject*)self)->range = calloc(1, sizeof(cdb_range_t));
}

static PyObject* Statistics_get(PyObject *self, void *closure) {
  return PyFloat_FromDouble(cdb_get_statistic(((StatisticsObject*)self)->range, (cdb_statistics_enum_t)closure));
}

/*
// Ruby Class / Method glue

void Init_circulardb_ext() {

  // CircularDB::Aggregate class
  rb_define_method(cAggregate, "initialize", cdb_agg_rb_initialize, 1);
  rb_define_method(cAggregate, "read_records", cdb_agg_rb_read_records, -1);
  rb_define_method(cAggregate, "print_records", cdb_agg_rb_print_records, -1);
  rb_define_method(cAggregate, "statistics", cdb_agg_rb_statistics, -1);
}
*/

static PyMemberDef Storage_members[] = {
  { "header", T_OBJECT, offsetof(StorageObject, header), 0, "The CircularDB header." },
  { NULL }
};

static PyMethodDef Storage_methods[] = {
  { "open",           (PyCFunction)cdb_py_open_cdb, 0, "" },
  { "close",          (PyCFunction)cdb_py_close_cdb, 0, "" },
  { "read_header",    (PyCFunction)cdb_py_read_header, 0, "" },
  { "write_header",   (PyCFunction)cdb_py_write_header, 0, "" },
  { "read_records",   (PyCFunction)cdb_py_read_records, METH_VARARGS|METH_KEYWORDS, PyDoc_STR("Read records from a CircularDB file.") },
  { "write_records",  (PyCFunction)cdb_py_write_records, METH_VARARGS, "" },
  { "write_record",   (PyCFunction)cdb_py_write_record, METH_VARARGS, "" },
  { "update_records", (PyCFunction)cdb_py_update_records, METH_VARARGS, "" },
  { "update_record",  (PyCFunction)cdb_py_update_record, METH_VARARGS, "" },
  { "print",          (PyCFunction)cdb_py_print, 0, "" },
  { "print_header",   (PyCFunction)cdb_py_print_header, 0, "" },
  { "print_records",  (PyCFunction)cdb_py_print_records, METH_VARARGS|METH_KEYWORDS, "" },
  { "discard",        (PyCFunction)cdb_py_discard_records_in_time_range, METH_VARARGS, "" },
  { "statistics",     (PyCFunction)cdb_py_statistics, 0, "" },
  { NULL }
};

static PyGetSetDef Storage_getseters[] = {
  { "filename",    (getter)Storage_header_get, 0,                          "Filename", (void*)"filename" },
  { "num_records", (getter)Storage_header_get, 0,                          "Current number of records", (void*)"num_records" },
  { "max_records", (getter)Storage_header_get, (setter)Storage_header_set, "Max records before rollover", (void*)"max_records" },
  { "max_value",   (getter)Storage_header_get, (setter)Storage_header_set, "Max CDB value", (void*)"max_value" },
  { "min_value",   (getter)Storage_header_get, (setter)Storage_header_set, "Min CDB value", (void*)"min_value" },
  { "interval",    (getter)Storage_header_get, (setter)Storage_header_set, "Interval between updates", (void*)"interval" },
  { "name",        (getter)Storage_header_get, (setter)Storage_header_set, "Name", (void*)"name" },
  { "desc",        (getter)Storage_header_get, (setter)Storage_header_set, "Description", (void*)"desc" },
  { "units",       (getter)Storage_header_get, (setter)Storage_header_set, "Record Units", (void*)"units" },
  { "type",        (getter)Storage_header_get, (setter)Storage_header_set, "Type: Gauge or Counter", (void*)"type" },
  { NULL }  /* Sentinel */
};

static PyTypeObject StorageType = {
    PyObject_HEAD_INIT(NULL)
    0,                  /*ob_size*/
    "circulardb.Storage",    /*tp_name*/
    sizeof(StorageObject),  /*tp_basicsize*/
    0,                  /*tp_itemsize*/
    /* methods */
    Storage_dealloc,    /*tp_dealloc*/
    0,                  /*tp_print*/
    0,                  /*tp_getattr*/
    0,                  /*tp_setattr*/
    0,                  /*tp_compare*/
    0,                  /*tp_repr*/
    0,                  /*tp_as_number*/
    0,                  /*tp_as_sequence*/
    0,                  /*tp_as_mapping*/
    0,                  /*tp_hash*/
    0,                  /*tp_call*/
    0,                  /*tp_str*/
    0,                  /*tp_getattro*/
    0,                  /*tp_setattro*/
    0,                  /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    0,                  /*tp_doc*/
    0,                  /*tp_traverse*/
    0,                  /*tp_clear*/
    0,                  /*tp_richcompare*/
    0,                  /*tp_weaklistoffset*/
    0,                  /*tp_iter*/
    0,                  /*tp_iternext*/
    Storage_methods,    /* tp_methods */
    Storage_members,    /* tp_members */
    Storage_getseters,  /* tp_getset */
    0,                  /* tp_base */
    0,                  /* tp_dict */
    0,                  /* tp_descr_get */
    0,                  /* tp_descr_set */
    0,                  /* tp_dictoffset */
    (initproc)Storage_init, /* tp_init */
    0,                  /* tp_alloc */
    Storage_new,        /* tp_new */
};

static PyGetSetDef Statistics_getseters[] = {
  { "median",  (getter)Statistics_get, 0,  "median",  (void*)CDB_MEDIAN },
  { "mad",     (getter)Statistics_get, 0,  "mad",     (void*)CDB_MAD },
  { "average", (getter)Statistics_get, 0,  "average", (void*)CDB_MEAN },
  { "mean",    (getter)Statistics_get, 0,  "mean",    (void*)CDB_MEAN },
  { "sum",     (getter)Statistics_get, 0,  "sum",     (void*)CDB_SUM },
  { "min",     (getter)Statistics_get, 0,  "min",     (void*)CDB_MIN },
  { "max",     (getter)Statistics_get, 0,  "max",     (void*)CDB_MAX },
  { "stddev",  (getter)Statistics_get, 0,  "stddev",  (void*)CDB_STDDEV },
  { "absdev",  (getter)Statistics_get, 0,  "absdev",  (void*)CDB_ABSDEV },
  { "pct95th", (getter)Statistics_get, 0,  "pct95th", (void*)CDB_95TH },
  { "pct75th", (getter)Statistics_get, 0,  "pct75th", (void*)CDB_75TH },
  { "pct50th", (getter)Statistics_get, 0,  "pct50th", (void*)CDB_50TH },
  { "pct25th", (getter)Statistics_get, 0,  "pct25th", (void*)CDB_25TH },
  { NULL }  /* Sentinel */
};

static PyTypeObject StatisticsType = {
    PyObject_HEAD_INIT(NULL)
    0,                  /*ob_size*/
    "circulardb.Statistics",    /*tp_name*/
    sizeof(StatisticsObject),  /*tp_basicsize*/
    0,                  /*tp_itemsize*/
    /* methods */
    Statistics_dealloc,    /*tp_dealloc*/
    0,                  /*tp_print*/
    0,                  /*tp_getattr*/
    0,                  /*tp_setattr*/
    0,                  /*tp_compare*/
    0,                  /*tp_repr*/
    0,                  /*tp_as_number*/
    0,                  /*tp_as_sequence*/
    0,                  /*tp_as_mapping*/
    0,                  /*tp_hash*/
    0,                  /*tp_call*/
    0,                  /*tp_str*/
    0,                  /*tp_getattro*/
    0,                  /*tp_setattro*/
    0,                  /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    0,                  /*tp_doc*/
    0,                  /*tp_traverse*/
    0,                  /*tp_clear*/
    0,                  /*tp_richcompare*/
    0,                  /*tp_weaklistoffset*/
    0,                  /*tp_iter*/
    0,                  /*tp_iternext*/
    0,                  /* tp_methods */
    0,                  /* tp_members */
    Statistics_getseters,  /* tp_getset */
    0,                  /* tp_base */
    0,                  /* tp_dict */
    0,                  /* tp_descr_get */
    0,                  /* tp_descr_set */
    0,                  /* tp_dictoffset */
    0,                  /* tp_init */
    0,                  /* tp_alloc */
    0,                  /* tp_new */
};

/* Initialize this module. */
PyMODINIT_FUNC
initcirculardb(void)
{
  PyObject *mod;

  if (PyType_Ready(&StorageType) < 0)
    return;

  StatisticsType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&StatisticsType) < 0)
    return;

  AggregateType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&AggregateType) < 0)
    return;

  // Create the module.
  mod = Py_InitModule3("circulardb", NULL, "CircularDB Library Extension");

  if (mod == NULL)
    return;

  // Add the type to the module.
  Py_INCREF(&StorageType);
  Py_INCREF(&StatisticsType);
  Py_INCREF(&AggregateType);
  PyModule_AddObject(mod, "Storage", (PyObject *)&StorageType);
  PyModule_AddObject(mod, "Statistics", (PyObject *)&StatisticsType);
  PyModule_AddObject(mod, "Aggregate", (PyObject *)&AggregateType);
}

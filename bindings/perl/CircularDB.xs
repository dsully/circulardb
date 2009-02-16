#ifdef __cplusplus
"C" {
#endif
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#ifdef __cplusplus
}
#endif

#include "ppport.h"

#include <circulardb.h>

/* strlen the length automatically */
#define my_hv_store(a,b,c)   hv_store(a,b,strlen(b),c,0)
#define my_hv_fetch(a,b)     hv_fetch(a,b,strlen(b),0)

#ifdef _MSC_VER
# define alloca _alloca
#endif

#define _CDB_WRITE  0
#define _CDB_UPDATE 1

#ifndef O_BINARY
#define O_BINARY 0
#endif

/* Helper to pull the cdb_t pointer out of a wrapped SV */
cdb_t* extract_cdb_ptr(SV* obj) {
  dTHX;
  HV *self = (HV *) SvRV(obj);
  return (cdb_t *) SvIV(*(my_hv_fetch(self, "_cdb")));
}

void _check_return(int ret) {

  switch (ret) {
    case CDB_SUCCESS : break;
  /*
    case CDB_ETMRANGE: rb_raise(rb_eArgError, "End time must be >= Start time.");
    case CDB_ESANITY : rb_raise(rb_eStandardError, "Database is unsynced.");
    case CDB_ENOMEM  : rb_raise(rb_eNoMemError, "No memory could be allocated.");
    case CDB_ENORECS : rb_raise(rb_eRangeError, "There were no records in the database to be read.");
    case CDB_EINTERPD: rb_raise(rb_eRuntimeError, "Aggregate driver issue. Possibly no records!");
    case CDB_EINTERPF: rb_raise(rb_eRuntimeError, "Aggregate follower issue. Possibly no records!");
    default          : rb_sys_fail(0);
    */
  }
}

void _cdb_update_header_hash(SV *obj, cdb_t *cdb) {
  HV *self   = (HV *) SvRV(obj);
  HV *header = (HV *) SvRV(*(my_hv_fetch(self, "header")));

  /* Update the number of records in the header hash */ 
  my_hv_store(header, "num_records", newSVnv(cdb->header->num_records));
}

cdb_range_t* _new_statistics(SV *obj) {
  cdb_range_t *range = (cdb_range_t*)calloc(1, sizeof(cdb_range_t));

  /* Store the range statistics as an ivar. This overwrites any previous
   * value. Statistics are good only for the last read. */
  //HV *self  = (HV *) SvRV(obj);
  //HV *stats = (HV *) SvRV(*(my_hv_fetch(self, "statistics")));

  //if (!SvOK(stats)) {
    //sv_bless(obj_ref, gv_stashpv(class, FALSE));
    //my_hv_store(self, "statistics", newHV());

    //statistics = rb_class_new_instance(0, 0, cStatistics);
    //rb_iv_set(self, "@statistics", statistics);
  //}

  memset(range, 0, sizeof(cdb_range_t));

  return range;
}

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

cdb_request_t _parse_cdb_request(time_t start, time_t end, uint64_t count, bool cooked, int step) {
  cdb_request_t request = cdb_new_request();

  request.start  = start;
  request.end    = end;
  request.count  = count;
  request.step   = step;
  request.cooked = cooked;

  return request;
}

/* Helper functions for write_* and update_* since they do almost the same * thing. */
int _cdb_write_or_update_records(SV* obj, SV* array_ref, int type) {

  uint64_t len = 0;
  uint64_t i   = 0;
  uint64_t cnt = 0;
  bool     ret = false;

  AV *array;
  cdb_t *cdb;
  cdb_record_t *records;

  array   = (AV *)SvRV(array_ref);
  len     = av_len(array) + 1;

  cdb     = extract_cdb_ptr(obj);
  records = alloca(sizeof(cdb_record_t) * len);

  // Turn any undefs into NaN
  for (i = 0; i < len; i++) {

    AV *record = (AV *)SvRV(*av_fetch(array, i, 0));

    /* Compatability with pure perl version:
     * Ignore any records that have 0 or undef time values
     * Turn any undef/non-digit values into NaN */
    SV *time_sv  = *av_fetch(record, 0, 0);
    SV *value_sv = *av_fetch(record, 1, 0);

    if (!SvIOK(time_sv) || (time_t)SvIV(time_sv) == 0) {
      continue;
    }

    if (!SvNIOK(value_sv)) {
      value_sv = newSVnv(CDB_NAN);
    }

    records[i].time  = (time_t)SvIV(time_sv);
    records[i].value = (double)SvNV(value_sv);
  }

  if (type == _CDB_WRITE) {
    ret = cdb_write_records(cdb, records, len, &cnt);
  } else {
    ret = cdb_update_records(cdb, records, len, &cnt);
  }

  if (ret != CDB_SUCCESS) {
    warn("Couldn't write/update records: %d\n", ret);
    return 0;
  }

  _cdb_update_header_hash(obj, cdb);

  return (int)cnt;
}

int _cdb_write_or_update_record(SV* obj, SV *time_sv, SV *value_sv, int type) {

  bool ret = false;
  time_t time;
  double value;

  cdb_t *cdb = extract_cdb_ptr(obj);

  if (!SvIOK(time_sv) || (time_t)SvIV(time_sv) == 0) {
    return (int)ret;
  }

  if (!SvNIOK(value_sv)) {
    value_sv = newSVnv(CDB_NAN);
  }

  time  = (time_t)SvIV(time_sv);
  value = (double)SvNV(value_sv);

  if (type == _CDB_WRITE) {
    ret = cdb_write_record(cdb, time, value);
  } else {
    ret = cdb_update_record(cdb, time, value);
  }

  if (ret == false) {
    warn("Couldn't write/update record!\n");
  } else {
    _cdb_update_header_hash(obj, cdb);
  }

  return (int)ret;
}

MODULE = CircularDB PACKAGE = CircularDB::Storage

PROTOTYPES: DISABLE

SV*
new(class, path, ...)
  char *class;
  char *path;

  PREINIT:
  int flags = -1;
  int mode  = -1;
  int type  = CDB_DEFAULT_DATA_TYPE;
  int interval = 0;

  char *name  = "";
  char *desc  = "";
  char *units = CDB_DEFAULT_DATA_UNIT;

  uint64_t max_records = 0;
  uint64_t min_value   = 0;
  uint64_t max_value   = 0;

  CODE:
  int ret;
  bool has_name = false;

  HV *self    = newHV();
  HV *header  = newHV();

  SV *obj_ref = newRV_noinc((SV*) self);
  cdb_t *cdb = cdb_new();

  /* class, path  flags, mode, name, max_records, type, units, min_value, max_value, interval */
  if (items > 2) {
    if (SvOK(ST(2))) {
      if (strcmp(SvPV_nolen(ST(2)), "r") == 0) {
        flags = O_RDONLY|O_EXCL|O_BINARY;
      } else if (strcmp(SvPV_nolen(ST(2)), "w") == 0) {
        flags = O_CREAT|O_RDWR|O_EXCL|O_BINARY;
      } else {
        flags = O_CREAT|O_RDWR|O_EXCL|O_BINARY;
      }
    }

    if (SvOK(ST(3)))
      mode = SvIV(ST(3));

    if (SvOK(ST(4)))
      name = SvPV_nolen(ST(4));
      has_name = true;

    if (SvOK(ST(5)))
      max_records = SvNV(ST(5));

    if (SvOK(ST(6)))
      type = _cdb_type_from_string(SvPV_nolen(ST(6)));

    if (SvOK(ST(7)))
      units = SvPV_nolen(ST(7));

    if (SvOK(ST(8)))
      min_value = SvNV(ST(8));

    if (SvOK(ST(9)))
      max_value = SvNV(ST(9));

    if (SvOK(ST(10)))
      interval = SvIV(ST(10));
  }

  cdb->filename = path;
  cdb->flags    = flags;
  cdb->mode     = mode;

  /* Try the open */
  ret = cdb_open(cdb);

  if (ret != CDB_SUCCESS) {
    warn("Couldn't open CircularDB file: %s Error: [%s]\n", path, strerror(ret));
    XSRETURN_UNDEF;
  }

  /* Try and read a header, if it doesn't exist, create one */
  if (has_name == false) {

    ret = cdb_read_header(cdb);

    if (ret != CDB_SUCCESS) {
      warn("Couldn't read header! File: %s Error: [%s]\n", path, strerror(ret));
      XSRETURN_UNDEF;
    }

  } else {

    cdb_generate_header(cdb, name, desc, max_records, type, units, min_value, max_value, interval);

    ret = cdb_write_header(cdb);

    if (ret != CDB_SUCCESS) {
      warn("Couldn't write header! File: %s Error: [%s]\n", path, strerror(ret));
      XSRETURN_UNDEF;
    }
  }

  my_hv_store(header, "filename", newSVpv(path, 0));
  my_hv_store(header, "name", newSVpv(cdb->header->name, 0));
  my_hv_store(header, "desc", newSVpv(cdb->header->desc, 0));
  my_hv_store(header, "type", newSVpv(_string_from_cdb_type(cdb->header->type), 0));
  my_hv_store(header, "units", newSVpv(cdb->header->units, 0));
  my_hv_store(header, "num_records", newSVnv(cdb->header->num_records));
  my_hv_store(header, "max_records", newSVnv(cdb->header->max_records));
  my_hv_store(header, "min_value", newSVnv(cdb->header->min_value));
  my_hv_store(header, "max_value", newSVnv(cdb->header->max_value));
  my_hv_store(header, "interval", newSVnv(cdb->header->interval));

  my_hv_store(self, "header", newRV_noinc((SV *)header));
  my_hv_store(self, "statistics", newSV(0));
  my_hv_store(self, "_cdb", newSViv((IV) cdb));

  /* Bless the hashref to create a class object */
  sv_bless(obj_ref, gv_stashpv(class, FALSE));

  RETVAL = obj_ref;

  OUTPUT:
  RETVAL

void
DESTROY(obj)
  SV *obj;

  CODE:
  cdb_free(extract_cdb_ptr(obj));

void
close(obj)
  SV *obj;

  CODE:
  cdb_t *cdb = extract_cdb_ptr(obj);

  if (cdb_close(cdb) != CDB_SUCCESS) {
    warn("Couldn't close cdb!\n");
  }

void
print(obj)
  SV *obj;

  CODE:
  cdb_print(extract_cdb_ptr(obj));

void
print_header(obj)
  SV *obj;

  CODE:
  cdb_print_header(extract_cdb_ptr(obj));

void
read_header(obj)
  SV *obj;

  CODE:
  cdb_t *cdb = extract_cdb_ptr(obj);

  if (cdb_read_header(cdb) != CDB_SUCCESS) {
    warn("Can't read header!\n");
    XSRETURN_UNDEF;
  }

  _cdb_update_header_hash(obj, cdb);

SV*
read_records(obj, ...)
  SV *obj;

  PREINIT:
  time_t start = 0;
  time_t end   = 0;
  bool cooked  = true;
  uint64_t cnt = 0;
  int step     = 0;

  CODE:

  uint64_t i   = 0;
  int ret      = 0;

  AV *array  = newAV();
  cdb_t *cdb = extract_cdb_ptr(obj);

  cdb_range_t *range    = _new_statistics(obj);
  cdb_record_t *records = NULL;
  cdb_request_t request;

  if (items > 0) {
    if (SvOK(ST(1)))
      start = SvIV(ST(1));

    if (SvOK(ST(2)))
      end = SvIV(ST(2));

    if (SvOK(ST(3)))
      cnt = SvNV(ST(3));

    if (SvOK(ST(4)))
      cooked = SvIV(ST(4));

    if (SvOK(ST(5)))
      step = SvIV(ST(5));
  }

  request = _parse_cdb_request(start, end, cnt, cooked, step);

  ret = cdb_read_records(cdb, &request, &cnt, &records, range);

  _check_return(ret);

  for (i = 0; i < cnt; i++) {
    AV *entry  = newAV();

    av_push(entry, newSViv(records[i].time));

    /* NaN values should be returned as 'undef' */
    if (isnan(records[i].value)) {
      av_push(entry, newSV(0));
    } else {
      av_push(entry, newSVnv(records[i].value));
    }

    /* And create a new array ref, without incrementing the reference count */
    av_push(array, newRV_noinc((SV*)entry));
  }

  free(records);

  _cdb_update_header_hash(obj, cdb);

  RETVAL = newRV_noinc((SV*)array);

  OUTPUT:
  RETVAL

void
print_records(obj, ...)
  SV *obj;

  PREINIT:
  time_t start = 0;
  time_t end   = 0;
  int cnt = 0;
  char *date_format = "%Y-%m-%d %H:%M:%S";
  bool cooked  = true;
  int step     = 0;
  PerlIO *fh   = PerlIO_stderr();

  CODE:
  cdb_t *cdb;
  cdb_request_t request;

  if (items > 1) {
    if (SvOK(ST(1)))
      start = SvIV(ST(1));

    if (SvOK(ST(2)))
      end = SvIV(ST(2));

    if (SvOK(ST(3)))
      cnt = SvNV(ST(3));

    /* dereference and get the SV* that contains the Magic & FH,
     * then pull the fd from the PerlIO object */
    if (SvOK(ST(4)))
      fh = IoIFP(GvIOp(SvRV(ST(4))));

    if (SvOK(ST(5)))
      date_format = SvPV_nolen(ST(5));

    if (SvOK(ST(6)))
      cooked = SvIV(ST(6));

    if (SvOK(ST(7)))
      step = SvIV(ST(7));
  }

  cdb     = extract_cdb_ptr(obj);
  request = _parse_cdb_request(start, end, cnt, cooked, step);

  cdb_print_records(cdb, &request, PerlIO_findFILE(fh), date_format);

void
write_header(obj)
  SV *obj;

  CODE:
  cdb_t *cdb = extract_cdb_ptr(obj);
  int ret    = cdb_write_header(cdb);

  if (ret != CDB_SUCCESS) {
    warn("Can't write header on file: %s : %d\n", cdb->filename, ret);
  }

int
write_records(obj, array_ref)
  SV *obj;
  SV *array_ref;

  CODE:
  RETVAL = _cdb_write_or_update_records(obj, array_ref, _CDB_WRITE);
  OUTPUT:
  RETVAL

int
write_record(obj, time, value)
  SV *obj;
  SV *time;
  SV *value;

  CODE:
  RETVAL = _cdb_write_or_update_record(obj, time, value, _CDB_WRITE);
  OUTPUT:
  RETVAL

int
update_records(obj, array_ref)
  SV *obj;
  SV *array_ref;

  CODE:
  RETVAL = _cdb_write_or_update_records(obj, array_ref, _CDB_UPDATE);
  OUTPUT:
  RETVAL

int
update_record(obj, time, value)
  SV *obj;
  SV *time;
  SV *value;

  CODE:
  RETVAL = _cdb_write_or_update_record(obj, time, value, _CDB_UPDATE);
  OUTPUT:
  RETVAL

static SV*
_set_header(obj, name, value)
  SV* obj;
  char* name;
  SV* value;

  CODE:

  HV *self   = (HV *) SvRV(obj);
  HV *header = (HV *) SvRV(*(my_hv_fetch(self, "header")));

  cdb_t *cdb = extract_cdb_ptr(obj);

  if (strcmp(name, "name") == 0) {
    strncpy(cdb->header->name, SvPV_nolen(value), sizeof(cdb->header->name));
    cdb->synced = false;

  } else if (strcmp(name, "min_value") == 0) {
    cdb->header->min_value = SvIV(value);
    cdb->synced = false;

  } else if (strcmp(name, "max_value") == 0) {
    cdb->header->max_value = SvIV(value);
    cdb->synced = false;

  } else if (strcmp(name, "interval") == 0) {
    cdb->header->interval = SvIV(value);
    cdb->synced = false;

  } else if (strcmp(name, "units") == 0) {
    strncpy(cdb->header->units, SvPV_nolen(value), sizeof(cdb->header->units));
    cdb->synced = false;
  }

  if (cdb->synced == false) {
    my_hv_store(header, name, newSVsv(value));
  }

  RETVAL = value;

  OUTPUT:
  RETVAL

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
cdb_t* extract_cdb_ptr(SV *self) {
  return (cdb_t *) SvIV(*(my_hv_fetch((HV *)SvRV(self), "_cdb")));
}

/* self is a CircularDB::Statistics object */
cdb_range_t* extract_range_ptr(SV *self) {
  return (cdb_range_t *)SvIV(*(my_hv_fetch((HV *)SvRV(self), "_range")));
}

void _check_return(int ret) {
  switch (ret) {
    case CDB_SUCCESS : break;
    case CDB_ETMRANGE: warn("End time must be >= Start time.\n");
    case CDB_ESANITY : warn("Database is unsynced.\n");
    case CDB_ENOMEM  : warn("No memory could be allocated.\n");
    case CDB_ENORECS : warn("There were no records in the database to be read.\n");
    case CDB_EINTERPD: warn("Aggregate driver issue. Possibly no records!\n");
    case CDB_EINTERPF: warn("Aggregate follower issue. Possibly no records!\n");
    default          : warn("An unknown CircularDB error occured. Errno: %s\n", strerror(errno));
  }
}

void _cdb_update_header_hash(SV *self, cdb_t *cdb) {
  HV *header = (HV *)SvRV(*(my_hv_fetch((HV *)SvRV(self), "header")));

  /* Update the number of records in the header hash */ 
  my_hv_store(header, "num_records", newSVnv(cdb->header->num_records));
}

/* Create a new ::Statistics object. */
cdb_range_t* _new_statistics(SV *obj) {
  cdb_range_t *range;

  HV *self  = (HV *)SvRV(obj);
  SV *stats = (SV *)my_hv_fetch(self, "statistics");

  /* This magic is all having to do with the Perl/XSUB stack. */
  if (!SvOK(stats)) {
    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    XPUSHs(sv_2mortal(newSVpv("CircularDB::Statistics", 0)));
    PUTBACK;
    call_method("_new", G_SCALAR);
    SPAGAIN;

    stats = POPs;

    /* Store the range statistics as an ivar. This overwrites any previous
     * value. Statistics are good only for the last read. */
    if (SvOK(stats)) {
      my_hv_store(self, "statistics", SvREFCNT_inc(stats));
    }

    PUTBACK;
    FREETMPS;
    LEAVE;
  }

  /* range now points into the object */
  range = extract_range_ptr(stats);

  memset(range, 0, sizeof(cdb_range_t));

  return range;
}

/* Helper functions for dealing with CDB Types */
int _cdb_type_from_string(char *type) {
  if (strEQ(type, "gauge")) {
    return CDB_TYPE_GAUGE;
  } else if (strEQ(type, "counter")) {
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
int _cdb_write_or_update_records(SV* self, SV* array_ref, int type) {

  uint64_t len = 0;
  uint64_t i   = 0;
  uint64_t cnt = 0;
  bool     ret = false;

  AV *array;
  cdb_t *cdb;
  cdb_record_t *records;

  array   = (AV *)SvRV(array_ref);
  len     = av_len(array) + 1;

  cdb     = extract_cdb_ptr(self);
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

  _cdb_update_header_hash(self, cdb);

  return (int)cnt;
}

int _cdb_write_or_update_record(SV* self, SV *time_sv, SV *value_sv, int type) {

  bool ret = false;
  time_t time;
  double value;

  cdb_t *cdb = extract_cdb_ptr(self);

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
    _cdb_update_header_hash(self, cdb);
  }

  return (int)ret;
}

/* Class & Instance methods start here. */

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

  HV *self   = newHV();
  HV *header = newHV();

  cdb_t *cdb = cdb_new();

  /* class, path  flags, mode, name, max_records, type, units, min_value, max_value, interval */
  if (items >= 2 && SvOK(ST(2)))
    flags = SvIV(ST(2));

  if (items >= 3 && SvOK(ST(3)))
    mode = SvIV(ST(3));

  if (items >= 4 && SvOK(ST(4)))
    name = SvPV_nolen(ST(4));
    has_name = true;

  if (items >= 5 && SvOK(ST(5)))
    max_records = SvNV(ST(5));

  if (items >= 6 && SvOK(ST(6)))
    type = _cdb_type_from_string(SvPV_nolen(ST(6)));

  if (items >= 7 && SvOK(ST(7)))
    units = SvPV_nolen(ST(7));

  if (items >= 8 && SvOK(ST(8)))
    min_value = SvNV(ST(8));

  if (items >= 9 && SvOK(ST(10)))
    max_value = SvNV(ST(9));

  if (items >= 10 && SvOK(ST(10)))
    interval = SvIV(ST(10));

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
  RETVAL = sv_bless(newRV_noinc((SV*) self), gv_stashpv(class, FALSE));
  OUTPUT:
  RETVAL

void
DESTROY(self)
  SV *self;

  CODE:
  cdb_free(extract_cdb_ptr(self));

void
close(self)
  SV *self;

  CODE:
  cdb_t *cdb = extract_cdb_ptr(self);

  if (cdb_close(cdb) != CDB_SUCCESS) {
    warn("Couldn't close cdb!\n");
  }

void
print(self)
  SV *self;

  CODE:
  cdb_print(extract_cdb_ptr(self));

void
print_header(self)
  SV *self;

  CODE:
  cdb_print_header(extract_cdb_ptr(self));

void
read_header(self)
  SV *self;

  CODE:
  cdb_t *cdb = extract_cdb_ptr(self);

  if (cdb_read_header(cdb) != CDB_SUCCESS) {
    warn("Can't read header!\n");
    XSRETURN_UNDEF;
  }

  _cdb_update_header_hash(self, cdb);

SV*
read_records(self, ...)
  SV *self;

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
  cdb_t *cdb = extract_cdb_ptr(self);

  cdb_range_t *range    = NULL;
  cdb_record_t *records = NULL;
  cdb_request_t request;

  if (items >= 1 && SvOK(ST(1)))
    start = SvIV(ST(1));

  if (items >= 2 && SvOK(ST(2)))
    end = SvIV(ST(2));

  if (items >= 3 && SvOK(ST(3)))
    cnt = SvNV(ST(3));

  if (items >= 4 && SvOK(ST(4)))
    cooked = SvIV(ST(4));

  if (items >= 5 && SvOK(ST(5)))
    step = SvIV(ST(5));

  /* This must come after the items arg parsing, as _new_statistcs mucks with
   * the XSUB stack. There's probably a way to fix that, but this is easier. */
  range   = _new_statistics(self);
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

  Safefree(records);

  _cdb_update_header_hash(self, cdb);

  RETVAL = newRV_noinc((SV*)array);

  OUTPUT:
  RETVAL

void
print_records(self, ...)
  SV *self;

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

  if (items >= 1 && SvOK(ST(1)))
    start = SvIV(ST(1));

  if (items >= 2 && SvOK(ST(2)))
    end = SvIV(ST(2));

  if (items >= 3 && SvOK(ST(3)))
    cnt = SvNV(ST(3));

  /* dereference and get the SV* that contains the Magic & FH,
   * then pull the fd from the PerlIO object */
  if (items >= 4 && SvOK(ST(4)))
    fh = IoIFP(GvIOp(SvRV(ST(4))));

  if (items >= 5 && SvOK(ST(5)))
    date_format = SvPV_nolen(ST(5));

  if (items >= 6 && SvOK(ST(6)))
    cooked = SvIV(ST(6));

  if (items >= 7 && SvOK(ST(7)))
    step = SvIV(ST(7));

  cdb     = extract_cdb_ptr(self);
  request = _parse_cdb_request(start, end, cnt, cooked, step);

  cdb_print_records(cdb, &request, PerlIO_findFILE(fh), date_format);

void
write_header(self)
  SV *self;

  CODE:
  cdb_t *cdb = extract_cdb_ptr(self);
  int ret    = cdb_write_header(cdb);

  if (ret != CDB_SUCCESS) {
    warn("Can't write header on file: %s : %d\n", cdb->filename, ret);
  }

int
write_records(self, array_ref)
  SV *self;
  SV *array_ref;

  CODE:
  RETVAL = _cdb_write_or_update_records(self, array_ref, _CDB_WRITE);
  OUTPUT:
  RETVAL

int
write_record(self, time, value)
  SV *self;
  SV *time;
  SV *value;

  CODE:
  RETVAL = _cdb_write_or_update_record(self, time, value, _CDB_WRITE);
  OUTPUT:
  RETVAL

int
update_records(self, array_ref)
  SV *self;
  SV *array_ref;

  CODE:
  RETVAL = _cdb_write_or_update_records(self, array_ref, _CDB_UPDATE);
  OUTPUT:
  RETVAL

int
update_record(self, time, value)
  SV *self;
  SV *time;
  SV *value;

  CODE:
  RETVAL = _cdb_write_or_update_record(self, time, value, _CDB_UPDATE);
  OUTPUT:
  RETVAL

int
discard_records_in_time_range(self, start, end)
  SV *self;
  time_t start;
  time_t end;

  CODE:
  int ret;
  uint64_t cnt = 0;
  cdb_request_t request;

  cdb_t *cdb = extract_cdb_ptr(self);

  request = _parse_cdb_request(start, end, 0, false, 0);
  ret     = cdb_discard_records_in_time_range(cdb, &request, &cnt);

  _check_return(ret);

  RETVAL = cnt;
  OUTPUT:
  RETVAL

static SV*
_set_header(self, name, value)
  SV* self;
  char* name;
  SV* value;

  CODE:

  HV *header = (HV *) SvRV(*(my_hv_fetch((HV *)SvRV(self), "header")));

  cdb_t *cdb = extract_cdb_ptr(self);

  if (strEQ(name, "name")) {
    strncpy(cdb->header->name, SvPV_nolen(value), sizeof(cdb->header->name));
    cdb->synced = false;

  } else if (strEQ(name, "min_value")) {
    cdb->header->min_value = SvIV(value);
    cdb->synced = false;

  } else if (strEQ(name, "max_value")) {
    cdb->header->max_value = SvIV(value);
    cdb->synced = false;

  } else if (strEQ(name, "interval")) {
    cdb->header->interval = SvIV(value);
    cdb->synced = false;

  } else if (strEQ(name, "units")) {
    strncpy(cdb->header->units, SvPV_nolen(value), sizeof(cdb->header->units));
    cdb->synced = false;
  }

  if (cdb->synced == false) {
    my_hv_store(header, name, newSVsv(value));
  }

  RETVAL = value;
  OUTPUT:
  RETVAL


MODULE = CircularDB PACKAGE = CircularDB::Aggregate

PROTOTYPES: DISABLE

SV*
new(class, name)
  char *class;
  char *name;

  CODE:
  HV *self = newHV();

  my_hv_store(self, "name", newSVpv(name, 0));
  my_hv_store(self, "statistics", newSV(0));
  my_hv_store(self, "cdbs", newRV_noinc((SV*)newAV()));

  /* Bless the hashref to create a class object */
  RETVAL = sv_bless(newRV_noinc((SV*) self), gv_stashpv(class, FALSE));

  OUTPUT:
  RETVAL

SV*
read_records(self, ...)
  SV *self;

  PREINIT:
  time_t start = 0;
  time_t end   = 0;
  bool cooked  = true;
  uint64_t cnt = 0;
  int step     = 0;

  CODE:

  uint64_t i   = 0;
  int ret      = 0;
  int num_cdbs = 0;

  AV *cdb_objects;
  AV *array  = newAV();

  cdb_range_t *range    = _new_statistics(self);
  cdb_record_t *records = NULL;
  cdb_request_t request;

  cdb_objects = (AV*)SvRV(*my_hv_fetch((HV *)SvRV(self), "cdbs"));
  num_cdbs    = av_len(cdb_objects) + 1;

  /* initialize the cdbs array to the size of the cdb_objects array */
  cdb_t *cdbs[num_cdbs];

  if (items >= 1 && SvOK(ST(1)))
    start = SvIV(ST(1));

  if (items >= 2 && SvOK(ST(2)))
    end = SvIV(ST(2));

  if (items >= 3 && SvOK(ST(3)))
    cnt = SvNV(ST(3));

  if (items >= 4 && SvOK(ST(4)))
    cooked = SvIV(ST(4));

  if (items >= 5 && SvOK(ST(5)))
    step = SvIV(ST(5));

  request = _parse_cdb_request(start, end, cnt, cooked, step);

  /* First, loop over the incoming array of CircularDB::Storage objects and
   * extract the pointers to the cdb_t structs. */
  for (i = 0; i < num_cdbs; i++) {

    cdbs[i] = extract_cdb_ptr(*av_fetch(cdb_objects, i, 0));
  }

  ret = cdb_read_aggregate_records(cdbs, num_cdbs, &request, &cnt, &records, range);

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

  RETVAL = newRV_noinc((SV*)array);

  OUTPUT:
  RETVAL

void
print_records(self, ...)
  SV *self;

  PREINIT:
  time_t start = 0;
  time_t end   = 0;
  int cnt = 0;
  char *date_format = "%Y-%m-%d %H:%M:%S";
  bool cooked  = true;
  int step     = 0;
  PerlIO *fh   = PerlIO_stderr();

  CODE:
  uint64_t i   = 0;
  int num_cdbs = 0;

  AV *cdb_objects;
  cdb_request_t request;

  cdb_objects = (AV*)SvRV(*my_hv_fetch((HV *)SvRV(self), "cdbs"));
  num_cdbs    = av_len(cdb_objects) + 1;

  /* initialize the cdbs array to the size of the cdb_objects array */
  cdb_t *cdbs[num_cdbs];

  if (items >= 1 && SvOK(ST(1)))
    start = SvIV(ST(1));

  if (items >= 2 && SvOK(ST(2)))
    end = SvIV(ST(2));

  if (items >= 3 && SvOK(ST(3)))
    cnt = SvNV(ST(3));

  /* dereference and get the SV* that contains the Magic & FH,
   * then pull the fd from the PerlIO object */
  if (items >= 4 && SvOK(ST(4)))
    fh = IoIFP(GvIOp(SvRV(ST(4))));

  if (items >= 5 && SvOK(ST(5)))
    date_format = SvPV_nolen(ST(5));

  if (items >= 6 && SvOK(ST(6)))
    cooked = SvIV(ST(6));

  if (items >= 7 && SvOK(ST(7)))
    step = SvIV(ST(7));

  request = _parse_cdb_request(start, end, cnt, cooked, step);

  for (i = 0; i < num_cdbs; i++) {
    cdbs[i] = extract_cdb_ptr(*av_fetch(cdb_objects, i, 0));
  }

  cdb_print_aggregate_records(cdbs, num_cdbs, &request, PerlIO_findFILE(fh), date_format);


MODULE = CircularDB PACKAGE = CircularDB::Statistics

PROTOTYPES: DISABLE

# This shouldn't be called by the user. */
SV*
_new(class)
  char *class;

  CODE:
  cdb_range_t *range;
  HV *self = newHV();

  Newxz(range, 1, cdb_range_t);

  /* Store a pointer to the range struct */
  my_hv_store(self, "_range", newSViv((IV)range));

  RETVAL = sv_bless(newRV_noinc((SV*) self), gv_stashpv(class, FALSE));
  OUTPUT:
  RETVAL

void
DESTROY(self)
  SV *self;

  CODE:
  Safefree(extract_range_ptr(self));

double
median(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_MEDIAN);
  OUTPUT:
  RETVAL

double
mad(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_MAD);
  OUTPUT:
  RETVAL

double
mean(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_MEAN);
  OUTPUT:
  RETVAL

double
sum(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_SUM);
  OUTPUT:
  RETVAL

double
min(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_MIN);
  OUTPUT:
  RETVAL

double
max(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_MAX);
  OUTPUT:
  RETVAL

double
stddev(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_STDDEV);
  OUTPUT:
  RETVAL

double
absdev(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_ABSDEV);
  OUTPUT:
  RETVAL

double
pct95th(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_95TH);
  OUTPUT:
  RETVAL

double
pct75th(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_75TH);
  OUTPUT:
  RETVAL

double
pct50th(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_50TH);
  OUTPUT:
  RETVAL

double
pct25th(self)
  SV *self;

  CODE:
  RETVAL = cdb_get_statistic(extract_range_ptr(self), CDB_25TH);
  OUTPUT:
  RETVAL

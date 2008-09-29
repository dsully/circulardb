/* $Id: circulardb_ext.c 16 2007-09-04 21:06:11Z dsully $ 
 *
 * Ruby bindings for CircularDB
 *
 * */

#include <ruby.h>
#include <rubyio.h>

#include <circulardb.h>
#include <fcntl.h>

#define RCDB_WRITE  0
#define RCDB_UPDATE 1

/* TODO handle clone/dup */

static VALUE mCircularDB;
static VALUE cStorage;
static VALUE cAggregate;
static VALUE cStatistics;
static VALUE mTime;

/* Symbol holders */
static ID s_to_i;

void print_class(char* token, VALUE obj) {
    VALUE class = rb_any_to_s(obj);

    fprintf(stderr, "token: [%s] id: [%s]\n", token, StringValuePtr(class));
}

void _check_return(int ret) {

    switch (ret) {
        case CDB_SUCCESS : break;
        case CDB_ETMRANGE: rb_raise(rb_eArgError, "End time must be >= Start time.");
        case CDB_ESANITY : rb_raise(rb_eStandardError, "Database is unsynced.");
        case CDB_ENOMEM  : rb_raise(rb_eNoMemError, "No memory could be allocated.");
        case CDB_ENORECS : rb_raise(rb_eRangeError, "There were no records in the database to be read.");
        case CDB_EINTERPD: rb_raise(rb_eRuntimeError, "Aggregate driver issue. Possibly no records!");
        case CDB_EINTERPF: rb_raise(rb_eRuntimeError, "Aggregate follower issue. Possibly no records!");
        default          : rb_sys_fail(0);
    }
}

/* Cleanup after an object has been GCed */
static void cdb_rb_free(void *p) {
    cdb_free(p);
}

static VALUE cdb_rb_alloc(VALUE klass) { 

    cdb_t *cdb = cdb_new();

    VALUE obj = Data_Wrap_Struct(klass, 0, cdb_rb_free, cdb);

    return obj;
} 

/* Allocator/Dealloc for CircularDB::Statistics */
static void cdb_statistics_rb_free(void *p) {
    if (p != NULL) {
        free(p);
        p = NULL;
    }
}

static VALUE cdb_statistics_rb_alloc(VALUE klass) { 

    cdb_range_t *range = calloc(1, sizeof(cdb_range_t));

    VALUE obj = Data_Wrap_Struct(klass, 0, cdb_statistics_rb_free, range);

    return obj;
}

cdb_range_t* _new_statistics(VALUE self) {
    cdb_range_t *range;

    /* Store the range statistics as an ivar. This overwrites any previous
     * value. Statistics are good only for the last read. */
    VALUE statistics = rb_iv_get(self, "@statistics");

    if (statistics == Qnil) {
        statistics = rb_class_new_instance(0, 0, cStatistics);
        rb_iv_set(self, "@statistics", statistics);
    }

    /* range now points into the object */
    Data_Get_Struct(statistics, cdb_range_t, range);

    MEMZERO(range, cdb_range_t, 1);

    return range;
}

void _cdb_rb_update_header_hash(VALUE self, cdb_t *cdb) {

    VALUE header = rb_iv_get(self, "@header");

    /* Update the number of records in the ruby header hash */ 
    rb_hash_aset(header, ID2SYM(rb_intern("num_records")), ULL2NUM(cdb->header->num_records));
}

int _cdb_type_from_symbol(VALUE symbol) {
    if (strcmp(rb_id2name(SYM2ID(symbol)), "gauge") == 0) {
        return CDB_TYPE_GAUGE;
    } else if (strcmp(rb_id2name(SYM2ID(symbol)), "counter") == 0) {
        return CDB_TYPE_COUNTER;
    }

    return CDB_DEFAULT_DATA_TYPE;
}

VALUE _symbol_from_cdb_type(int type) {
    switch (type) {
        case CDB_TYPE_GAUGE:
            return ID2SYM(rb_intern("gauge"));
        case CDB_TYPE_COUNTER:
            return ID2SYM(rb_intern("counter"));
    };
}

static VALUE cdb_rb_initialize(int argc, VALUE *argv, VALUE self) {

    cdb_t *cdb;
    VALUE header = rb_hash_new();
    VALUE filename, flags, mode, name, max_records, num_records, type, units, min_value, max_value, interval;

    rb_scan_args(argc, argv, "19", &filename, &flags, &mode, &name, &max_records, &type, &units, &min_value, &max_value, &interval);

    Data_Get_Struct(self, cdb_t, cdb);

    rb_hash_aset(header, ID2SYM(rb_intern("filename")), filename);

    cdb->filename = StringValuePtr(filename);
    cdb->flags    = NIL_P(flags) ? -1 : NUM2INT(flags);
    cdb->mode     = NIL_P(mode)  ? -1 : NUM2INT(mode);

    /* Try the open */
    if (cdb_open(cdb) != CDB_SUCCESS) {
        rb_sys_fail(0);
    }

    /* Try and read a header, if it doesn't exist, create one */
    if (NIL_P(name)) {

        if (cdb_read_header(cdb) != CDB_SUCCESS) {
            rb_sys_fail(0);
        }

        name         = rb_str_new2(cdb->header->name);
        type         = _symbol_from_cdb_type(cdb->header->type);
        units        = rb_str_new2(cdb->header->units);
        max_records  = ULL2NUM(cdb->header->max_records);
        num_records  = ULL2NUM(cdb->header->num_records);
        min_value    = ULL2NUM(cdb->header->min_value);
        max_value    = ULL2NUM(cdb->header->max_value);
        interval     = ULL2NUM(cdb->header->interval);

    } else {

        if (NIL_P(max_records)) max_records = INT2FIX(0);
        if (NIL_P(interval)) interval       = INT2FIX(0);
        if (NIL_P(min_value)) min_value     = INT2FIX(0);
        if (NIL_P(max_value)) max_value     = INT2FIX(0);

        if (NIL_P(type)) {
            type = _symbol_from_cdb_type(CDB_DEFAULT_DATA_TYPE);
        }

        if (NIL_P(units) || (strcmp(StringValuePtr(units), "") == 0)) {
            units = rb_str_new2(CDB_DEFAULT_DATA_UNIT);
        }

        cdb_generate_header(cdb,
            StringValuePtr(name),
            rb_num2ull(max_records),
            _cdb_type_from_symbol(type),
            StringValuePtr(units),
            rb_num2ull(min_value),
            rb_num2ull(max_value),
            NUM2INT(interval)
        );

        num_records = INT2FIX(0);

        if (cdb_write_header(cdb) != CDB_SUCCESS) {
            rb_sys_fail(0);
        }
    }

    rb_hash_aset(header, ID2SYM(rb_intern("name")), name);
    rb_hash_aset(header, ID2SYM(rb_intern("type")), type);
    rb_hash_aset(header, ID2SYM(rb_intern("units")), units);
    rb_hash_aset(header, ID2SYM(rb_intern("num_records")), num_records);
    rb_hash_aset(header, ID2SYM(rb_intern("max_records")), max_records);
    rb_hash_aset(header, ID2SYM(rb_intern("min_value")), min_value);
    rb_hash_aset(header, ID2SYM(rb_intern("max_value")), max_value);
    rb_hash_aset(header, ID2SYM(rb_intern("interval")), interval);

    rb_iv_set(self, "@header", header);
    rb_iv_set(self, "@statistics", Qnil);

    return self;
}

static VALUE _set_header(VALUE self, VALUE name, VALUE value) {

    cdb_t *cdb;
    VALUE header = rb_iv_get(self, "@header");

    Data_Get_Struct(self, cdb_t, cdb);

    if (strcmp(StringValuePtr(name), "name") == 0) {

        rb_hash_aset(header, ID2SYM(rb_intern("name")), value);
        strncpy(cdb->header->name, StringValuePtr(value), sizeof(cdb->header->name));
        cdb->synced = false;

    } else if (strcmp(StringValuePtr(name), "min_value") == 0) {

        rb_hash_aset(header, ID2SYM(rb_intern("min_value")), value);
        cdb->header->min_value = rb_num2ull(value);
        cdb->synced = false;

    } else if (strcmp(StringValuePtr(name), "max_value") == 0) {

        rb_hash_aset(header, ID2SYM(rb_intern("max_value")), value);
        cdb->header->max_value = rb_num2ull(value);
        cdb->synced = false;

    } else if (strcmp(StringValuePtr(name), "interval") == 0) {

        rb_hash_aset(header, ID2SYM(rb_intern("interval")), value);
        cdb->header->interval = rb_num2ull(value);
        cdb->synced = false;

    } else if (strcmp(StringValuePtr(name), "units") == 0) {

        rb_hash_aset(header, ID2SYM(rb_intern("units")), value);
        strncpy(cdb->header->units, StringValuePtr(value), sizeof(cdb->header->units));
        cdb->synced = false;
    }

    return value;
}

cdb_request_t _parse_cdb_request(VALUE start, VALUE end, VALUE count, VALUE cooked, VALUE step) {

    cdb_request_t request = cdb_new_request();

    /* cooked defaults to true from cdb_new_request() */
    if (NIL_P(cooked) || cooked == Qfalse) {
        request.cooked = false;
    }

    if (!NIL_P(start)) request.start = NUM2UINT(start);
    if (!NIL_P(end))   request.end   = NUM2UINT(end);
    if (!NIL_P(count)) request.count = rb_num2ull(count);
    if (!NIL_P(step))  request.step  = NUM2LONG(step);

    return request;
}

static VALUE cdb_rb_read_records(int argc, VALUE *argv, VALUE self) {

    VALUE start, end, count, cooked, step, array;

    uint64_t i   = 0;
    uint64_t cnt = 0;
    int      ret = 0;

    cdb_t *cdb;
    cdb_range_t *range    = _new_statistics(self);
    cdb_record_t *records = NULL;

    rb_scan_args(argc, argv, "05", &start, &end, &count, &cooked, &step);

    cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

    Data_Get_Struct(self, cdb_t, cdb);

    ret = cdb_read_records(cdb, &request, &cnt, &records, range);

    _check_return(ret);

    array = rb_ary_new2(cnt);

    for (i = 0; i < cnt; i++) {
        rb_ary_store(array, i, rb_ary_new3(2, ULONG2NUM(records[i].time), rb_float_new(records[i].value)));
    }

    free(records);

    _cdb_rb_update_header_hash(self, cdb);

    return array;
}

/* get back the data in a way that's friendly to gnuplot.rb */
static VALUE cdb_rb_read_records_cp(int argc, VALUE *argv, VALUE self) {

    VALUE start, end, count, cooked, step, aggregate, array, x, y;

    uint64_t i   = 0;
    uint64_t cnt = 0;
    uint64_t agg = 0;
    int      ret = 0;

    cdb_t *cdb;
    cdb_range_t *range    = _new_statistics(self);
    cdb_record_t *records = NULL;

    rb_scan_args(argc, argv, "06", &start, &end, &count, &cooked, &step, &aggregate);

    cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

    Data_Get_Struct(self, cdb_t, cdb);

    agg = rb_num2ull(aggregate);
    ret = cdb_read_records(cdb, &request, &cnt, &records, range);

    _check_return(ret);

    x = rb_ary_new2(cnt);
    y = rb_ary_new2(cnt);

    for (i = 0; i < cnt; i++) {
        rb_ary_store(x, i, ULONG2NUM(records[i].time));
        rb_ary_store(y, i, rb_float_new(records[i].value / agg));
    }

    array = rb_ary_new3(2, x, y);

    free(records);

    _cdb_rb_update_header_hash(self, cdb);

    return array;
}

static VALUE _parse_time(VALUE time) {

    if (rb_obj_is_kind_of(time, mTime)) {
        time = rb_funcall(time, s_to_i, 0);
    }

    return NUM2ULONG(time);
}

/* Helper functions for write_* and update_* since they do almost the same * thing. */
static VALUE _cdb_write_or_update_records(VALUE self, VALUE array, int type) {

    uint64_t len = RARRAY(array)->len;
    uint64_t i   = 0;
    uint64_t cnt = 0;
    bool     ret = false;

    cdb_t *cdb;
    cdb_record_t *records = ALLOCA_N(cdb_record_t, len);

    Data_Get_Struct(self, cdb_t, cdb);

    /* Turn any nil's into NaN */
    for (i = 0; i < len; i++) {
        VALUE record = rb_ary_entry(array, i);
        VALUE value  = rb_ary_entry(record, 1);

        records[i].time  = _parse_time(rb_ary_entry(record, 0));
        records[i].value = value == Qnil ? CDB_NAN : NUM2DBL(value);
    }

    if (type == RCDB_WRITE) {
        ret = cdb_write_records(cdb, records, len, &cnt);
    } else {
        ret = cdb_update_records(cdb, records, len, &cnt);
    }

    if (ret != CDB_SUCCESS) {
        rb_sys_fail(0);
    }

    _cdb_rb_update_header_hash(self, cdb);

    return ULL2NUM(cnt);
}

static VALUE _cdb_write_or_update_record(VALUE self, VALUE time, VALUE value, int type) {

    bool ret = false;

    cdb_t *cdb;

    Data_Get_Struct(self, cdb_t, cdb);

    /* Convert nil to NAN, which is what the circulardb code expects */
    if (value == Qnil) {
        value = rb_float_new(CDB_NAN);
    }

    if (type == RCDB_WRITE) {
        ret = cdb_write_record(cdb, _parse_time(time), NUM2DBL(value));
    } else {
        ret = cdb_update_record(cdb, _parse_time(time), NUM2DBL(value));
    }

    if (ret == false) {
        rb_sys_fail(0);
    }

    _cdb_rb_update_header_hash(self, cdb);

    return ULL2NUM((int)ret);
}

static VALUE cdb_rb_write_records(VALUE self, VALUE array) {

    return _cdb_write_or_update_records(self, array, RCDB_WRITE);
}

static VALUE cdb_rb_write_record(VALUE self, VALUE time, VALUE value) {

    return _cdb_write_or_update_record(self, time, value, RCDB_WRITE);
}

static VALUE cdb_rb_update_records(VALUE self, VALUE array) {

    return _cdb_write_or_update_records(self, array, RCDB_UPDATE);
}

static VALUE cdb_rb_update_record(VALUE self, VALUE time, VALUE value) {

    return _cdb_write_or_update_record(self, time, value, RCDB_UPDATE);
}

static VALUE cdb_rb_discard_records_in_time_range(VALUE self, VALUE start_time, VALUE end_time) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);
    uint64_t cnt = 0;

    cdb_request_t request = _parse_cdb_request(start_time, end_time, Qnil, Qnil, Qnil);

    int ret = cdb_discard_records_in_time_range(cdb, &request, &cnt);

    _check_return(ret);

    return ULL2NUM(cnt);
}

static VALUE cdb_rb_open_cdb(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_open(cdb) != CDB_SUCCESS) {
        rb_sys_fail(0);
    }

    return self;
}

static VALUE cdb_rb_close_cdb(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_close(cdb) != CDB_SUCCESS) {
        rb_sys_fail(0);
    }

    return self;
}

static VALUE cdb_rb_read_header(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_read_header(cdb) != CDB_SUCCESS) {
        rb_sys_fail(0);
    }

    _cdb_rb_update_header_hash(self, cdb);

    return self;
}

static VALUE cdb_rb_write_header(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_write_header(cdb) != CDB_SUCCESS) {
        rb_sys_fail(0);
    }

    return self;
}

static VALUE cdb_rb_print(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    cdb_print(cdb);

    return self;
}

static VALUE cdb_rb_print_header(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    cdb_print_header(cdb);

    return self;
}

static VALUE cdb_rb_print_records(int argc, VALUE *argv, VALUE self) {

    VALUE start, end, count, file_obj, date_format, cooked, step;
    cdb_t *cdb;

    rb_scan_args(argc, argv, "06", &start, &end, &count, &file_obj, &date_format, &cooked, &step);

    cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

    if (NIL_P(date_format)) {
        date_format = rb_str_new2("");
    }

    if (NIL_P(file_obj)) {
        file_obj = rb_const_get(cStorage, rb_intern("STDERR"));
    }

    Data_Get_Struct(self, cdb_t, cdb);

    cdb_print_records(cdb, &request, RFILE(file_obj)->fptr->f, StringValuePtr(date_format));

    return self;
}

static VALUE cdb_rb_statistics(int argc, VALUE *argv, VALUE self) {
    VALUE statistics = rb_iv_get(self, "@statistics");

    /* Do a read of the full range to populate */
    if (statistics == Qnil) {
        cdb_rb_read_records(argc, argv, self);
        statistics = rb_iv_get(self, "@statistics");
    }

    return statistics;
}

/* Implement a a zip map write to stay out of Ruby thread context switching. */
static int _process_each_record(VALUE uuid, VALUE values, VALUE args) {

    if (uuid == Qnil) {
        rb_raise(rb_eRuntimeError, "Empty or null UUID.");
    }

    VALUE basepath = rb_ary_entry(args, 0);
    VALUE array    = rb_funcall(rb_ary_entry(args, 1), rb_intern("zip"), 1, values);

    uint64_t len = RARRAY(array)->len;
    uint64_t i   = 0;
    uint64_t cnt = 0;
    bool     ret = false;
    int  pathlen = RSTRING(basepath)->len + RSTRING(uuid)->len + 2;
    uint64_t *t  = (uint64_t*)rb_ary_entry(args, 2);

    cdb_t *cdb = cdb_new();
    cdb->flags = O_RDWR|O_CREAT;
    cdb->mode  = -1;

    cdb_record_t *records = ALLOCA_N(cdb_record_t, len);
    cdb->filename         = ALLOCA_N(char, pathlen);

    snprintf(cdb->filename, pathlen, "%s/%s", StringValuePtr(basepath), StringValuePtr(uuid));

    /* Try the open */
    if (cdb_open(cdb) != CDB_SUCCESS) {
        cdb_free(cdb);
        rb_sys_fail(0);
    }

    /* Turn any nil's into NaN */
    for (i = 0; i < len; i++) {
        VALUE record = rb_ary_entry(array, i);
        VALUE value  = rb_ary_entry(record, 1);

        records[i].time  = NUM2ULONG(rb_ary_entry(record, 0));
        records[i].value = value == Qnil ? CDB_NAN : NUM2DBL(value);
    }

    ret = cdb_write_records(cdb, records, len, &cnt);

    if (ret != CDB_SUCCESS || cdb_close(cdb) != CDB_SUCCESS) {
        cdb_free(cdb);
        rb_sys_fail(0);
    }

    cdb_free(cdb);

    *t += cnt;

    return Qtrue;
}

static VALUE cdb_rb_write_zip_map(VALUE self, VALUE basepath, VALUE timestamps, VALUE records_map) {

    uint64_t written = 0;

    VALUE args = rb_ary_new2(3);

    rb_ary_store(args, 0, basepath);
    rb_ary_store(args, 1, timestamps);
    rb_ary_store(args, 2, (VALUE)&written);

    rb_hash_foreach(records_map, _process_each_record, args);

    return INT2FIX(written);
}

/****************************************************************************************
 * Aggregate functions
 ****************************************************************************************/
static VALUE cdb_agg_rb_initialize(VALUE self, VALUE name) {

    rb_iv_set(self, "@name", name);
    rb_iv_set(self, "@cdbs", rb_ary_new());
    rb_iv_set(self, "@statistics", Qnil);

    return self;
}

static VALUE cdb_agg_rb_read_records(int argc, VALUE *argv, VALUE self) {

    VALUE start, end, count, cooked, step, array;
    VALUE cdb_objects = rb_iv_get(self, "@cdbs");

    uint64_t i   = 0;
    uint64_t cnt = 0;
    int ret      = 0;
    int num_cdbs = RARRAY(cdb_objects)->len;

    /* initialize the cdbs array to the size of the cdb_objects array */
    cdb_t *cdbs[num_cdbs];
    cdb_record_t *records = NULL;
    cdb_range_t *range    = _new_statistics(self);

    rb_scan_args(argc, argv, "05", &start, &end, &count, &cooked, &step);

    cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

    /* First, loop over the incoming array of CircularDB::Storage objects and
     * extract the pointers to the cdb_t structs. */
    for (i = 0; i < num_cdbs; i++) {
        Data_Get_Struct(RARRAY(cdb_objects)->ptr[i], cdb_t, cdbs[i]);
    }

    ret = cdb_read_aggregate_records(cdbs, num_cdbs, &request, &cnt, &records, range);

    _check_return(ret);

    array = rb_ary_new2(cnt);

    for (i = 0; i < cnt; i++) {
        rb_ary_store(array, i, rb_ary_new3(2, ULONG2NUM(records[i].time), rb_float_new(records[i].value)));
    }

    free(records);

    return array;
}

static VALUE cdb_agg_rb_print_records(int argc, VALUE *argv, VALUE self) {

    VALUE start, end, count, file_obj, date_format, cooked, step;
    VALUE cdb_objects = rb_iv_get(self, "@cdbs");

    uint64_t i   = 0;
    int num_cdbs = RARRAY(cdb_objects)->len;

    /* initialize the cdbs array to the size of the cdb_objects array */
    cdb_t *cdbs[num_cdbs];

    rb_scan_args(argc, argv, "07", &start, &end, &count, &file_obj, &date_format, &cooked, &step);

    cdb_request_t request = _parse_cdb_request(start, end, count, cooked, step);

    if (NIL_P(date_format)) {
        date_format = rb_str_new2("");
    }

    if (NIL_P(file_obj)) {
        file_obj = rb_const_get(cStorage, rb_intern("STDERR"));
    }

    /* First, loop over the incoming array of CircularDB::Storage objects and
     * extract the pointers to the cdb_t structs. */
    for (i = 0; i < num_cdbs; i++) {
        Data_Get_Struct(RARRAY(cdb_objects)->ptr[i], cdb_t, cdbs[i]);
    }

    cdb_print_aggregate_records(cdbs, num_cdbs, &request, RFILE(file_obj)->fptr->f, StringValuePtr(date_format));

    return self;
}

static VALUE cdb_agg_rb_statistics(int argc, VALUE *argv, VALUE self) {
    VALUE statistics = rb_iv_get(self, "@statistics");

    /* Do a read of the full range to populate */
    if (statistics == Qnil) {
        cdb_agg_rb_read_records(argc, argv, self);
        statistics = rb_iv_get(self, "@statistics");
    }

    return statistics;
}

/****************************************************************************************
 * Statistics functions
 ****************************************************************************************/
static VALUE _cdb_rb_get_statistics(VALUE self, cdb_statistics_enum_t type) {
    cdb_range_t *range;

    Data_Get_Struct(self, cdb_range_t, range);

    return rb_float_new(cdb_get_statistic(range, type));
}

static VALUE statistics_median(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_MEDIAN);
}

static VALUE statistics_mad(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_MAD);
}

static VALUE statistics_average(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_MEAN);
}

static VALUE statistics_mean(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_MEAN);
}

static VALUE statistics_sum(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_SUM);
}

static VALUE statistics_min(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_MIN);
}

static VALUE statistics_max(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_MAX);
}

static VALUE statistics_stddev(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_STDDEV);
}

static VALUE statistics_absdev(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_ABSDEV);
}

static VALUE statistics_95th(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_95TH);
}

static VALUE statistics_75th(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_75TH);
}

static VALUE statistics_50th(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_50TH);
}

static VALUE statistics_25th(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_25TH);
}

/****************************************************************************************
 * Ruby Class / Method glue
 ****************************************************************************************/

/*
 *  Document-class: mCircularDB
 *  This is CircularDB 
 */
void Init_circulardb_ext() {

    mCircularDB = rb_const_get(rb_cObject, rb_intern("CircularDB"));
    cStorage    = rb_define_class_under(mCircularDB, "Storage", rb_cObject);
    cAggregate  = rb_define_class_under(mCircularDB, "Aggregate", rb_cObject);
    cStatistics = rb_define_class_under(mCircularDB, "Statistics", rb_cObject);

    /* CircularDB::Storage class */
    rb_define_alloc_func(cStorage, cdb_rb_alloc);
    rb_define_method(cStorage, "initialize", cdb_rb_initialize, -1); 
    rb_define_method(cStorage, "open", cdb_rb_open_cdb, 0); 
    rb_define_method(cStorage, "close", cdb_rb_close_cdb, 0); 
    rb_define_method(cStorage, "read_header", cdb_rb_read_header, 0); 
    rb_define_method(cStorage, "read_records", cdb_rb_read_records, -1); 
    rb_define_method(cStorage, "read_records_gp", cdb_rb_read_records_cp, -1); 
    rb_define_method(cStorage, "write_header", cdb_rb_write_header, 0); 
    rb_define_method(cStorage, "write_record", cdb_rb_write_record, 2); 
    rb_define_method(cStorage, "write_records", cdb_rb_write_records, 1); 
    rb_define_method(cStorage, "update_record", cdb_rb_update_record, 2); 
    rb_define_method(cStorage, "update_records", cdb_rb_update_records, 1); 
    rb_define_method(cStorage, "discard_records_in_time_range", cdb_rb_discard_records_in_time_range, 2); 
    rb_define_method(cStorage, "print", cdb_rb_print, 0); 
    rb_define_method(cStorage, "print_header", cdb_rb_print_header, 0); 
    rb_define_method(cStorage, "print_records", cdb_rb_print_records, -1); 
    rb_define_method(cStorage, "_set_header", _set_header, 2); 
    rb_define_method(cStorage, "statistics", cdb_rb_statistics, -1); 

    /* Class method for fast writing */
    rb_define_module_function(cStorage, "write_zip_map", cdb_rb_write_zip_map, 3);

    /* CircularDB::Aggregate class */
    rb_define_method(cAggregate, "initialize", cdb_agg_rb_initialize, 1); 
    rb_define_method(cAggregate, "read_records", cdb_agg_rb_read_records, -1); 
    rb_define_method(cAggregate, "print_records", cdb_agg_rb_print_records, -1); 
    rb_define_method(cAggregate, "statistics", cdb_agg_rb_statistics, -1); 

    /* CircularDB::Statistics class */
    rb_define_alloc_func(cStatistics, cdb_statistics_rb_alloc);
    rb_define_method(cStatistics, "median", statistics_median, 0); 
    rb_define_method(cStatistics, "mad", statistics_mad, 0); 
    rb_define_method(cStatistics, "average", statistics_average, 0); 
    rb_define_method(cStatistics, "mean", statistics_mean, 0); 
    rb_define_method(cStatistics, "sum", statistics_sum, 0); 
    rb_define_method(cStatistics, "min", statistics_min, 0); 
    rb_define_method(cStatistics, "max", statistics_max, 0); 
    rb_define_method(cStatistics, "stddev", statistics_stddev, 0); 
    rb_define_method(cStatistics, "absdev", statistics_absdev, 0); 
    rb_define_method(cStatistics, "pct95th", statistics_95th, 0); 
    rb_define_method(cStatistics, "pct75th", statistics_75th, 0); 
    rb_define_method(cStatistics, "pct50th", statistics_50th, 0); 
    rb_define_method(cStatistics, "pct25th", statistics_25th, 0); 

    /* To handle Time objects */
    mTime = rb_const_get(rb_cObject, rb_intern("Time"));
    s_to_i = rb_intern("to_i");
}

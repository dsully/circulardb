/* $Id: circulardb_ext.c 16 2007-09-04 21:06:11Z dsully $ 
 *
 * Ruby bindings for CircularDB
 *
 * */

#include <ruby.h>
#include <rubyio.h>

#include <circulardb.h>

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

static VALUE cdb_rb_initialize(int argc, VALUE *argv, VALUE self) {

    cdb_t *cdb;
    VALUE header = rb_hash_new();
    VALUE filename, flags, mode, name, max_records, num_records, type, units, desc, last_updated;

    char *regex = ALLOCA_N(char, 4);

    rb_scan_args(argc, argv, "17", &filename, &flags, &mode, &name, &max_records, &type, &units, &desc);

    Data_Get_Struct(self, cdb_t, cdb);

    /* Add the default extension if it doesn't exist. */
    if (sprintf(regex, "\\.%s", CDB_EXTENSION)) {

        VALUE re = rb_reg_new(regex, strlen(regex), 0);

        if (!RTEST(rb_reg_match(re, filename))) {

            /* lazy */
            filename = rb_str_cat2(filename, ".");
            filename = rb_str_cat2(filename, CDB_EXTENSION);
        }
    }

    cdb->filename = StringValuePtr(filename);

    rb_hash_aset(header, ID2SYM(rb_intern("filename")), filename);

    if (NIL_P(flags)) {
        cdb->flags = -1;
    } else {
        cdb->flags = NUM2INT(flags);
    }

    if (NIL_P(mode)) {
        cdb->mode = -1;
    } else {
        cdb->mode = NUM2INT(mode);
    }

    /* Try the open */
    if (cdb_open(cdb) > 0) {
        rb_raise(rb_eIOError, strerror(errno));
    }

    /* Try and read a header, if it doesn't exist, create one */
    if (NIL_P(name)) {

        if (cdb_read_header(cdb) > 0) {
            rb_raise(rb_eIOError, strerror(errno));
        }

        name         = rb_str_new2(cdb->header->name);
        type         = rb_str_new2(cdb->header->type);
        units        = rb_str_new2(cdb->header->units);
        desc         = rb_str_new2(cdb->header->description);
        max_records  = ULL2NUM(cdb->header->max_records);
        num_records  = ULL2NUM(cdb->header->num_records);
        last_updated = INT2FIX(cdb->header->last_updated);

    } else {

        if (NIL_P(max_records)) max_records = INT2FIX(0);
        if (NIL_P(type))  type  = rb_str_new2(CDB_DEFAULT_DATA_TYPE);
        if (NIL_P(units)) units = rb_str_new2(CDB_DEFAULT_DATA_UNIT);
        if (NIL_P(desc))  desc  = rb_str_new2("");

        cdb_generate_header(cdb,
            StringValuePtr(name),
            rb_num2ull(max_records),
            StringValuePtr(type),
            StringValuePtr(units),
            StringValuePtr(desc)
        );

        num_records = INT2FIX(0);

        if (cdb_write_header(cdb) > 0) {
            rb_raise(rb_eIOError, strerror(errno));
        }
    }

    rb_hash_aset(header, ID2SYM(rb_intern("name")), name);
    rb_hash_aset(header, ID2SYM(rb_intern("type")), type);
    rb_hash_aset(header, ID2SYM(rb_intern("units")), units);
    rb_hash_aset(header, ID2SYM(rb_intern("num_records")), num_records);
    rb_hash_aset(header, ID2SYM(rb_intern("max_records")), max_records);
    rb_hash_aset(header, ID2SYM(rb_intern("description")), desc);

    if (!NIL_P(last_updated)) {
        rb_hash_aset(header, ID2SYM(rb_intern("last_updated")), last_updated);
    }

    rb_iv_set(self, "@header", header);
    rb_iv_set(self, "@statistics", Qnil);

    return self;
}

static VALUE _set_header(VALUE self, VALUE name, VALUE value) {

    cdb_t *cdb;
    VALUE header = rb_iv_get(self, "@header");

    Data_Get_Struct(self, cdb_t, cdb);

    if (strcmp(StringValuePtr(name), "description") == 0) {

        rb_hash_aset(header, ID2SYM(rb_intern("description")), value);
        strncpy(cdb->header->description, StringValuePtr(value), sizeof(cdb->header->description));
        cdb->synced = 0;

    } else if (strcmp(StringValuePtr(name), "units") == 0) {

        rb_hash_aset(header, ID2SYM(rb_intern("units")), value);
        strncpy(cdb->header->units, StringValuePtr(value), sizeof(cdb->header->units));
        cdb->synced = 0;
    }

    return value;
}

static VALUE cdb_rb_read_records(int argc, VALUE *argv, VALUE self) {

    VALUE start, end, num_req, cooked, array;

    uint64_t i   = 0;
    uint64_t cnt = 0;
    time_t first_time, last_time;

    cdb_t *cdb;
    cdb_range_t *range    = _new_statistics(self);
    cdb_record_t *records = NULL;

    rb_scan_args(argc, argv, "04", &start, &end, &num_req, &cooked);

    if (NIL_P(start))   start   = INT2FIX(0);
    if (NIL_P(end))     end     = INT2FIX(0);
    if (NIL_P(num_req)) num_req = INT2FIX(0);
    if (NIL_P(cooked))   cooked = INT2FIX(0);

    Data_Get_Struct(self, cdb_t, cdb);

    cnt = cdb_read_records(cdb,
        NUM2UINT(start),
        NUM2UINT(end),
        rb_num2ull(num_req),
        cooked,
        &first_time,
        &last_time,
        &records,
        range
    );

    switch (cnt) {
        case -1: rb_raise(rb_eStandardError, "End time must be >= Start time.");
        case -2: rb_raise(rb_eStandardError, "Database is unsynced.");
    }

    array = rb_ary_new2(cnt);

    for (i = 0; i < cnt; i++) {
        rb_ary_store(array, i, rb_ary_new3(2, ULONG2NUM(records[i].time), rb_float_new(records[i].value)));
    }

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

    if (type == 1) {
        cnt = cdb_write_records(cdb, records, len);
    } else {
        cnt = cdb_update_records(cdb, records, len);
    }

    if (cnt == 0) {
        rb_raise(rb_eIOError, strerror(errno));
    }

    _cdb_rb_update_header_hash(self, cdb);

    return ULL2NUM(cnt);
}

static VALUE _cdb_write_or_update_record(VALUE self, VALUE time, VALUE value, int type) {

    uint64_t cnt = 0;

    cdb_t *cdb;

    Data_Get_Struct(self, cdb_t, cdb);

    /* Convert nil to NAN, which is what the circulardb code expects */
    if (value == Qnil) {
        value = rb_float_new(CDB_NAN);
    }

    if (type == 1) {
        cnt = cdb_write_record(cdb, _parse_time(time), NUM2DBL(value));
    } else {
        cnt = cdb_update_record(cdb, _parse_time(time), NUM2DBL(value));
    }

    if (cnt == 0) {
        rb_raise(rb_eIOError, strerror(errno));
    }

    _cdb_rb_update_header_hash(self, cdb);

    return ULL2NUM(cnt);
}

static VALUE cdb_rb_write_records(VALUE self, VALUE array) {

    return _cdb_write_or_update_records(self, array, 1);
}

static VALUE cdb_rb_write_record(VALUE self, VALUE time, VALUE value) {

    return _cdb_write_or_update_record(self, time, value, 1);
}

static VALUE cdb_rb_update_records(VALUE self, VALUE array) {

    return _cdb_write_or_update_records(self, array, 2);
}

static VALUE cdb_rb_update_record(VALUE self, VALUE time, VALUE value) {

    return _cdb_write_or_update_record(self, time, value, 2);
}

static VALUE cdb_rb_discard_records_in_time_range(VALUE self, VALUE start_time, VALUE end_time) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    return ULL2NUM(cdb_discard_records_in_time_range(cdb, NUM2ULONG(start_time), NUM2ULONG(end_time)));
}

static VALUE cdb_rb_open_cdb(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_open(cdb) > 0) {
        rb_raise(rb_eIOError, strerror(errno));
    }

    return self;
}

static VALUE cdb_rb_close_cdb(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_close(cdb) > 0) {
        rb_raise(rb_eIOError, strerror(errno));
    }

    return self;
}

static VALUE cdb_rb_read_header(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_read_header(cdb) > 0) {
        rb_raise(rb_eIOError, strerror(errno));
    }

    _cdb_rb_update_header_hash(self, cdb);

    return self;
}

static VALUE cdb_rb_write_header(VALUE self) {

    cdb_t *cdb;
    Data_Get_Struct(self, cdb_t, cdb);

    if (cdb_write_header(cdb) > 0) {
        rb_raise(rb_eIOError, strerror(errno));
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

    VALUE start, end, num_req, file_obj, date_format, cooked, array;
    time_t first_time, last_time;
    cdb_t *cdb;

    rb_scan_args(argc, argv, "06", &start, &end, &num_req, &file_obj, &date_format, &cooked);

    if (NIL_P(start))   start   = INT2FIX(0);
    if (NIL_P(end))     end     = INT2FIX(0);
    if (NIL_P(num_req)) num_req = INT2FIX(0);
    if (NIL_P(cooked)) cooked   = INT2FIX(0);

    if (NIL_P(date_format)) {
        date_format = rb_str_new2("");
    }

    if (NIL_P(file_obj)) {
        file_obj = rb_const_get(cStorage, rb_intern("STDERR"));
    }

    Data_Get_Struct(self, cdb_t, cdb);

    cdb_print_records(
        cdb, 
        NUM2UINT(start),
        NUM2UINT(end),
        rb_num2ull(num_req),
        RFILE(file_obj)->fptr->f,
        StringValuePtr(date_format),
        NUM2UINT(cooked),
        &first_time,
        &last_time
    );

    array = rb_ary_new2(2);
    rb_ary_store(array, 0, UINT2NUM(first_time));
    rb_ary_store(array, 1, UINT2NUM(last_time));

    return array;
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

    VALUE start, end, num_req, cooked, array;
    VALUE cdb_objects = rb_iv_get(self, "@cdbs");

    uint64_t i   = 0;
    uint64_t cnt = 0;
    int num_cdbs = RARRAY(cdb_objects)->len;

    /* initialize the cdbs array to the size of the cdb_objects array */
    cdb_t *cdbs[num_cdbs];
    cdb_record_t *records = NULL;
    cdb_range_t *range    = _new_statistics(self);
    time_t first_time, last_time;

    rb_scan_args(argc, argv, "04", &start, &end, &num_req, &cooked);

    if (NIL_P(start))   start   = INT2FIX(0);
    if (NIL_P(end))     end     = INT2FIX(0);
    if (NIL_P(num_req)) num_req = INT2FIX(0);
    if (NIL_P(cooked))  cooked  = INT2FIX(0);

    /* First, loop over the incoming array of CircularDB::Storage objects and
     * extract the pointers to the cdb_t structs. */
    for (i = 0; i < num_cdbs; i++) {
        Data_Get_Struct(RARRAY(cdb_objects)->ptr[i], cdb_t, cdbs[i]);
    }

    cnt = cdb_read_aggregate_records(
        cdbs,
        num_cdbs,
        NUM2UINT(start),
        NUM2UINT(end),
        rb_num2ull(num_req),
        NUM2UINT(cooked),
        &first_time,
        &last_time,
        &records,
        range
    );

    array = rb_ary_new2(cnt);

    for (i = 0; i < cnt; i++) {
        rb_ary_store(array, i, rb_ary_new3(2, ULONG2NUM(records[i].time), rb_float_new(records[i].value)));
    }

    free(records);

    return array;
}

static VALUE cdb_agg_rb_print_records(int argc, VALUE *argv, VALUE self) {

    VALUE start, end, num_req, file_obj, date_format, cooked, array;
    VALUE cdb_objects = rb_iv_get(self, "@cdbs");

    uint64_t i   = 0;
    int num_cdbs = RARRAY(cdb_objects)->len;

    /* initialize the cdbs array to the size of the cdb_objects array */
    cdb_t *cdbs[num_cdbs];
    time_t first_time, last_time;

    rb_scan_args(argc, argv, "06", &start, &end, &num_req, &file_obj, &date_format, &cooked);

    if (NIL_P(start))   start   = INT2FIX(0);
    if (NIL_P(end))     end     = INT2FIX(0);
    if (NIL_P(num_req)) num_req = INT2FIX(0);
    if (NIL_P(cooked))  cooked  = INT2FIX(0);

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

    cdb_print_aggregate_records(
        cdbs,
        num_cdbs,
        NUM2UINT(start),
        NUM2UINT(end),
        rb_num2ull(num_req),
        RFILE(file_obj)->fptr->f,
        StringValuePtr(date_format),
        NUM2UINT(cooked),
        &first_time,
        &last_time
    );

    array = rb_ary_new2(2);
    rb_ary_store(array, 0, UINT2NUM(first_time));
    rb_ary_store(array, 1, UINT2NUM(last_time));

    return array;
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

static VALUE statistics_variance(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_VARIANCE);
}

static VALUE statistics_skew(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_SKEW);
}

static VALUE statistics_kurtosis(VALUE self) {
    return _cdb_rb_get_statistics(self, CDB_KURTOSIS);
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
    rb_define_method(cStatistics, "variance", statistics_variance, 0); 
    rb_define_method(cStatistics, "skew", statistics_skew, 0); 
    rb_define_method(cStatistics, "kurtosis", statistics_kurtosis, 0); 
    rb_define_method(cStatistics, "pct95th", statistics_95th, 0); 
    rb_define_method(cStatistics, "pct75th", statistics_75th, 0); 
    rb_define_method(cStatistics, "pct50th", statistics_50th, 0); 
    rb_define_method(cStatistics, "pct25th", statistics_25th, 0); 

    /* To handle Time objects */
    mTime = rb_const_get(rb_cObject, rb_intern("Time"));
    s_to_i = rb_intern("to_i");
}

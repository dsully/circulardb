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

/* Cleanup after an object has been GCed */
static void cdb_rb_free(void *p) {
    cdb_free(p);
}

static VALUE cdb_rb_alloc(VALUE klass) { 

    cdb_t *cdb = cdb_new();

    VALUE obj = Data_Wrap_Struct(klass, 0, cdb_rb_free, cdb);

    return obj;
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
        &records
    );

    switch (cnt) {
        case -1: rb_raise(rb_eStandardError, "End time must be >= Start time.");
        case -2: rb_raise(rb_eStandardError, "Database is unsynced.");
    }

    array = rb_ary_new2(cnt);

    for (i = 0; i < cnt; i++) {

        VALUE entry = rb_ary_new2(2);

        rb_ary_store(entry, 0, ULONG2NUM(records[i].time));
        //rb_ary_store(entry, 1, rb_dbl2big(records[i].value));
        
        if (records[i].value == DBL_MIN) {
            rb_ary_store(entry, 1, Qnil);
        } else {
            rb_ary_store(entry, 1, rb_float_new(records[i].value));
        }

        rb_ary_store(array, i, entry);
    }

    free(records);

    _cdb_rb_update_header_hash(self, cdb);

    return array;
}

/* Helper functions for write_* and update_* since they do almost the same * thing. */
static VALUE _cdb_write_or_update_records(VALUE self, VALUE array, int type) {

    uint64_t len = RARRAY(array)->len;
    uint64_t i   = 0;
    uint64_t cnt = 0;

    cdb_t *cdb;
    cdb_record_t *records = ALLOCA_N(cdb_record_t, len);

    Data_Get_Struct(self, cdb_t, cdb);

    for (i = 0; i < len; i++) {
        VALUE record = rb_ary_entry(array, i);

        records[i].time  = NUM2ULONG(rb_ary_entry(record, 0));
        records[i].value = NUM2DBL(rb_ary_entry(record, 1));
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

    /* Convert nil to DBL_MIN, which is what the circulardb code expects */
    if (value == Qnil) {
        value = rb_float_new(DBL_MIN);
    }

    if (type == 1) {
        cnt = cdb_write_record(cdb, NUM2ULONG(time), NUM2DBL(value));
    } else {
        cnt = cdb_update_record(cdb, NUM2ULONG(time), NUM2DBL(value));
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

static VALUE cdb_rb_aggregate_using_function_for_records(int argc, VALUE *argv, VALUE self) {

    VALUE function, start, end, num_req;

    cdb_t *cdb;
    double ret = 0;

    rb_scan_args(argc, argv, "13", &function, &start, &end, &num_req);

    if (NIL_P(start))   start   = INT2FIX(0);
    if (NIL_P(end))     end     = INT2FIX(0);
    if (NIL_P(num_req)) num_req = INT2FIX(0);

    Data_Get_Struct(self, cdb_t, cdb);

    ret = cdb_aggregate_using_function_for_records(
        cdb, StringValuePtr(function), NUM2UINT(start), NUM2UINT(end), rb_num2ull(num_req)
    );

    return rb_float_new(ret);
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
        &records
    );

    array = rb_ary_new2(cnt);

    for (i = 0; i < cnt; i++) {

        VALUE entry = rb_ary_new2(2);

        rb_ary_store(entry, 0, ULONG2NUM(records[i].time));
        
        if (records[i].value == DBL_MIN) {
            rb_ary_store(entry, 1, Qnil);
        } else {
            rb_ary_store(entry, 1, rb_float_new(records[i].value));
        }

        rb_ary_store(array, i, entry);
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

void Init_circulardb_ext() {

    mCircularDB = rb_const_get(rb_cObject, rb_intern("CircularDB"));
    cStorage    = rb_define_class_under(mCircularDB, "Storage", rb_cObject);
    cAggregate  = rb_define_class_under(mCircularDB, "Aggregate", rb_cObject);

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
    rb_define_method(cStorage, "aggregate_using_function_for_records", cdb_rb_aggregate_using_function_for_records, -1); 
    rb_define_method(cStorage, "_set_header", _set_header, 2); 

    rb_define_method(cAggregate, "read_records", cdb_agg_rb_read_records, -1); 
    rb_define_method(cAggregate, "print_records", cdb_agg_rb_print_records, -1); 
}

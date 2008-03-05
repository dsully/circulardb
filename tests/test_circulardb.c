/*
 * testcirculardb
 *
 */

#ifndef LINT
static const char svnid[] __attribute__ ((unused)) = "$Id$";
#endif

#include <check.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../include/circulardb_interface.h"

#define TEST_FILENAME "/tmp/cdb_test.cdb"

void setup(void) {
    unlink(TEST_FILENAME);
}

void teardown(void) {
    unlink(TEST_FILENAME);
}

cdb_t* create_cdb(const char* type, const char* unit, uint64_t max) {
    cdb_t *cdb = cdb_new();

    if (max == 0) max = 500;

    cdb->filename = (char*)TEST_FILENAME;

    cdb->flags = O_CREAT|O_RDWR;
    cdb_open(cdb);
    cdb_generate_header(cdb, (char*)"test", max, (char*)type, (char*)unit, (char*)"");
    cdb_write_header(cdb);

    return cdb;
}

START_TEST (test_cdb_basic_create)
{
    cdb_t *cdb = create_cdb("gauge", "absolute", 0);

    if (!cdb) fail("cdb is null");

    fail_if(strcmp(cdb->header->name, "test") != 0, NULL);
    fail_if(strcmp(cdb->header->type, "gauge") != 0, NULL);
    fail_if(strcmp(cdb->header->units, "absolute") != 0, NULL);

    cdb_close(cdb);
    cdb_free(cdb);
}
END_TEST

START_TEST (test_cdb_basic_rw)
{
    int i = 0;
    time_t first_time, last_time;

    cdb_range_t *range        = calloc(1, sizeof(cdb_range_t));
    cdb_record_t w_records[RECORD_SIZE * 10];
    cdb_record_t *r_records = NULL;

    cdb_t *cdb = create_cdb("gauge", "absolute", 0);

    for (i = 0; i < 10; i++) {
        w_records[i].time  = i+1190860353;
        w_records[i].value = i+1;
    }

    if (!cdb) fail("cdb is null");

    fail_unless(cdb_write_records(cdb, w_records, 10) == 10, "Couldn't write 10 records");

    fail_unless(
        cdb_read_records(cdb, 0, 0, 0, 1, &first_time, &last_time, &r_records, range) == 10,
        "Couldn't read 10 records"
    );

    fail_unless(cdb->header->num_records == 10, "header count doesn't match");

    for (i = 0; i < 10; i++) {
        fail_unless(w_records[i].time == r_records[i].time, NULL);
        fail_unless(w_records[i].value == r_records[i].value, NULL);
    }

    fail_unless(cdb_get_statistic(range, CDB_MEAN) == 5.5, "Invalid MEAN");
    fail_unless(cdb_get_statistic(range, CDB_MEDIAN) == 5.5, "Invalid MEDIAN");
    fail_unless(cdb_get_statistic(range, CDB_SUM) == 55, "Invalid SUM");
    fail_unless(cdb_get_statistic(range, CDB_MAX) == 10, "Invalid MAX");
    fail_unless(cdb_get_statistic(range, CDB_MIN) == 1, "Invalid MIN");

    {
        /* Check setting a float and reading it back. */
        fail_unless(cdb_update_record(cdb, r_records[5].time, 999.0005) == 1, "Couldn't update record");

        range = NULL;
        r_records = NULL;
        cdb_read_records(cdb, 0, 0, 0, 1, &first_time, &last_time, &r_records, range);

        fail_unless(r_records[5].value == 999.0005, "Float check");
    }

    {
        range = NULL;
        r_records = NULL;
        uint64_t cnt = cdb_read_records(cdb, 0, 0, 4, 1, &first_time, &last_time, &r_records, range);

        fail_unless(cnt == 4, "Requested numreqs");
        fail_unless(r_records[0].value == 7, NULL);
        fail_unless(r_records[1].value == 8, NULL);
        fail_unless(r_records[2].value == 9, NULL);
        fail_unless(r_records[3].value == 10, NULL);
    }

    {
        range = NULL;
        r_records = NULL;
        uint64_t cnt = cdb_read_records(cdb, 0, 0, -4, 1, &first_time, &last_time, &r_records, range);

        fail_unless(cnt == 4, "Requested numreqs");
        fail_unless(r_records[0].value == 1, NULL);
        fail_unless(r_records[1].value == 2, NULL);
        fail_unless(r_records[2].value == 3, NULL);
        fail_unless(r_records[3].value == 4, NULL);
    }

    {
        range = NULL;
        r_records = NULL;
        uint64_t cnt = cdb_read_records(cdb, 1190860355, 0, 0, 1, &first_time, &last_time, &r_records, range);

        fail_unless(cnt == 8, "Requested numreqs");
        fail_unless(r_records[0].time == 1190860355, NULL);
        fail_unless(r_records[1].time == 1190860356, NULL);
    }

    {
        range = NULL;
        r_records = NULL;
        uint64_t cnt = cdb_read_records(cdb, 0, 1190860355, 0, 1, &first_time, &last_time, &r_records, range);

        fail_unless(cnt == 3, "Requested numreqs");
        fail_unless(r_records[0].time == 1190860353, NULL);
        fail_unless(r_records[1].time == 1190860354, NULL);
        fail_unless(r_records[2].time == 1190860355, NULL);
    }

    {
        range = NULL;
        r_records = NULL;
        uint64_t cnt = cdb_read_records(cdb, 1190860353, 1190860355, 0, 1, &first_time, &last_time, &r_records, range);

        fail_unless(cnt == 2, "Requested numreqs");
        fail_unless(r_records[0].time == 1190860354, NULL);
        fail_unless(r_records[1].time == 1190860355, NULL);
    }

    {
        range = NULL;
        r_records = NULL;
        uint64_t cnt = cdb_read_records(cdb, 1190860353, 1190860360, -1, 1, &first_time, &last_time, &r_records, range);

        fail_unless(cnt == 1, "Requested numreqs");
        fail_unless(r_records[0].time == 1190860354, NULL);
    }

    free(range);
    free(r_records);
    cdb_close(cdb);
    cdb_free(cdb);
}
END_TEST

START_TEST (test_cdb_aggregate_basic)
{
/*
    int i = 0;
    time_t first_time, last_time;

    cdb_range_t *range        = calloc(1, sizeof(cdb_range_t));
    cdb_record_t w_records[RECORD_SIZE * 10];
    cdb_record_t *r_records = NULL;

    cdb_t *cdb = create_cdb("gauge", "absolute", 0);

    for (i = 0; i < 10; i++) {
        w_records[i].time  = i+1190860353;
        w_records[i].value = i+1;
    }

    if (!cdb) fail("cdb is null");

    fail_unless(cdb_write_records(cdb, w_records, 10) == 10, "Couldn't write 10 records");

    fail_unless(
        cdb_read_records(cdb, 0, 0, 0, 1, &first_time, &last_time, &r_records, range) == 10,
        "Couldn't read 10 records"
    );

    times = (0..10).collect { |i| Time.now.to_i + i }

    agg = CircularDB::Aggregate.new("test")
    assert(agg)

    (1..3).each do |i|

      records = Array.new

      (1..10).each { |j| records.push([ times[j], j.to_f ]) }

      cdb = CircularDB::Storage.new(File.join(@tempdir, "#{i}.cdb"), File::CREAT|File::RDWR|File::EXCL, nil, @name)
      cdb.write_records(records)
      agg.cdbs << cdb
    end

    read = agg.read_records

    for (i = 0; i < num_cdbs; i++) {
        Data_Get_Struct(RARRAY(cdb_objects)->ptr[i], cdb_t, cdbs[i]);
    }

    cnt = cdb_read_aggregate_records(
        cdbs, num_cdbs, start, end, num_req, cooked, &first_time, &last_time, &records, range
    );

    assert_equal(10, read.length)
    assert_equal(10, agg.num_records)

    (0..9).each do |i|
      assert_equal((i+1)*3, read[i][1])
    end

    assert_equal(165, agg.statistics.sum)
    assert_equal(3.0, agg.statistics.min)
    assert_equal(30.0, agg.statistics.max)
    assert_equal(16.5, agg.statistics.median)
    assert_equal(16.5, agg.statistics.mean)
*/
}
END_TEST

START_TEST (test_cdb_overflow)
{
    cdb_record_t *r_records = NULL;
    cdb_range_t *range      = calloc(1, sizeof(cdb_range_t));
    time_t first_time, last_time;

    cdb_t *cdb = create_cdb("counter", "requests per sec", 0);

    if (!cdb) fail("cdb is null");

    cdb_write_record(cdb, 1190860358, pow(2, 32));
    cdb_write_record(cdb, 1190860364, 10);
    cdb_write_record(cdb, 1190860365, 12);

    fail_unless(
        cdb_read_records(cdb, 0, 0, 0, 1, &first_time, &last_time, &r_records, range) == 2,
        "Couldn't read 2 records"
    );

    fail_unless(isnan(r_records[0].value));
    fail_unless(r_records[1].value == 2);

    // check that sum isn't nan

    free(range);
    free(r_records);
    cdb_close(cdb);
    cdb_free(cdb);
}
END_TEST

START_TEST (test_cdb_wrap)
{
    cdb_record_t *r_records = NULL;
    cdb_range_t *range      = calloc(1, sizeof(cdb_range_t));
    time_t first_time, last_time;

    cdb_t *cdb = create_cdb("gauge", "percent", 5);

    if (!cdb) fail("cdb is null");

    cdb_write_record(cdb, 1190860358, 10);
    cdb_write_record(cdb, 1190860364, 12);
    cdb_write_record(cdb, 1190860365, 14);
    cdb_write_record(cdb, 1190860366, 16);
    cdb_write_record(cdb, 1190860367, 18);
    cdb_write_record(cdb, 1190860368, 20);

    fail_unless(
        cdb_read_records(cdb, 0, 0, 0, 1, &first_time, &last_time, &r_records, range) == 5,
        "Couldn't read 5 records"
    );

    fail_unless(r_records[0].value == 12);

    free(range);
    free(r_records);
    cdb_close(cdb);
    cdb_free(cdb);
}
END_TEST

Suite* cdb_suite (void) {
    Suite *s = suite_create("CircularDB");

    TCase *tc_core1 = tcase_create("Basic");
    tcase_add_checked_fixture(tc_core1, setup, teardown);
    tcase_add_test(tc_core1, test_cdb_basic_create);
    tcase_add_test(tc_core1, test_cdb_basic_rw);
    tcase_add_test(tc_core1, test_cdb_overflow);
    tcase_add_test(tc_core1, test_cdb_wrap);
    suite_add_tcase(s, tc_core1);

    TCase *tc_core2 = tcase_create("Aggregate");
    tcase_add_checked_fixture(tc_core2, setup, teardown);
    tcase_add_test(tc_core2, test_cdb_aggregate_basic);
    suite_add_tcase(s, tc_core2);

    return s;
}
         
int
main (void) {
    int number_failed;
    Suite *s = cdb_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

/* -*- Mode: C; tab-width: 4 -*- */
/* vim: set tabstop=4 expandtab shiftwidth=4: */

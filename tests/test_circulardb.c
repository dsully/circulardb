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

cdb_t* create_cdb(const char *file, const char* name, const char* type, const char* unit) {
  cdb_t *cdb = cdb_new();

  cdb->filename = (char*)file;
  unlink(cdb->filename);

  cdb->flags = O_CREAT|O_RDWR;
  cdb_open(cdb);
  cdb_generate_header(cdb, (char*)name, 7000, (char*)type, (char*)unit, (char*)"");
  cdb_write_header(cdb);

  return cdb;
}

START_TEST (test_cdb_basic_create)
{
  const char *name = "Testing";
  const char *file = "/tmp/cdb_test.cdb";
  const char *type = "gauge";
  const char *unit = "absolute";
  cdb_t *cdb = create_cdb(file, name, type, unit);

  if (!cdb) fail("cdb is null");

  fail_if(strcmp(cdb->header->name, name) != 0, NULL);
  fail_if(strcmp(cdb->header->type, type) != 0, NULL);
  fail_if(strcmp(cdb->header->units, unit) != 0, NULL);

  cdb_close(cdb);
  cdb_free(cdb);
  unlink(file);
}
END_TEST

START_TEST (test_cdb_basic_rw)
{
  const char *name = "Testing";
  const char *file = "/tmp/cdb_test.cdb";
  const char *type = "gauge";
  const char *unit = "absolute";

  int i = 0;
  time_t start_time, last_time;

  cdb_range_t *range    = calloc(1, sizeof(cdb_range_t));
  cdb_record_t w_records[RECORD_SIZE * 10];
  cdb_record_t *r_records = NULL;

  cdb_t *cdb = create_cdb(file, name, type, unit);

  for (i = 0; i < 10; i++) {
    w_records[i].time  = 1190860353+(i*1);
    w_records[i].value = i+1;
  }

  if (!cdb) fail("cdb is null");

  fail_unless(cdb_write_records(cdb, w_records, 10) == 10, "Couldn't write 10 records");

  fail_unless(
    cdb_read_records(cdb, 0, 0, 0, 1, &start_time, &last_time, &r_records, range) == 10,
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
    cdb_read_records(cdb, 0, 0, 0, 1, &start_time, &last_time, &r_records, range);

    fail_unless(r_records[5].value == 999.0005, "Float check");
  }

  free(range);
  free(r_records);
  cdb_close(cdb);
  cdb_free(cdb);
  unlink(file);
}
END_TEST


Suite* cdb_suite (void) {
  Suite *s = suite_create("CircularDB");

  TCase *tc_core = tcase_create("Basic");
  tcase_add_test(tc_core, test_cdb_basic_create);
  tcase_add_test(tc_core, test_cdb_basic_rw);
  suite_add_tcase(s, tc_core);

  return s;
}
     
int
main (void) 
{
  int number_failed;
  Suite *s = cdb_suite();
  SRunner *sr = srunner_create(s);
  srunner_run_all(sr, CK_NORMAL);
  number_failed = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

/* -*- Mode: C; tab-width: 2 -*- */
/* vim: set tabstop=2 expandtab shiftwidth=2: */

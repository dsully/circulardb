/*
 * CircularDB implementation for time series data.
 *
 * Copyright (c) 2007-2009 Powerset, Inc
 * Copyright (c) Dan Grillo, Manish Dubey, Dan Sully
 *
 * All rights reserved.
 */

#ifndef __CIRCULARDB_INTERFACE_H__
#define __CIRCULARDB_INTERFACE_H__

#ifdef __cplusplus
extern "C" {
#endif

#ifndef CDB_VERSION

#include <float.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

/* Add to ~/.magic:
 *
 * 0       string          CDB\0           CircularDB
 * >4      string          >\0             v%s:
 * >10     string          >\0             '%s',
 * >138    string          >\0             '%s',
 * >204    byte            0x02            gauge
 * >204    byte            0x04            counter
 *
 */

#define CDB_TOKEN   "CDB"
#define CDB_VERSION "1.1.1"

#define CDB_EXTENSION "cdb"
#define CDB_DEFAULT_DATA_UNIT "absolute"
#define CDB_DEFAULT_RECORDS 105120  // 1 year - 5 minute intervals
#define CDB_DEFAULT_INTERVAL 60 * 5 // 5 minutes

#define CDB_NAN (double)(0.0/0.0)

#define CDB_TYPE_GAUGE 2
#define CDB_TYPE_COUNTER 4

#define CDB_DEFAULT_DATA_TYPE CDB_TYPE_GAUGE

typedef struct cdb_header_s {
    char        token[4];           // CDB
    char        version[6];         //
    char        name[128];          // "short name" for this database
    char        desc[512];          // a longer description of this database.
    char        units[64];          // bytes, percent, seconds, etc
    int32_t     type;               // Defined above
    double      min_value;          // Values outside this range will be ignored/dropped
    double      max_value;          // Set both to 0 to disable.
    uint64_t    max_records;        // Maximum records this CDB can hold before cycling.
    int32_t     interval;           // Interval that we expect to see data at.
    uint64_t    start_record;       // Pointer to the logical start record
    uint64_t    num_records;
} cdb_header_t;

/* Use a 64bit value for the time, to be compatible across platforms and not
 * rely on the definition of time_t */
typedef int64_t cdb_time_t;

typedef struct cdb_record_s {
    cdb_time_t time;
    double value;
} cdb_record_t;

typedef struct cdb_s {
    int fd;
    int flags;
    int mode;
    bool synced;
    char *filename;
    cdb_header_t *header;
} cdb_t;

/* roll up all the previous positional arguments */
typedef struct cdb_request_s {
    cdb_time_t start;
    cdb_time_t end;
    int64_t count; /* number of records requested */
    bool cooked;   /* For counter types, do the math */
    uint32_t step;     /* Request averaged data */
} cdb_request_t;

/* Hold all the stats for a particular time range, so this computation can be
 * done only once. */
typedef struct cdb_range_s {
    cdb_time_t start_time;
    cdb_time_t end_time;
    uint64_t num_recs;
    double median, mean, sum, min, max, mad, stddev, absdev, variance, skew, kurtosis;
    double pct95th, pct75th, pct50th, pct25th;
} cdb_range_t;

typedef enum cdb_statistics_enum_s {
    CDB_MEDIAN,
    CDB_MEAN,
    CDB_SUM,
    CDB_MIN,
    CDB_MAX,
    CDB_MAD,
    CDB_STDDEV,
    CDB_ABSDEV,
    CDB_95TH,
    CDB_75TH,
    CDB_50TH,
    CDB_25TH,
} cdb_statistics_enum_t;

/* Error codes */
enum {
    CDB_SUCCESS  = 0,
    CDB_FAILURE  = -1,
    CDB_ETMRANGE = 1,   /* invalid time range */
    CDB_EFAULT   = 2,   /* invalid pointer */
    CDB_EINVAL   = 3,   /* invalid argument supplied by user */
    CDB_EFAILED  = 4,   /* generic failure */
    CDB_ESANITY  = 5,   /* sanity check failed - shouldn't happen */
    CDB_ENOMEM   = 6,   /* malloc failed */
    CDB_EINVMAX  = 7,   /* max_records is invalid */ 
    CDB_ERDONLY  = 8,   /* Trying to write to a read-only database */
    CDB_ENORECS  = 9,   /* No records were returned when they were expected */
    CDB_EINTERPD = 10,   /* Aggregate driver failure */ 
    CDB_EINTERPF = 11,  /* Aggregate follower failure */
    CDB_EBADTOK  = 12,  /* The CDB had an invalid header token */
    CDB_EBADVER  = 13,  /* The CDB had an incompatible version string */
};

#define RECORD_SIZE sizeof(cdb_record_t)
#define HEADER_SIZE sizeof(cdb_header_t)
#define RANGE_SIZE  sizeof(cdb_range_t)

/* Basic CDB handling functions */
cdb_t* cdb_new(void);
cdb_request_t cdb_new_request(void);

/* Return CDB_SUCCESS or errno */
int cdb_free(cdb_t *cdb);
int cdb_open(cdb_t *cdb);
int cdb_close(cdb_t *cdb);
int cdb_read_header(cdb_t *cdb);

/* Return CDB_SUCCESS, CDB_ERDONLY or errno */
int cdb_write_header(cdb_t *cdb);

void cdb_generate_header(cdb_t *cdb, char* name, char* desc, uint64_t max_records, int32_t type,
    char* units, uint64_t min_value, uint64_t max_value, int interval);

/* Return CDB_SUCCESS, CDB_ERDONLY, CDB_EINVMAX or errno */
int cdb_write_records(cdb_t *cdb, cdb_record_t *records, uint64_t len, uint64_t *num_recs);
bool cdb_write_record(cdb_t *cdb, cdb_time_t time, double value);

/* Update particular record(s) in the DB after they have already been written. */
/* Return CDB_SUCCESS, CDB_ERDONLY or errno */
int cdb_update_records(cdb_t *cdb, cdb_record_t *records, uint64_t len, uint64_t *num_recs);
bool cdb_update_record(cdb_t *cdb, cdb_time_t time, double value);

/* Return CDB_SUCCESS, CDB_ERDONLY or errno */
int cdb_discard_records_in_time_range(cdb_t *cdb, cdb_request_t *request, uint64_t *num_recs);

double cdb_get_statistic(cdb_range_t *range, cdb_statistics_enum_t type);

/* Return CDB_SUCCESS, CDB_ENOMEM, CDB_ETMRANGE, CDB_ENORECS or errno */
int cdb_read_records(cdb_t *cdb, cdb_request_t *request,
    uint64_t *num_recs, cdb_record_t **records, cdb_range_t *range);

void cdb_print_header(cdb_t * cdb);

void cdb_print_records(cdb_t *cdb, cdb_request_t *request, FILE *fh, const char *date_format);

void cdb_print(cdb_t *cdb);

/* Aggregation interface */
/* Return CDB_SUCCESS, CDB_ENOMEM, CDB_ETMRANGE, CDB_ENORECS, CDB_EINTERPD, CDB_EINTERPF or errno */
int cdb_read_aggregate_records(cdb_t **cdbs, int num_cdbs, cdb_request_t *request,
    uint64_t *num_recs, cdb_record_t **records, cdb_range_t *range);

void cdb_print_aggregate_records(cdb_t **cdbs, int num_cdbs, cdb_request_t *request, FILE *fh, const char *date_format);

#endif

#ifdef __cplusplus
}
#endif

#endif

/* -*- Mode: C; tab-width: 4 -*- */
/* vim: set tabstop=4 expandtab shiftwidth=4: */

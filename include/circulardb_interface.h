/*
 * $Id$
 *
 * CircularDB implementation for time series data.
 *
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
    0       string          CDB\0           CircularDB File
    >4      string          >\0             Version '%s'
    >10     string          >\0             Name '%s'
    >138    string          >\0             Units '%s'
    >202    byte            0               Type 'Gauge'
    >202    byte            1               Type 'Counter'
 */

#define CDB_TOKEN   "CDB"
#define CDB_VERSION "1.1.0"

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
    char        units[64];          // bytes, percent, seconds, etc
    int         type;               // Defined above
    double      min_value;          // Values outside this range will be ignored/dropped
    double      max_value;          // Set both to 0 to disable.
    uint64_t    max_records;        // Maximum records this CDB can hold before cycling. 
    int         interval;           // Interval that we expect to see data at.
    uint64_t    start_record;       // Pointer to the logical start record
    uint64_t    num_records;
} cdb_header_t;

typedef struct cdb_record_s {
    time_t time;
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
    time_t start;
    time_t end;
    int64_t count; /* number of records requested */
    bool cooked;   /* For counter types, do the math */
    long step;     /* Request averaged data */
} cdb_request_t;

/* Hold all the stats for a particular time range, so this computation can be
 * done only once. */
typedef struct cdb_range_s {
    time_t start_time;
    time_t end_time;
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

void cdb_generate_header(cdb_t *cdb, char* name, uint64_t max_records, int type, 
    char* units, uint64_t min_value, uint64_t max_value, int interval);

/* Return CDB_SUCCESS, CDB_ERDONLY, CDB_EINVMAX or errno */
int cdb_write_records(cdb_t *cdb, cdb_record_t *records, uint64_t len, uint64_t *num_recs);
bool cdb_write_record(cdb_t *cdb, time_t time, double value);

/* Update particular record(s) in the DB after they have already been written. */
/* Return CDB_SUCCESS, CDB_ERDONLY or errno */
int cdb_update_records(cdb_t *cdb, cdb_record_t *records, uint64_t len, uint64_t *num_recs);
bool cdb_update_record(cdb_t *cdb, time_t time, double value);

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

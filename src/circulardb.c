/*
 * CircularDB implementation for time series data.
 *
 * Copyright (c) 2007-2009 Powerset, Inc
 * Copyright (c) Dan Grillo, Manish Dubey, Dan Sully
 *
 * All rights reserved.
 */

#include "config.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* For the aggretgation interface */
#include <gsl/gsl_errno.h>
#include <gsl/gsl_interp.h>
#include <gsl/gsl_sort.h>
#include <gsl/gsl_statistics.h>

#include <circulardb_interface.h>

/* Future win32 support */
#ifndef O_BINARY
#define O_BINARY 0
#endif

/* DRK general: why not mmap the arena and treat it as an array to avoid all the "* RECORD_SIZE" */

/* Save away errno so it doesn't get overwritten */
int cdb_error(void) {
    int cdb_error = errno;
    return cdb_error;
}

static void _print_record(FILE *fh, cdb_time_t time, double value, const char *date_format) {

    if (date_format == NULL || strcmp(date_format, "") == 0) {

        fprintf(fh, "%d %.8g\n", (int)(time), value);

    } else {

        char formatted[256];
        time_t stime = (time_t)time;

        strftime(formatted, sizeof(formatted), date_format, localtime(&stime));

        fprintf(fh, "%d [%s] %.8g\n", (int)(time), formatted, value);
    }
}

static uint64_t _physical_record_for_logical_record(cdb_header_t *header, int64_t logical_record) {

    uint64_t physical_record = 0;

    /* -ve indicates nth record from the end
       +ve indicates nth record from the beginning (zero based counting for
          this case)

       -3 = seek to 3rd record from the end
        5 = seek to 6th record from the beginning
        0 = seek to first record (start) from the beginning

       change the request for -nth record (nth record from the end)
       to a request for mth record from the beginning, where:

       m = N + n (n is -ve)

       N : total num of records. */

    if (logical_record < 0) {
        int64_t rec_num_from_start = header->num_records + logical_record;

        /* if asking for records more than there are in the db, just give as many as we can */
        logical_record = rec_num_from_start >= 0 ? rec_num_from_start : 0;
    }

    if (logical_record >= header->num_records) {
#ifdef DEBUG
        printf("Can't seek to record [%"PRIu64"] with only [%"PRIu64"] in db\n", logical_record, header->num_records);
#endif
        return 0;
    }

    /* DRK unclear behavior here. Looks like if I specify -N where N >
       num_records, I get pointed to #0, but if I specify +N where N >
       num_records, it's an error */

    /* Nth logical record (from the beginning) maps to Mth physical record
       in the file, where:

       m = (n+s) % N

       s = physical record that is 0th logical record
       N = total number of records in the db.  */

    physical_record = logical_record + header->start_record;

    if (header->num_records > 0) {
        physical_record = (physical_record % header->num_records);
    }

    return physical_record;
}

static int64_t _seek_to_logical_record(cdb_t *cdb, int64_t logical_record) {

    uint64_t physical_record = _physical_record_for_logical_record(cdb->header, logical_record);
    uint64_t offset = HEADER_SIZE + (physical_record * RECORD_SIZE);

    if (lseek(cdb->fd, offset, SEEK_SET) != offset) {
        return -1;
    }

    return physical_record;
}

static cdb_time_t _time_for_logical_record(cdb_t *cdb, int64_t logical_record) {

    cdb_record_t record[RECORD_SIZE];
    cdb_time_t time = 0;

    /* skip over any record that has NULL time or bad time values
       Such datapoints in cdb indicate a corrupted cdb. */
    while (!time || time <= 0) {

        if (_seek_to_logical_record(cdb, logical_record) < 0) {
            time = 0;
            break;
        }

        logical_record += 1;

        if (read(cdb->fd, &record, RECORD_SIZE) < 0) {
            time = 0;
            break;
        }

        time = record->time;
    }

    return time;
}

/* note - if no exact match, will return a record with a time greater than the requested value */
static int64_t _logical_record_for_time(cdb_t *cdb, cdb_time_t req_time, int64_t start_logical_record, int64_t end_logical_record) {

    int first_time = 0;
    cdb_time_t start_time, next_time, center_time;
    int64_t delta, center_logical_record, next_logical_record;

    uint64_t num_recs = cdb->header->num_records;

    /* for the very first time find start and end */
    if (start_logical_record == 0 && end_logical_record == 0) {

        start_logical_record = 0;
        end_logical_record   = num_recs - 1;

        first_time = 1;
    }

    /* if no particular time was requested, just return the first one. */
    if (req_time == 0) {
        return start_logical_record;
    }

    /* if there are only 2 or 1 records in the search space, return the last record */
    if (end_logical_record - start_logical_record <= 1) {
        return end_logical_record;
    }

    start_time = _time_for_logical_record(cdb, start_logical_record);

    /* if requested time is less than start_time, start_time is the best we can do. */
    if (req_time <= start_time) {
        return start_logical_record;
    }

    /* don't go out of bounds */
    if (start_logical_record + 1 >= num_recs) {
        return start_logical_record;
    }

    next_logical_record = start_logical_record;
    next_time = start_time;

    /* if _seek_to_logical_record encounters bad data it fabricates and
       returns next good value. Try to get the *real* next rec. */
    while ((next_time - start_time) == 0) {

        next_logical_record += 1;

        next_time = _time_for_logical_record(cdb, next_logical_record);

        if (next_logical_record >= num_recs) {
            break;
        }
    }

    delta = next_time - start_time;

    /* delta = 0 means that _seek_to_logical_record fabricated data on the fly */
    if (delta == 0) {
        return start_logical_record;
    /* if we have wrapped over, or if the requested time is in the range of start and next, return start */
    } else if (delta < 0) {
        return start_logical_record;
    } else if (req_time <= next_time) {
        return next_logical_record;
    }

    /* for the very first binary search pivot point, take an aggressive guess to make search converge faster */
    if (first_time) {
        center_logical_record = (req_time - start_time) / (next_time - start_time) - 1;
    } else {
        center_logical_record = start_logical_record + (end_logical_record - start_logical_record) / 2;
    }

    if (num_recs > 0) {
        center_logical_record = (center_logical_record % num_recs);
    }

    center_time = _time_for_logical_record(cdb, center_logical_record);

    if (req_time >= center_time) {
        start_logical_record = center_logical_record;
    } else {
        end_logical_record = center_logical_record;
    }

    return _logical_record_for_time(cdb, req_time, start_logical_record, end_logical_record);
}

bool _cdb_is_writable(cdb_t *cdb) {

    /* We can't check for the O_RDONLY bit being because its defined value is zero.
     * i.e. there are no bits set to look for. We therefore assume
     * O_RDONLY if neither O_WRONLY nor O_RDWR are set. */
    if (cdb->flags & O_RDWR) {
        return true;
    }

    return false;
}

int cdb_read_header(cdb_t *cdb) {

    /* If the header has already been read from backing store do not read again */
    if (cdb->synced == false) {
        struct stat st;

        if (cdb_open(cdb) != 0) {
            return cdb_error();
        }

        if (pread(cdb->fd, cdb->header, HEADER_SIZE, 0) != HEADER_SIZE) {
            return cdb_error();
        }

        if (strncmp(cdb->header->token, CDB_TOKEN, sizeof(CDB_TOKEN)) != 0) {
            return CDB_EBADTOK;
        }

        if (strncmp(cdb->header->version, CDB_VERSION, sizeof(CDB_VERSION)) != 0) {
            return CDB_EBADVER;
        }

        cdb->synced = true;

        /* Calculate the number of records */
        if (fstat(cdb->fd, &st) == 0) {
            cdb->header->num_records = (st.st_size - HEADER_SIZE) / RECORD_SIZE;
        } else {
            cdb->header->num_records = 0;
        }
    }

    return CDB_SUCCESS;
}

int cdb_write_header(cdb_t *cdb) {

    if (cdb->synced) {
        return CDB_SUCCESS;
    }

    if (_cdb_is_writable(cdb) == false) {
        return CDB_ERDONLY;
    }

    cdb->synced = false;

    if (cdb_open(cdb) > 0) {
        return cdb_error();
    }

    if (pwrite(cdb->fd, cdb->header, HEADER_SIZE, 0) != HEADER_SIZE) {
        return cdb_error();
    }

    /* This really slows down moneypenny - we're ok with trusting the VFS
     * right now. */
    /* OS X doesn't have fdatasync() */
#if 0
#ifdef HAVE_FDATASYNC
    if (fdatasync(cdb->fd) != 0) return cdb_error();
#else
#ifdef HAVE_FSYNC
    if (fsync(cdb->fd) != 0) return cdb_error();
#endif
#endif
#endif

    cdb->synced = true;

    return CDB_SUCCESS;
}

void cdb_print_header(cdb_t * cdb) {

    printf("version: [%s]\n", cdb->header->version);
    printf("name: [%s]\n", cdb->header->name);
    printf("desc: [%s]\n", cdb->header->desc);
    printf("units: [%s]\n", cdb->header->units);

    if (cdb->header->type == CDB_TYPE_COUNTER) {
        printf("type: [COUNTER]\n");
    }

    if (cdb->header->type == CDB_TYPE_GAUGE) {
        printf("type: [GAUGE]\n");
    }

    printf("min_value: [%g]\n", cdb->header->min_value);
    printf("max_value: [%g]\n", cdb->header->max_value);
    printf("max_records: [%"PRIu64"]\n", cdb->header->max_records);
    printf("num_records: [%"PRIu64"]\n", cdb->header->num_records);
    printf("start_record: [%"PRIu64"]\n", cdb->header->start_record);
}

int cdb_write_records(cdb_t *cdb, cdb_record_t *records, uint64_t len, uint64_t *num_recs) {

    /* read old header if it exists.
       write a header out, since the db may not have existed.
    */
    uint64_t i   = 0;
    uint64_t j   = 0;
    *num_recs    = 0;
    off_t offset = 0;

    if (cdb_read_header(cdb) != CDB_SUCCESS) {
        return cdb_error();
    }

    if (_cdb_is_writable(cdb) == false) {
        return CDB_ERDONLY;
    }

    if (cdb->header->max_records <= 0) {
        return CDB_EINVMAX;
    }

    /* Logic for writes:
    cdb is 5 records.
        try to write 7 records
        len = 7
        len + 0 > 5
        j = (7 + 0) - 5
        j = 2
        i = 7 - 2
        i = 5

        need to write j records at offset: HEADER_SIZE + (cdb->header->start_record * RECORD_SIZE)
        index into records is i

        need to write i records at offset: HEADER_SIZE + (cdb->header->num_records * RECORD_SIZE)
        index into records is 0;
    */

    /* Calculate our indicies into the records array. */
    if (len + cdb->header->num_records >= cdb->header->max_records) {
        j = (len + cdb->header->num_records) - cdb->header->max_records;
        i = (len - j);
    } else {
        i = len;
    }

    /* If we need to wrap around */
    if (j > 0) {

        offset = HEADER_SIZE + (cdb->header->start_record * RECORD_SIZE);
        cdb->header->start_record += j;
        cdb->header->start_record %= cdb->header->max_records;

        if (pwrite(cdb->fd, &records[i], (RECORD_SIZE * j), offset) != (RECORD_SIZE * j)) {
            return cdb_error();
        }

        cdb->synced = false;

        /* start_record is no longer 0, so update the header */
        if (cdb_write_header(cdb) != CDB_SUCCESS) {
            return cdb_error();
        }
    }

    /* Normal case */
    offset = HEADER_SIZE + (cdb->header->num_records * RECORD_SIZE);
    cdb->header->num_records += i;

    if (pwrite(cdb->fd, &records[0], (RECORD_SIZE * i), offset) != (RECORD_SIZE * i)) {
        return cdb_error();
    }

    *num_recs += len;

#ifdef DEBUG
    printf("write_records: wrote [%"PRIu64"] records\n", *num_recs);
#endif

    return CDB_SUCCESS;
}

bool cdb_write_record(cdb_t *cdb, cdb_time_t time, double value) {

    cdb_record_t record[RECORD_SIZE];
    uint64_t num_recs = 0;

    record->time  = time;
    record->value = value;

    if (cdb_write_records(cdb, record, 1, &num_recs) != 0) {
        return false;
    }

    return true;
}

int cdb_update_records(cdb_t *cdb, cdb_record_t *records, uint64_t len, uint64_t *num_recs) {

    int ret    = CDB_SUCCESS;
    *num_recs  = 0;
    uint64_t i = 0;

    if (cdb_read_header(cdb) != CDB_SUCCESS) {
        return cdb_error();
    }

    if (_cdb_is_writable(cdb) == false) {
        return CDB_ERDONLY;
    }

#ifdef DEBUG
    printf("in update_records with [%"PRIu64"] num_recs\n", cdb->header->num_records);
#endif

    for (i = 0; i < len; i++) {

        cdb_time_t time = records[i].time;
        cdb_time_t rtime;
        uint64_t lrec;

        lrec = _logical_record_for_time(cdb, time, 0, 0);

        if (lrec >= 1) {
            lrec -= 1;
            /* DRK things like this are extremely confusing. some logical
               functions are zero based? some are one? or you're trying to back up? */
        }

        rtime = _time_for_logical_record(cdb, lrec);

        while (rtime < time && lrec < cdb->header->num_records - 1) {

            /* DRK is this true? or is the condition (lrec - start_lrec) <
               num_recs -- seems you can't wrap here (i.e. what if the initial
               lrec was num_recs - 1) */

            lrec += 1;
            rtime = _time_for_logical_record(cdb, lrec);
        }

#ifdef DEBUG
        printf("update_records: value: lrec [%"PRIu64"] time [%d] rtime [%d]\n", lrec, (int)time, (int)rtime);
#endif

        while (time == rtime && lrec < cdb->header->num_records - 1) {

            _seek_to_logical_record(cdb, lrec);

            if (write(cdb->fd, &records[0], RECORD_SIZE) != RECORD_SIZE) {
                ret = cdb_error();
                break;
            }

            lrec += 1;

            rtime = _time_for_logical_record(cdb, lrec);
        }
    }

    if (ret == CDB_SUCCESS) {

        if (i > 0) {
            cdb->synced = false;
            *num_recs = i;
        }

        if (cdb_write_header(cdb) != CDB_SUCCESS) {
            ret = cdb_error();
        }
    }

    return ret;
}

bool cdb_update_record(cdb_t *cdb, cdb_time_t time, double value) {

    cdb_record_t record[RECORD_SIZE];
    uint64_t num_recs = 0;

    record->time  = time;
    record->value = value;

    if (cdb_update_records(cdb, record, 1, &num_recs) != 0) {
        return false;
    }

    return true;
}

int cdb_discard_records_in_time_range(cdb_t *cdb, cdb_request_t *request, uint64_t *num_recs) {

    uint64_t i = 0;
    int64_t lrec;
    off_t offset = RECORD_SIZE;
    *num_recs = 0;

    if (cdb_read_header(cdb) != CDB_SUCCESS) {
        return cdb_error();
    }

    if (_cdb_is_writable(cdb) == false) {
        return CDB_ERDONLY;
    }

    lrec = _logical_record_for_time(cdb, request->start, 0, 0);

    if (lrec >= 1) {
        lrec -= 1;
    }

    for (i = lrec; i < cdb->header->num_records; i++) {

        cdb_time_t rtime = _time_for_logical_record(cdb, i);

        if (rtime >= request->start && rtime <= request->end) {

            cdb_record_t record[RECORD_SIZE];

            record->time  = rtime;
            record->value = CDB_NAN;

            if (pwrite(cdb->fd, &record, RECORD_SIZE, offset) != RECORD_SIZE) {
                return cdb_error();
            }

            *num_recs += 1;
        }
    }

    if (*num_recs > 0) {
        cdb->synced = false;
    }

    if (cdb_write_header(cdb) != CDB_SUCCESS) {
        return cdb_error();
    }

    return CDB_SUCCESS;
}

static int _compute_scale_factor_and_num_records(cdb_t *cdb, int64_t *num_records, long *factor) {

    if (cdb->header->type == CDB_TYPE_COUNTER) {
        if (*num_records != 0) {

            if (*num_records > 0) {
                *num_records += 1;
            } else {
                *num_records -= 1;
            }
        }
    }

    if (strlen(cdb->header->units) > 0) {

        int multiplier = 1;
        char *frequency;

        if ((frequency = calloc(strlen(cdb->header->units), sizeof(char))) == NULL) {
            return CDB_ENOMEM;
        }

        if ((sscanf(cdb->header->units, "per %d %s", &multiplier, frequency) == 2) ||
            (sscanf(cdb->header->units, "per %s", frequency) == 1) ||
            (sscanf(cdb->header->units, "%*s per %s", frequency) == 1)) {

            if (strcmp(frequency, "min") == 0) {
                *factor = 60;
            } else if (strcmp(frequency, "hour") == 0) {
                *factor = 60 * 60;
            } else if (strcmp(frequency, "sec") == 0 || strcmp(frequency, "second") == 0) {
                *factor = 1;
            } else if (strcmp(frequency, "day") == 0) {
                *factor = 60 * 60 * 24;
            } else if (strcmp(frequency, "week") == 0) {
                *factor = 60 * 60 * 24 * 7;
            } else if (strcmp(frequency, "month") == 0) {
                *factor = 60 * 60 * 24 * 30;
            } else if (strcmp(frequency, "quarter") == 0) {
                *factor = 60 * 60 * 24 * 90;
            } else if (strcmp(frequency, "year") == 0) {
                *factor = 60 * 60 * 24 * 365;
            }

            if (*factor != 0) {
                *factor *= multiplier;
            }
        }

        free(frequency);
    }

    return CDB_SUCCESS;
}

/* Statistics code
 * Make only one call to reading for a particular time range and compute all our stats
 */
void _compute_statistics(cdb_range_t *range, uint64_t *num_recs, cdb_record_t *records) {

    uint64_t i     = 0;
    uint64_t valid = 0;
    double sum     = 0.0;
    double *values = calloc(*num_recs, sizeof(double));

    for (i = 0; i < *num_recs; i++) {

        if (!isnan(records[i].value)) {

            sum += values[valid] = records[i].value;
            valid++;
        }
    }

    range->num_recs = valid;
    range->mean     = gsl_stats_mean(values, 1, valid);
    range->max      = gsl_stats_max(values, 1, valid);
    range->min      = gsl_stats_min(values, 1, valid);
    range->sum      = sum;
    range->stddev   = gsl_stats_sd(values, 1, valid);
    range->absdev   = gsl_stats_absdev(values, 1, valid);

    /* The rest need sorted data */
    gsl_sort(values, 1, valid);

    range->median   = gsl_stats_median_from_sorted_data(values, 1, valid);
    range->pct95th  = gsl_stats_quantile_from_sorted_data(values, 1, valid, 0.95);
    range->pct75th  = gsl_stats_quantile_from_sorted_data(values, 1, valid, 0.75);
    range->pct50th  = gsl_stats_quantile_from_sorted_data(values, 1, valid, 0.50);
    range->pct25th  = gsl_stats_quantile_from_sorted_data(values, 1, valid, 0.25);

    /* MAD must come last because it alters the values array
     * http://en.wikipedia.org/wiki/Median_absolute_deviation */
    for (i = 0; i < valid; i++) {
        values[i] = fabs(values[i] - range->median);

        if (values[i] < 0.0) {
            values[i] *= -1.0;
        }
    }

    /* Final sort is required MAD */
    gsl_sort(values, 1, valid);
    range->mad = gsl_stats_median_from_sorted_data(values, 1, valid);

    free(values);
}

double cdb_get_statistic(cdb_range_t *range, cdb_statistics_enum_t type) {

    switch (type) {
        case CDB_MEDIAN:
            return range->median;
        case CDB_MAD:
            return range->mad;
        case CDB_95TH:
            return range->pct95th;
        case CDB_75TH:
            return range->pct75th;
        case CDB_50TH:
            return range->pct50th;
        case CDB_25TH:
            return range->pct25th;
        case CDB_MEAN:
            return range->mean;
        case CDB_SUM:
            return range->sum;
        case CDB_MAX:
            return range->max;
        case CDB_MIN:
            return range->min;
        case CDB_STDDEV:
            return range->stddev;
        case CDB_ABSDEV:
            return range->absdev;
        default:
            fprintf(stderr, "aggregate_using_function_for_records() function: [%d] not supported\n", type);
            return CDB_FAILURE;
    }
}

static int _cdb_read_records(cdb_t *cdb, cdb_request_t *request, uint64_t *num_recs, cdb_record_t **records) {

    int64_t first_requested_logical_record;
    int64_t last_requested_logical_record;
    uint64_t last_requested_physical_record;
    int64_t seek_logical_record;
    int64_t seek_physical_record;

    cdb_record_t *buffer = NULL;

    if (cdb_read_header(cdb) != CDB_SUCCESS) {
        return cdb_error();
    }

    if (request->start != 0 && request->end != 0 && request->end < request->start) {
        return CDB_ETMRANGE;
    }

    if (cdb->header == NULL || cdb->synced == false) {
        return CDB_ESANITY;
    }

    /* bail out if there are no records */
    if (cdb->header->num_records <= 0) {
        return CDB_ENORECS;
    }

    /*
      get the number of requested records:

      -ve indicates n records from the beginning
      +ve indicates n records off of the end.
      0 or undef means the whole thing.

      switch the meaning of -ve/+ve to be more array like
    */
    if (request->count != 0) {
        request->count = -request->count;
    }

#ifdef DEBUG
    printf("read_records start: [%ld]\n", request->start);
    printf("read_records end: [%ld]\n", request->end);
    printf("read_records num_requested: [%"PRIu64"]\n", request->count);
#endif

    if (request->count != 0 && request->count < 0 && request->start == 0) {
        /* if reading only few records from the end, just set -ve offset to seek to */
        first_requested_logical_record = request->count;

    } else {
        /* compute which record to start reading from the beginning, based on start time specified. */
        first_requested_logical_record = _logical_record_for_time(cdb, request->start, 0, 0);
    }

    /* if end is not defined, read all the records or only read uptill the specified record. */
    if (request->end == 0) {

        last_requested_logical_record = cdb->header->num_records - 1;

    } else {

        last_requested_logical_record = _logical_record_for_time(cdb, request->end, 0, 0);

        /* this can return something > end, check for that */
        if (_time_for_logical_record(cdb, last_requested_logical_record) > request->end) {
            last_requested_logical_record -= 1;
        }
    }

    last_requested_physical_record = (last_requested_logical_record + cdb->header->start_record) % cdb->header->num_records;

    seek_logical_record  = first_requested_logical_record;
    seek_physical_record = _seek_to_logical_record(cdb, seek_logical_record);

    if (last_requested_physical_record >= seek_physical_record) {

        uint64_t nrec = (last_requested_physical_record - seek_physical_record + 1);
        uint64_t rlen = RECORD_SIZE * nrec;

        if ((buffer = calloc(1, rlen)) == NULL) {
            free(buffer);
            return CDB_ENOMEM;
        }

        /* one slurp - XXX TODO - mmap */
        if (read(cdb->fd, buffer, rlen) != rlen) {
            free(buffer);
            return cdb_error();
        }

        *num_recs = nrec;

    } else {

        /* We've wrapped around the end of the file */
        uint64_t nrec1 = (cdb->header->num_records - seek_physical_record);
        uint64_t nrec2 = (last_requested_physical_record + 1);

        uint64_t rlen1 = RECORD_SIZE * nrec1;
        uint64_t rlen2 = RECORD_SIZE * nrec2;

        if ((buffer = calloc(1, rlen1 + rlen2)) == NULL) {
            free(buffer);
            return CDB_ENOMEM;
        }

        if (read(cdb->fd, buffer, rlen1) != rlen1) {
            free(buffer);
            return cdb_error();
        }

        if (pread(cdb->fd, &buffer[nrec1], rlen2, HEADER_SIZE) != rlen2) {
            free(buffer);
            return cdb_error();
        }

        *num_recs = nrec1 + nrec2;
    }

    /* Deal with cooking the output */
    if (request->cooked) {

        bool check_min_max = true;
        long factor = 0;
        uint64_t i = 0;
        uint64_t cooked_recs = 0;
        double prev_value = 0.0;
        cdb_time_t prev_date = 0;
        cdb_record_t *crecords;

        if ((crecords = calloc(*num_recs, RECORD_SIZE)) == NULL) {
            free(crecords);
            free(buffer);
            return CDB_ENOMEM;
        }

        if (_compute_scale_factor_and_num_records(cdb, &request->count, &factor)) {
            free(crecords);
            free(buffer);
            return cdb_error();
        }

        if (cdb->header->min_value == 0 && cdb->header->max_value == 0) {
            check_min_max = false;
        }

        for (i = 0; i < *num_recs; i++) {

            cdb_time_t date = buffer[i].time;
            double value    = buffer[i].value;

            if (cdb->header->type == CDB_TYPE_COUNTER) {
                double new_value = value;
                value = CDB_NAN;

                if (!isnan(prev_value) && !isnan(new_value)) {

                    double val_delta = new_value - prev_value;

                    if (val_delta >= 0) {
                        value = val_delta;
                    }
                }

                prev_value = new_value;
            }

            if (factor != 0 && cdb->header->type == CDB_TYPE_COUNTER) {

                /* Skip the first entry, since it's absolute and is needed
                 * to calculate the second */
                if (prev_date == 0) {
                    prev_date = date;
                    continue;
                }

                cdb_time_t time_delta = date - prev_date;

                if (time_delta > 0 && !isnan(value)) {
                    value = factor * (value / time_delta);
                }

                prev_date = date;
            }

            /* Check for min/max boundaries */
            /* Should this be done on write instead of read? */
            if (check_min_max && !isnan(value)) {
                if (value > cdb->header->max_value || value < cdb->header->min_value) {
                    value = CDB_NAN;
                }
            }

            /* Copy the munged data to our new array, since we might skip
             * elements. Also keep in mind mmap for the future */
            crecords[cooked_recs].time  = date;
            crecords[cooked_recs].value = value;
            cooked_recs += 1;
        }

        /* Now swap our cooked records for the buffer, so we can slice it as needed. */
        free(buffer);
        buffer = crecords;
        *num_recs = cooked_recs;
    }

    /* If we've been requested to average the records & timestamps */
    if (request->step > 1) {

        cdb_record_t *arecords;
        long     step      = request->step;
        uint64_t step_recs = 0;
        uint64_t leftover  = (*num_recs % step);
        uint64_t walkend   = (*num_recs - leftover);
        uint64_t i = 0;

        if ((arecords = calloc((int)((*num_recs / step) + leftover), RECORD_SIZE)) == NULL) {
            free(arecords);
            free(buffer);
            return CDB_ENOMEM;
        }

        /* Walk our list of cooked records, jumping ahead by the given step.
           For each set of records within that step, we want to get the average
           for those records and place them into a new array.
         */
        for (i = 0; i < walkend; i += step) {

            int j = 0;
            double xi[step];
            double yi[step];

            for (j = 0; j < step; j++) {

                /* No NaNs on average - they make bogus graphs. Is there a
                 * better value than 0 to use here? */
                if (isnan(buffer[i+j].value)) {
                    buffer[i+j].value = 0;
                }

                xi[j] = (double)buffer[i+j].time;
                yi[j] = buffer[i+j].value;
            }

            arecords[step_recs].time  = (cdb_time_t)gsl_stats_mean(xi, 1, step);
            arecords[step_recs].value = gsl_stats_mean(yi, 1, step);
            step_recs += 1;
        }

        /* Now collect from the last step point to the end and average. */
        if (leftover > 0) {
            uint64_t leftover_start = *num_recs - leftover;

            int j = 0;
            double xi[leftover];
            double yi[leftover];

            for (i = leftover_start; i < *num_recs; i++) {

                /* No NaNs on average - they make bogus graphs. Is there a
                 * better value than 0 to use here? */
                if (isnan(buffer[i].value)) {
                    buffer[i].value = 0;
                }

                xi[j] = (double)buffer[i].time;
                yi[j] = buffer[i].value;
                j++;
            }

            arecords[step_recs].time  = (cdb_time_t)gsl_stats_mean(xi, 1, j);
            arecords[step_recs].value = gsl_stats_mean(yi, 1, j);
            step_recs += 1;
        }

        free(buffer);
        buffer = arecords;
        *num_recs = step_recs;
    }

    /* now pull out the number of requested records if asked */
    if (request->count != 0 && *num_recs >= abs(request->count)) {

        uint64_t start_index = 0;

        if (request->count <= 0) {

            start_index = *num_recs - abs(request->count);
        }

        *num_recs = abs(request->count);

        if ((*records  = calloc(*num_recs, RECORD_SIZE)) == NULL) {
            free(buffer);
            return CDB_ENOMEM;
        }

        memcpy(*records, &buffer[start_index], RECORD_SIZE * *num_recs);

        free(buffer);

    } else {

        *records = buffer;
    }

    return CDB_SUCCESS;
}

int cdb_read_records(cdb_t *cdb, cdb_request_t *request,
    uint64_t *num_recs, cdb_record_t **records, cdb_range_t *range) {

    int ret   = CDB_SUCCESS;

    ret = _cdb_read_records(cdb, request, num_recs, records);

    if (ret == CDB_SUCCESS) {

        if (*num_recs > 0) {
            range->start_time = request->start;
            range->end_time   = request->end;

            _compute_statistics(range, num_recs, *records);
        }
    }

    return ret;
}

void cdb_print_records(cdb_t *cdb, cdb_request_t *request, FILE *fh, const char *date_format) {

    uint64_t i = 0;
    uint64_t num_recs = 0;

    cdb_record_t *records = NULL;

    if (_cdb_read_records(cdb, request, &num_recs, &records) == CDB_SUCCESS) {

        for (i = 0; i < num_recs; i++) {

            _print_record(fh, records[i].time, records[i].value, date_format);
        }
    }

    free(records);
}

void cdb_print(cdb_t *cdb) {

    const char *date_format = "%Y-%m-%d %H:%M:%S";
    cdb_request_t request;

    request.start  = 0;
    request.end    = 0;
    request.count  = 0;
    request.step   = 0;
    request.cooked = false;

    printf("============== Header ================\n");

    if (cdb_read_header(cdb) == CDB_SUCCESS) {
        cdb_print_header(cdb);
    }

    if (cdb->header->type == CDB_TYPE_COUNTER) {

        printf("============= Raw Counter Records =============\n");
        cdb_print_records(cdb, &request, stdout, date_format);
        printf("============== End Raw Counter Records ==============\n");

        printf("============== Cooked Records ================\n");

    } else {
        printf("============== Records ================\n");
    }

    request.cooked = true;
    cdb_print_records(cdb, &request, stdout, date_format);

    printf("============== End ================\n");
}

/* Take in an array of cdbs */
int cdb_read_aggregate_records(cdb_t **cdbs, int num_cdbs, cdb_request_t *request,
    uint64_t *driver_num_recs, cdb_record_t **records, cdb_range_t *range) {

    uint64_t i = 0;
    int ret    = CDB_SUCCESS;
    cdb_record_t *driver_records = NULL;
    *driver_num_recs = 0;

    if (cdbs[0] == NULL) {
        return CDB_ESANITY;
    }

    /* The first cdb is the driver */
    ret = _cdb_read_records(cdbs[0], request, driver_num_recs, &driver_records);

    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Bailed on: %s\n", cdbs[0]->filename);
        free(driver_records);
        return ret;
    }

    if ((*records = calloc(*driver_num_recs, RECORD_SIZE)) == NULL) {
        free(driver_records);
        return CDB_ENOMEM;
    }

    if (*driver_num_recs <= 1) {
        free(driver_records);
        return CDB_EINTERPD;
    }

    double *driver_x_values   = calloc(*driver_num_recs, sizeof(double));
    double *driver_y_values   = calloc(*driver_num_recs, sizeof(double));
    double *follower_x_values = calloc(*driver_num_recs, sizeof(double));
    double *follower_y_values = calloc(*driver_num_recs, sizeof(double));

    for (i = 0; i < *driver_num_recs; i++) {
        (*records)[i].time  = driver_x_values[i] = driver_records[i].time;
        (*records)[i].value = driver_y_values[i] = driver_records[i].value;
    }

    /* initialize and allocate the gsl objects  */
    gsl_interp_accel *accel = gsl_interp_accel_alloc();
    gsl_interp *interp = gsl_interp_alloc(gsl_interp_linear, *driver_num_recs);

    /* Allows 0.0 to be returned as a valid yi */
    gsl_set_error_handler_off();

    gsl_interp_init(interp, driver_x_values, driver_y_values, *driver_num_recs);

    for (i = 1; i < num_cdbs; i++) {

        uint64_t j = 0;
        uint64_t follower_num_recs = 0;

        cdb_record_t *follower_records = NULL;

        ret = _cdb_read_records(cdbs[i], request, &follower_num_recs, &follower_records);

        /* Just bail, free all allocations below and let the error bubble up */
        if (follower_num_recs != 0) {

            for (j = 0; j < *driver_num_recs; j++) {

                /* Check for out of bounds */
                if (j >= follower_num_recs) {
                    break;
                }

                follower_x_values[j] = follower_records[j].time;
                follower_y_values[j] = follower_records[j].value;
            }

            for (j = 0; j < *driver_num_recs; j++) {

                double yi = gsl_interp_eval(interp, follower_x_values, follower_y_values, driver_x_values[j], accel);

                if (isnormal(yi)) {
                    (*records)[j].value += yi;
                }
            }
        }

        ret = CDB_SUCCESS;

        free(follower_records);
    }

    if (ret == CDB_SUCCESS && *driver_num_recs > 0) {
        /* Compute all the statistics for this range */
        range->start_time = request->start;
        range->end_time   = request->end;

        _compute_statistics(range, driver_num_recs, *records);
    }

    free(driver_x_values);
    free(driver_y_values);
    free(follower_x_values);
    free(follower_y_values);

    gsl_interp_free(interp);
    gsl_interp_accel_free(accel);
    free(driver_records);

    return ret;
}

void cdb_print_aggregate_records(cdb_t **cdbs, int num_cdbs, cdb_request_t *request, FILE *fh, const char *date_format) {

    uint64_t i = 0;
    uint64_t num_recs = 0;

    cdb_record_t *records = NULL;
    cdb_range_t *range = calloc(1, sizeof(cdb_range_t));

    cdb_read_aggregate_records(cdbs, num_cdbs, request, &num_recs, &records, range);

    for (i = 0; i < num_recs; i++) {

        _print_record(fh, records[i].time, records[i].value, date_format);
    }

    free(range);
    free(records);
}

void cdb_generate_header(cdb_t *cdb, char* name, char* desc, uint64_t max_records, int type,
    char* units, uint64_t min_value, uint64_t max_value, int interval) {

    if (max_records == 0) {
        max_records = CDB_DEFAULT_RECORDS;
    }

    if (interval == 0) {
        interval = CDB_DEFAULT_INTERVAL;
    }

    if (type == 0) {
        cdb->header->type = CDB_DEFAULT_DATA_TYPE;
    }

    if (units == NULL || (strcmp(units, "") == 0)) {
        units = (char*)CDB_DEFAULT_DATA_UNIT;
    }

    if (desc == NULL) {
        desc = (char*)"";
    }

    memset(cdb->header->name, 0, sizeof(cdb->header->name));
    memset(cdb->header->desc, 0, sizeof(cdb->header->desc));
    memset(cdb->header->units, 0, sizeof(cdb->header->units));
    memset(cdb->header->version, 0, sizeof(cdb->header->version));
    memset(cdb->header->token, 0, sizeof(cdb->header->token));

    strncpy(cdb->header->name, name, sizeof(cdb->header->name));
    strncpy(cdb->header->desc, desc, sizeof(cdb->header->desc));
    strncpy(cdb->header->units, units, sizeof(cdb->header->units));
    strncpy(cdb->header->version, CDB_VERSION, sizeof(cdb->header->version));
    strncpy(cdb->header->token, CDB_TOKEN, sizeof(cdb->header->token));

    cdb->header->type         = type;
    cdb->header->interval     = interval;
    cdb->header->max_records  = max_records;
    cdb->header->min_value    = min_value;
    cdb->header->max_value    = max_value;
    cdb->header->num_records  = 0;
    cdb->header->start_record = 0;
}

cdb_t* cdb_new(void) {

    cdb_t *cdb  = calloc(1, sizeof(cdb_t));
    cdb->header = calloc(1, HEADER_SIZE);

    cdb->fd = -1;
    cdb->synced = false;
    cdb->flags = -1;
    cdb->mode = -1;

    return cdb;
}

cdb_request_t cdb_new_request(void) {
    cdb_request_t request;
    request.start  = 0;
    request.end    = 0;
    request.count  = 0;
    request.cooked = true;
    request.step   = 0;
    return request;
}

int cdb_open(cdb_t *cdb) {

    if (cdb->fd < 0) {

        /* Default flags if none were set */
        if (cdb->flags == -1) {
            cdb->flags = O_RDONLY|O_BINARY;
        }

        /* A cdb can't be write only - we need to read the header */
        if (cdb->flags & O_WRONLY) {
            cdb->flags = O_RDWR;
        }

        if (cdb->mode == -1) {
            cdb->mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        }

        cdb->fd = open(cdb->filename, cdb->flags, cdb->mode);

        if (cdb->fd < 0) {
            return cdb_error();
        }
    }

    return CDB_SUCCESS;
}

int cdb_close(cdb_t *cdb) {

    if (cdb != NULL) {

        if (cdb->fd > 0) {
            if (close(cdb->fd) != 0) {
                return cdb_error();
            }
            cdb->fd = -1;
        }
    }

    return CDB_SUCCESS;
}

int cdb_free(cdb_t *cdb) {
    int ret = CDB_SUCCESS;

    if (cdb != NULL) {

        if (cdb->header != NULL) {
            ret = cdb_close(cdb);
            free(cdb->header);
            cdb->header = NULL;
        }

        free(cdb);
        cdb = NULL;
    }

    return ret;
}

/* -*- Mode: C; tab-width: 4 -*- */
/* vim: set tabstop=4 expandtab shiftwidth=4: */

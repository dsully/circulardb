/*
 * CircularDB implementation for time series data.
 *
 */

#ifndef LINT
static const char svnid[] __attribute__ ((unused)) = "$Id$";
#endif

#include "config.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <inttypes.h>
#include <regex.h>
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
#include <math.h>  
#include <gsl/gsl_errno.h>
#include <gsl/gsl_interp.h>

#include <circulardb_interface.h>

/* DRK general: why not mmap the arena and treat it as an array to avoid all the "* RECORD_SIZE" */

static void _print_record(FILE *fh, time_t time, double value, const char *date_format) {

    if (date_format == NULL || strcmp(date_format, "") == 0) {

        fprintf(fh, "%d %.8g\n", (int)(time), value);

    } else {

        char formatted[256];
        
        strftime(formatted, sizeof(formatted), date_format, localtime(&time));

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
        /* printf("Can't seek to record [%ld] with only [%"PRIu64"] in db\n", logical_record, header->num_records); */
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

static uint64_t _seek_to_logical_record(cdb_t *cdb, int64_t logical_record) {

    uint64_t physical_record = _physical_record_for_logical_record(cdb->header, logical_record);
    uint64_t offset = HEADER_SIZE + (physical_record * RECORD_SIZE);

    if (lseek(cdb->fd, offset, SEEK_SET) != offset) {
        return 0;
    }

    return physical_record;
}

static time_t _time_for_logical_record(cdb_t *cdb, int64_t logical_record) {

    cdb_record_t record[RECORD_SIZE];
    time_t time = 0;

    /* skip over any record that has NULL time or bad time values
       Such datapoints in cdb indicate a corrupted cdb. */
    while (!time || time <= 0) {

        if (_seek_to_logical_record(cdb, logical_record) == 0) {
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
static int64_t _logical_record_for_time(cdb_t *cdb, time_t req_time, int64_t start_logical_record, int64_t end_logical_record) {

    int first_time = 0;
    time_t start_time, next_time, center_time;
    int64_t delta, center_logical_record, next_logical_record;

    uint64_t num_recs = cdb->header->num_records;

    /* for the very first time find start and end */
    if (start_logical_record == 0 && end_logical_record == 0) {
        start_logical_record = 0;
        end_logical_record = num_recs - 1;

        first_time = 1;
    } 

    /* if no particular time was requested, just return the first one. */
    if (req_time == 0) {
        return start_logical_record;
    }

    /* if there is 2 or 1 record in the search space, return the last */
    if (end_logical_record - start_logical_record <= 1) {
        return end_logical_record;
    }

    start_time = _time_for_logical_record(cdb, start_logical_record);

    /* if requested time is less than start_time, start_time is the best we can do. */
    if (req_time <= start_time) {
        return start_logical_record;
    }

    if (start_logical_record + 1 >= num_recs) {
        return start_logical_record;
    }

    next_logical_record = start_logical_record;
    next_time = start_time;

    /* if readLogicalRecord encounters bad data it fabricates and
       returns next good value. Try to get the *real* next rec. */

    while ((next_time - start_time) == 0) {

        next_logical_record += 1;

        next_time = _time_for_logical_record(cdb, next_logical_record);

        if (next_logical_record >= num_recs) {
            break;
        }
    }

    delta = next_time - start_time;

    /* delta = 0 means that readLogicalRecord fabricated data on the fly */
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

int cdb_read_header(cdb_t *cdb) {

    /* if the header has already been read from backing store do not read again */
    if (cdb->header != NULL && cdb->synced == 1) {
        return 0;
    }

    if (cdb_open(cdb) > 0) {
        return errno;
    }

    if (pread(cdb->fd, cdb->header, HEADER_SIZE, 0) != HEADER_SIZE) {
        return errno;
    }

    cdb->synced = 1;

    return 0;
}

int cdb_write_header(cdb_t *cdb) {

    time_t now;

    if (cdb->header != NULL && cdb->synced == 1) {
        return 0;
    }

    time(&now);

    cdb->header->last_updated = now;
    cdb->synced = 0;

    if (cdb_open(cdb) > 0) {
        return errno;
    }

    if (pwrite(cdb->fd, cdb->header, HEADER_SIZE, 0) != HEADER_SIZE) {
        return errno;
    }

    if (fdatasync(cdb->fd) != 0) {
        return errno;
    }

    cdb->synced = 1;

    return 0;
}

void cdb_print_header(cdb_t * cdb) {

    printf("name: [%s]\n", cdb->header->name);
    printf("desc: [%s]\n", cdb->header->description);
    printf("unit: [%s]\n", cdb->header->units);
    printf("type: [%s]\n", cdb->header->type);
    printf("version: [%s]\n", cdb->header->version);
    printf("max_records: [%"PRIu64"]\n", cdb->header->max_records);
    printf("num_records: [%"PRIu64"]\n", cdb->header->num_records);
    printf("start_record: [%"PRIu64"]\n", cdb->header->start_record);
    printf("last_updated: [%d]\n", (int)cdb->header->last_updated);
}

uint64_t cdb_write_records(cdb_t *cdb, cdb_record_t *records, uint64_t len) {

    /* read old header if it exists.
       write a header out, since the db may not have existed.
    */
    uint64_t num_recs = 0;
    uint64_t i = 0;

    /* XXX - Kinda hacky.. since no exceptions. Below with write_header as well */
    if (cdb_read_header(cdb) > 0) {
        return 0;
    }

    for (i = 0; i < len; i++) {

        /* printf("writing record: [%d] [%f]\n", (int)records[i].time, records[i].value); */

        off_t offset;

        if (records[i].time == 0) {
            /* printf("write_records: time == 0; skipping!\n"); */
            continue;
        }

        assert(cdb->header->max_records);

        /* Do the circular wrap-around. start_record needs to start
         * incrementing with num_records */
        if (cdb->header->num_records >= cdb->header->max_records) {

            offset = HEADER_SIZE + (cdb->header->start_record * RECORD_SIZE);
            cdb->header->start_record += 1;
            cdb->header->start_record %= cdb->header->max_records;

        } else {

            offset = HEADER_SIZE + (cdb->header->num_records * RECORD_SIZE);
            cdb->header->num_records += 1;
        }

        if (pwrite(cdb->fd, &records[i], RECORD_SIZE, offset) != RECORD_SIZE) {
            printf("Couldn't write record [%d] [%s] for: [%s]\n", errno, strerror(errno), cdb->filename);
            break;
        }

        num_recs += 1;
        /* DRK might want to consider periodic syncs? */
    }

    if (num_recs > 0) {
        cdb->synced = 0;
    }

    if (cdb_write_header(cdb) > 0) {
        return 0;
    }

    return num_recs;
}

bool cdb_write_record(cdb_t *cdb, time_t time, double value) {

    cdb_record_t record[RECORD_SIZE];

    record->time  = time;
    record->value = value;

    return (bool)cdb_write_records(cdb, record, 1);
}

uint64_t cdb_update_records(cdb_t *cdb, cdb_record_t *records, uint64_t len) {

    uint64_t num_recs = 0;
    uint64_t i = 0;

    if (cdb_read_header(cdb) > 0) {
        return 0;
    }

    num_recs = cdb->header->num_records;

    /* printf("in update_records with [%ld] num_recs\n", num_recs); */

    for (i = 0; i < len; i++) {

        time_t time  = records[i].time;
        double value = records[i].value;

        time_t rtime;
        uint64_t lrec;

        /* printf("updating record: [%d] [%.8g]\n", (int)time, value); */

        lrec = _logical_record_for_time(cdb, time, 0, 0);

        if (lrec >= 1) {
            lrec -= 1;
            /* DRK things like this are extremely confusing. some logical
               functions are zero based? some are one? or you're trying to back up? */
        }

        rtime = _time_for_logical_record(cdb, lrec);

        while (rtime < time && lrec < num_recs - 1) {

            /* DRK is this true? or is the condition (lrec - start_lrec) <
               num_recs -- seems you can't wrap here (i.e. what if the initial
               lrec was num_recs - 1) */

            lrec += 1;
            rtime = _time_for_logical_record(cdb, lrec);
        }

        /* printf("update_records: lrec [%llu] time [%d] rtime [%d]\n", lrec, time, rtime); */

        while (time == rtime && lrec < num_recs - 1) {

            /* DBL_MIN is used as the invalid value */
            if (!value) {
                value = DBL_MIN;
            }

            _seek_to_logical_record(cdb, lrec);

            if (write(cdb->fd, &records[0], RECORD_SIZE) != RECORD_SIZE) {
                /* printf("update_records: Couldn't write record [%s]\n", strerror(errno)); */
                break;
            }

            lrec += 1;

            rtime = _time_for_logical_record(cdb, lrec);
        }
    }

    return i;
}

bool cdb_update_record(cdb_t *cdb, time_t time, double value) {

    cdb_record_t record[RECORD_SIZE];

    record->time  = time;
    record->value = value;

    return (bool)cdb_update_records(cdb, record, 1);
}

uint64_t cdb_discard_records_in_time_range(cdb_t *cdb, time_t start, time_t end) {

    uint64_t i, num_updated = 0;
    int64_t lrec;
    off_t offset = RECORD_SIZE;

    if (cdb_read_header(cdb) > 0) {
        return 0;
    }

    lrec = _logical_record_for_time(cdb, start, 0, 0);

    if (lrec >= 1) {
        lrec -= 1;
    }

    for (i = lrec; i < cdb->header->num_records; i++) {

        time_t rtime = _time_for_logical_record(cdb, i);

        if (rtime >= start && rtime <= end) {

            cdb_record_t record[RECORD_SIZE];

            record->time  = rtime;
            record->value = DBL_MIN;

            if (pwrite(cdb->fd, &record, RECORD_SIZE, offset) != RECORD_SIZE) {
                break;
            }

            num_updated += 1;
        }
    }

    return num_updated;
}

static long _compute_scale_factor_and_num_records(cdb_t *cdb, int64_t *num_records) {

    long factor = 0;
    int  multiplier = 1;

    if (cdb_read_header(cdb) > 0) {
        return 0;
    }

    if (strcmp(cdb->header->type, "counter") == 0) {

        if (*num_records != 0) {

            if (*num_records > 0) {
                *num_records += 1;
            } else {
                *num_records -= 1;
            }
        }
    }

    if (cdb->header->units) {

        char *frequency;

        /* %as is a GNU extension */
        if ((sscanf(cdb->header->units, "per %d %as", &multiplier, &frequency) == 2) ||
            (sscanf(cdb->header->units, "per %as", &frequency)) == 1) {

            if (strcmp(frequency, "min") == 0) {
                factor = 60;
            } else if (strcmp(frequency, "hour") == 0) {
                factor = 60 * 60;
            } else if (strcmp(frequency, "sec") == 0) {
                factor = 1;
            } else if (strcmp(frequency, "day") == 0) {
                factor = 60 * 60 * 24;
            } else if (strcmp(frequency, "week") == 0) {
                factor = 60 * 60 * 24 * 7;
            } else if (strcmp(frequency, "month") == 0) {
                factor = 60 * 60 * 24 * 30;
            } else if (strcmp(frequency, "quarter") == 0) {
                factor = 60 * 60 * 24 * 90;
            } else if (strcmp(frequency, "year") == 0) {
                factor = 60 * 60 * 24 * 365;
            } 

            if (factor != 0) {
                factor *= multiplier;
            }

            free(frequency);
        }
    }

    return factor;
}

double cdb_aggregate_using_function_for_records(cdb_t *cdb, char *function, time_t start, time_t end, int64_t num_requested) {

    int factor, time_delta, is_counter, cooked = 0;
    uint64_t i, num_recs, seen = 0;

    double new_value = 0.0;
    double prev_value = 0.0;
    double val_delta = 0.0;
    double max = 0.0;
    double min = 0.0;
    double sum = 0.0;
    double ret = 0.0;
    double values;
    time_t total_time = 0;
    time_t prev_date = 0;
    time_t first_time, last_time;

    cdb_record_t *records = NULL;

    if (strcmp(cdb->header->type, "counter") == 0) {
        is_counter = 1;
    }

    factor = _compute_scale_factor_and_num_records(cdb, &num_requested);

    num_recs = cdb_read_records(cdb, start, end, num_requested, cooked, &first_time, &last_time, &records);

    /* DRK would be so nice if you could get an iterator into the database
       instead of having to copy all the values out */
    for (i = 0; i < num_recs; i++) {

        time_t date  = records[i].time;
        double value = records[i].value;

        if (!date || date < 0) {
            continue;
        }

        if (value == DBL_MAX) {
            value = DBL_MIN;
        }

        if (is_counter) {

            new_value = value;
            value = DBL_MAX;

            if (prev_value != DBL_MAX && new_value != DBL_MAX) {

                val_delta = new_value - prev_value;

                if (val_delta >= 0) {
                    value = val_delta;
                }
            }

            if (value != DBL_MAX) {
                sum += value;
            }

            prev_value = new_value;
        }

        if (factor) {

            if (!prev_date) {
                prev_date = date;
                continue;
            }

            time_delta = date - prev_date;

            if (time_delta > 0 && value != DBL_MAX) {

                value = factor * (value / time_delta);

                total_time += time_delta;
            }

            prev_date = date;
        }

        if (value == DBL_MAX) {
            continue;
        }

        values += value;

        seen += 1;

        if (!max) max = value; 
        if (!min) min = value; 

        if (!is_counter) {
            sum += value;
        }

        max = value > max ? value : max;
        min = value < min ? value : min;
    }

    free(records);

    if (strcmp(function, "median") == 0) {

        /* TODO - need to find a better way to get median. Not currently used, so no big deal. */
        /* ret = (values[(seen - 1) / 2] + values[seen / 2]) / 2;  */
        ret = 0;

    } else if (strcmp(function, "average") == 0) {

        if (is_counter) {

            if (total_time) {
                ret = sum / total_time;
            }

        } else {

            if (seen) {
                ret = sum / seen;
            }
        }

    } else if (strcmp(function, "sum") == 0) {
        ret = sum;

    } else if (strcmp(function, "max") == 0) {
        ret = max;

    } else if (strcmp(function, "min") == 0) {
        ret = min;

    } else {

        fprintf(stderr, "aggregate_using_function_for_records() function: [%s] not supported\n", function);
        return -1;
    }

    return ret;
}

uint64_t cdb_read_records(cdb_t *cdb, time_t start, time_t end, int64_t num_requested, 
    int cooked, time_t *first_time, time_t *last_time, cdb_record_t **records) {

    int64_t first_requested_logical_record;
    int64_t last_requested_logical_record;
    uint64_t last_requested_physical_record;
    int64_t seek_logical_record;
    uint64_t seek_physical_record;
    uint64_t nrecs = 0;

    cdb_record_t *buffer = NULL;

    if (cdb_read_header(cdb) > 0) {
        return 0;
    }

    if (start != 0 && end != 0 && end < start) {
        /* printf("End [%ld] has to be >= start [%ld] in time interval requested\n", end, start); */
        return -1;
    }

    if (cdb->header == NULL || cdb->synced != 1) {
        /* printf("read_records: Nothing to read in [%s]\n", cdb->filename); */
        return -2;
    }

    /* bail out if there are no records */
    if (cdb->header->num_records <= 0) {
        /* printf("read_records: No records!\n"); */
        return 0;
    }

    /*
      get the number of requested records:
     
      -ve indicates n records from the beginning
      +ve indicates n records off of the end.
      0 or undef means the whole thing.
     
      switch the meaning of -ve/+ve to be more array like
    */
    if (num_requested != 0) {
        num_requested = -num_requested;
    }

    /*
    printf("read_records start: [%ld]\n", start);
    printf("read_records end: [%ld]\n", end);
    printf("read_records num_requested: [%ld]\n", num_requested);
    */

    if (num_requested != 0 && num_requested < 0 && start == 0) {
        /* if reading only few records from the end, just set -ve offset to seek to */
        first_requested_logical_record = num_requested;

    } else {
        /* compute which record to start reading from the beginning, based on start time specified. */
        first_requested_logical_record = _logical_record_for_time(cdb, start, 0, 0);
    }

    /* if end is not defined, read all the records or only read uptill the specified record. */
    if (end == 0) {

        last_requested_logical_record = cdb->header->num_records - 1;

    } else {

        last_requested_logical_record = _logical_record_for_time(cdb, end, 0, 0);

        /* this can return something > end, check for that */
        if (_time_for_logical_record(cdb, last_requested_logical_record) > end) {
            last_requested_logical_record -= 1;
        }
    }

    if (cdb->header->num_records == 0) {
        return 0;
    }

    last_requested_physical_record = (last_requested_logical_record + cdb->header->start_record) % cdb->header->num_records;

    seek_logical_record = first_requested_logical_record;
    seek_physical_record = _seek_to_logical_record(cdb, seek_logical_record);

    if (last_requested_physical_record >= seek_physical_record) {

        uint64_t nrec = (last_requested_physical_record - seek_physical_record + 1);
        uint64_t rlen = RECORD_SIZE * nrec;

        buffer = malloc(rlen);

        /* one slurp - XXX TODO - mmap */
        if (read(cdb->fd, buffer, rlen) != rlen) {
            free(buffer);
            return 0;
        }

        nrecs = nrec;

    } else {

        /* We've wrapped around the end of the file */
        uint64_t nrec1 = (cdb->header->num_records - seek_physical_record);
        uint64_t nrec2 = (last_requested_physical_record + 1);

        uint64_t rlen1 = RECORD_SIZE * nrec1;
        uint64_t rlen2 = RECORD_SIZE * nrec2;

        buffer = malloc(rlen1 + rlen2);

        if (read(cdb->fd, buffer, rlen1) != rlen1) {
            free(buffer);
            return 0;
        }

        if (pread(cdb->fd, &buffer[nrec1], rlen2, HEADER_SIZE) != rlen2) {
            free(buffer);
            return 0;
        }

        nrecs = nrec1 + nrec2;
    }

    /* Deal with cooking the output */
    if (cooked) {

        int factor = 0;
        uint64_t i, cooked_recs = 0;
        double new_value  = 0.0;
        double prev_value = 0.0;
        double val_delta  = 0.0;
        time_t time_delta = 0;
        time_t prev_date  = 0;
        
        cdb_record_t *crecords = malloc(RECORD_SIZE * nrecs);

        factor = _compute_scale_factor_and_num_records(cdb, &num_requested);

        *first_time = buffer[0].time;
        *last_time  = buffer[nrecs - 1].time;

        for (i = 0; i < nrecs; i++) {

            time_t date  = buffer[i].time;
            double value = buffer[i].value;

            if (!date || date < 0) {
                continue;
            }

            if (value == DBL_MAX) {
                value = DBL_MIN;
            }

            if (strcmp(cdb->header->type, "counter") == 0) {

                new_value = value;

                value = DBL_MAX;

                if (prev_value != DBL_MAX && new_value != DBL_MAX) {

                    /* Handle counter wrap arounds */
                    if (new_value >= prev_value) {
                        val_delta = new_value - prev_value;
                    } else {
                        val_delta = (WRAP_AROUND -prev_value) + new_value;
                    }

                    if (val_delta >= 0) {
                        value = val_delta;
                    }
                }

                prev_value = new_value;
            }

            if (factor != 0) {

                /* Skip the first entry, since it's absolute and is needed
                 * to calculate the second */
                if (prev_date == 0) {
                    prev_date = date;
                    continue;
                }

                time_delta = date - prev_date;

                if (time_delta > 0 && value != DBL_MAX) {
                    value = factor * (value / time_delta);
                }

                prev_date = date;
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
        nrecs  = cooked_recs;
    }

    /* now pull out the number of requested records if asked */
    if (num_requested != 0 && nrecs >= abs(num_requested)) {

        uint64_t start_index = 0;

        if (num_requested <= 0) {

            start_index = nrecs - abs(num_requested);
        }

        nrecs = abs(num_requested);

        *records = malloc(RECORD_SIZE * nrecs);

        memcpy(*records, &buffer[start_index], RECORD_SIZE * nrecs);

        free(buffer);

    } else {

        *records = buffer;
    }

    return nrecs;
}

void cdb_print_records(cdb_t *cdb, time_t start, time_t end, int64_t num_requested, FILE *fh, 
    const char *date_format, int cooked, time_t *first_time, time_t *last_time) {

    uint64_t i, num_recs = 0;
    
    cdb_record_t *records = NULL;

    num_recs = cdb_read_records(cdb, start, end, num_requested, cooked, first_time, last_time, &records);

    for (i = 0; i < num_recs; i++) {

        _print_record(fh, records[i].time, records[i].value, date_format);
    }

    free(records);
}

void cdb_print(cdb_t *cdb) {

    const char *date_format = "%Y-%m-%d %H:%M:%S";
    time_t first_time, last_time;

    printf("============== Header ================\n");
    cdb_read_header(cdb);
    cdb_print_header(cdb);

    printf("============== Records ================\n");
    cdb_print_records(cdb, 0, 0, 0, stdout, date_format, 0, &first_time, &last_time);
    printf("============== End ================\n");

    printf("============== Cooked Records ================\n");
    cdb_print_records(cdb, 0, 0, 0, stdout, date_format, 1, &first_time, &last_time);
    printf("============== End ================\n");
}

/* Take in an array of cdbs */
uint64_t cdb_read_aggregate_records(cdb_t **cdbs, int num_cdbs, time_t start, time_t end, int64_t num_requested,
    int cooked, time_t *first_time, time_t *last_time, cdb_record_t **records) {

    uint64_t i, driver_num_recs = 0;
    cdb_record_t *driver_records = NULL;

    /* The first cdb is the driver */
    driver_num_recs = cdb_read_records(cdbs[0], start, end, num_requested, cooked, first_time, last_time, &driver_records);

    double driver_x_values[driver_num_recs];
    double driver_y_values[driver_num_recs];

    *records = malloc(RECORD_SIZE * driver_num_recs);

    for (i = 0; i < driver_num_recs; i++) {
        (*records)[i].time  = driver_x_values[i] = driver_records[i].time;
        (*records)[i].value = driver_y_values[i] = driver_records[i].value;
    }

    /* initialize and allocate the gsl objects  */
    gsl_interp_accel *accel = gsl_interp_accel_alloc();
    gsl_interp *interp = gsl_interp_alloc(gsl_interp_linear, driver_num_recs);

    /* Useful for debugging */
    /* gsl_set_error_handler_off(); */

    gsl_interp_init(interp, driver_x_values, driver_y_values, driver_num_recs);

    for (i = 1; i < num_cdbs; i++) {

        uint64_t j, follower_num_recs = 0;

        double follower_x_values[driver_num_recs];
        double follower_y_values[driver_num_recs];

        cdb_record_t *follower_records = NULL;

        /* follower can't have more records than the driver */
        if (cdbs[i]->header->num_records < driver_num_recs) {
            continue;
        }

        follower_num_recs = cdb_read_records(cdbs[i], start, end, num_requested, cooked, first_time, last_time, &follower_records);

        if (follower_num_recs == 0) {
            continue;
        }

        for (j = 0; j < driver_num_recs; j++) {
            follower_x_values[j] = follower_records[j].time;
            follower_y_values[j] = follower_records[j].value;
        }

        for (j = 0; j < driver_num_recs; j++) {

            double yi = gsl_interp_eval(interp, follower_x_values, follower_y_values, driver_x_values[j], accel);

            if (isnormal(yi)) {
                (*records)[j].value += yi;
            }
        }

        free(follower_records);
    }

    gsl_interp_free(interp);
    gsl_interp_accel_free(accel);
    free(driver_records);

    return driver_num_recs;
}

void cdb_print_aggregate_records(cdb_t **cdbs, int num_cdbs, time_t start, time_t end, int64_t num_requested,
    FILE *fh, const char *date_format, int cooked, time_t *first_time, time_t *last_time) {

    uint64_t i, num_recs = 0;
    
    cdb_record_t *records = NULL;

    num_recs = cdb_read_aggregate_records(cdbs, num_cdbs, start, end, num_requested, cooked, first_time, last_time, &records);

    for (i = 0; i < num_recs; i++) {

        _print_record(fh, records[i].time, records[i].value, date_format);
    }

    free(records);
}

void cdb_generate_header(cdb_t *cdb, char* name, uint64_t max_records, char* type, char* units, char* description) {

    memset(cdb->header->name, 0, sizeof(cdb->header->name));
    memset(cdb->header->type, 0, sizeof(cdb->header->type));
    memset(cdb->header->units, 0, sizeof(cdb->header->units));
    memset(cdb->header->version, 0, sizeof(cdb->header->version));
    memset(cdb->header->description, 0, sizeof(cdb->header->description));

    if (max_records == 0) {
        max_records = CDB_DEFAULT_RECORDS;
    }

    if (type == NULL || (strcmp(type, "") == 0)) {
        type = (char*)CDB_DEFAULT_DATA_TYPE;
    }

    if (units == NULL || (strcmp(units, "") == 0)) {
        units = (char*)CDB_DEFAULT_DATA_UNIT;
    }

    if (description == NULL || (strcmp(description, "") == 0)) {
        sprintf(cdb->header->description, "Circular DB %s : %s with %"PRIu64" entries", name, type, max_records);
    } else {
        strncpy(cdb->header->description, description, strlen(description));
    }

    strncpy(cdb->header->name, name, strlen(name));
    strncpy(cdb->header->units, units, strlen(units));
    strncpy(cdb->header->type, type, strlen(type));
    strncpy(cdb->header->version, CDB_VERSION, strlen(CDB_VERSION));

    cdb->header->max_records = max_records;
    cdb->header->num_records = 0;
    cdb->header->start_record = 0;
}

cdb_t* cdb_new(void) {

    cdb_t *cdb = malloc(sizeof(cdb_t));
    memset(cdb, 0, sizeof(cdb_t));

    cdb->header = malloc(HEADER_SIZE);
    memset(cdb->header, 0, HEADER_SIZE);

    cdb->fd = -1;
    cdb->synced = 0;
    cdb->flags = -1;
    cdb->mode = -1;

    return cdb;
}

int cdb_open(cdb_t *cdb) {

    if (cdb->fd < 0) {

        /* Default flags if none were set */
        if (cdb->flags == -1) {
            cdb->flags = O_RDONLY;
        }

        if (cdb->mode == -1) {
            cdb->mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        }

        cdb->fd = open(cdb->filename, cdb->flags, cdb->mode);

        if (cdb->fd < 0) {
            return errno;
        }
    }

    return 0;
}

int cdb_close(cdb_t *cdb) {

    if (cdb != NULL) {

        if (cdb_write_header(cdb) > 0) {
            return errno;
        }

        if (cdb->fd > 0) {

            if (close(cdb->fd) == 0) {
                cdb->fd = -1;
                return 0;
            }

            return errno;
        }
    }

    return 0;
}

void cdb_free(cdb_t *cdb) {

    if (cdb != NULL) {

        cdb_close(cdb);

        if (cdb->header != NULL) {
            free(cdb->header);
        }

        free(cdb);
    }
}

int circulardb_run() {
    return 0;
}

/* -*- Mode: C; tab-width: 4 -*- */
/* vim: set tabstop=4 expandtab shiftwidth=4: */

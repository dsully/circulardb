#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <vector>

#include <fcntl.h>
#include "circulardb_interface.h"

using namespace std;

char formatted[256];

char* format_time(time_t time) {
  const char *date_format = "%Y-%m-%d %H:%M:%S";

  strftime(formatted, sizeof(formatted), date_format, localtime(&time));

  return (char*)&formatted;
}

void validate(cdb_t *cdb) {
  cout << "Working on: " << cdb->filename << endl;

  /* Check for exceptional conditions */ 
  if (cdb_read_header(cdb) != CDB_SUCCESS) {
    cout << "Couldn't read header for: " << cdb->filename << endl;
    return;
  }

  if (cdb->header->num_records == 0) {
    cout << "No records for: " << cdb->filename << endl;
    return;
  }

  std::set<cdb_time_t> all_dates;
  std::map<cdb_time_t, std::string> wraps;
  std::vector<cdb_time_t> duplicates;
  std::vector<cdb_time_t> bad_dates;

  cdb_time_t prev_date  = -1;
  double prev_value = -1;
  uint64_t num_recs = 0;
  uint64_t i = 0;

  cdb_range_t *range    = (cdb_range_t*)calloc(1, sizeof(cdb_range_t));
  cdb_request_t request = cdb_new_request();
  cdb_record_t *records = NULL;

  /* We want to check against raw data for counters */ 
  request.cooked = false;

  cdb_read_records(cdb, &request, &num_recs, &records, range);

  for (i = 0; i < num_recs; i++) {
    cdb_time_t date  = records[i].time;
    double value = records[i].value;

    if (all_dates.find(date) != all_dates.end()) {
      duplicates.push_back(date);
    } else {
      all_dates.insert(date);
    }

    if (cdb->header->type == CDB_TYPE_COUNTER && prev_value != -1 && (value < prev_value)) {
      std::ostringstream wrap_string;
      wrap_string << value << " < " << prev_value;
      cout << wrap_string.str() << endl;

      wraps[date] = wrap_string.str();
    }

    if (prev_date != -1 && date < prev_date) {
      bad_dates.push_back(date);
    }

    prev_date  = date;
    prev_value = value;
  }

  if (!bad_dates.empty()) {
    cout << "Error: DB has " << bad_dates.size() << " record(s) with out of order timestamps." << endl;

    for (unsigned int i = 0; i < bad_dates.size(); i++) {
      cout << "  [" << bad_dates[i] << "] " << format_time((time_t)bad_dates[i]) << endl;
    }
  }

  if (!duplicates.empty()) {
    cout << "Error: DB has " << duplicates.size() << " record(s) with duplicate timestamps." << endl;

    for (unsigned int i = 0; i < duplicates.size(); i++) {
      cout << "  [" << duplicates[i] << "] " << format_time((time_t)duplicates[i]) << endl;
    }
  }

  if (!wraps.empty()) {
    cout << "Error: DB has " << wraps.size() << " record(s) with counter wraps." << endl;

    for (std::map<cdb_time_t, std::string>::const_iterator it = wraps.begin(); it != wraps.end(); ++it) {
      cout << "  [" << it->first << "] " << format_time((time_t)it->first) << " :" << it->second << endl;
    }
  }

  cout << endl;

  free(records);
  free(range);
}

int main(int argc, char** argv) {
  int i;

  if (argc > 1) {
    for (i = 1; i < argc; i++) {
      int ret;

      cdb_t *cdb = cdb_new();
      cdb->filename = argv[i];
      cdb->flags = O_RDONLY;

      ret = cdb_read_header(cdb);

      if (ret == CDB_SUCCESS) {
        validate(cdb);
      } else if (ret == CDB_EBADTOK) {
        fprintf(stderr, "Couldn't open CircularDB file: Bad/bogus token.\n");
      } else if (ret == CDB_EBADVER) {
        fprintf(stderr, "Couldn't open CircularDB file: Incompatible version.\n");
      } else {
        perror("Couldn't open CircularDB file: Unknown error.\n");
      }

      cdb_close(cdb);
      cdb_free(cdb);
    }
  } else {
    printf("cdb_validate: Need at least 1 CircularDB file to validate.\n");
  }

  return(0);
}

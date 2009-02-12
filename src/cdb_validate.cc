#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <vector>

#include <fcntl.h>
#include "circulardb_interface.h"

using namespace std;

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

  std::set<time_t> all_dates;
  std::map<time_t, std::string> wraps;
  std::vector<time_t> duplicates;
  std::vector<time_t> bad_dates;

  time_t prev_date  = -1;
  double prev_value = -1;
  uint64_t num_recs = 0;
  uint64_t i = 0;

  cdb_range_t *range    = (cdb_range_t*)calloc(1, sizeof(cdb_range_t));
  cdb_request_t request = cdb_new_request();
  cdb_record_t *records = NULL;

  /* We want to check against raw data for counters */ 
  request.cooked = false;

  char formatted[256];
  const char *date_format = "%Y-%m-%d %H:%M:%S";

  cdb_read_records(cdb, &request, &num_recs, &records, range);

  for (i = 0; i < num_recs; i++) {
    time_t date  = records[i].time;
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

      strftime(formatted, sizeof(formatted), date_format, localtime(&bad_dates[i]));

      cout << "  [" << bad_dates[i] << "] " << formatted << endl;
    }
  }

  if (!duplicates.empty()) {
    cout << "Error: DB has " << duplicates.size() << " record(s) with duplicate timestamps." << endl;

    for (unsigned int i = 0; i < duplicates.size(); i++) {

      strftime(formatted, sizeof(formatted), date_format, localtime(&duplicates[i]));

      cout << "  [" << duplicates[i] << "] " << formatted << endl;
    }
  }

  if (!wraps.empty()) {
    cout << "Error: DB has " << wraps.size() << " record(s) with counter wraps." << endl;

    for (std::map<time_t, std::string>::const_iterator it = wraps.begin(); it != wraps.end(); ++it) {

      strftime(formatted, sizeof(formatted), date_format, localtime(&it->first));

      cout << "  [" << it->first << "] " << formatted << " :" << it->second << endl;
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
      cdb_t *cdb = cdb_new();
      cdb->filename = argv[i];
      cdb->flags = O_RDONLY;
      cdb_open(cdb);
      validate(cdb);
      cdb_close(cdb);
      cdb_free(cdb);
    }
  } else {
    printf("cdb_validate: Need at least 1 CircularDB file to validate.\n");
  }

  return(0);
}

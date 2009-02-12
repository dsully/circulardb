#include <fcntl.h>
#include "circulardb_interface.h"

int main(int argc, char** argv) {
  int i;

  if (argc > 1) {
    for (i = 1; i < argc; i++) {
      cdb_t *cdb = cdb_new();
      cdb->filename = argv[i];
      cdb->flags = O_RDONLY;
      cdb_open(cdb);
      cdb_print(cdb);
      cdb_close(cdb);
      cdb_free(cdb);
    }
  } else {
    printf("cdb_read: Need at least 1 CircularDB file to read.\n");
  }

  return(0);
}

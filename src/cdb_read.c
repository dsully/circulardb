#include <fcntl.h>
#include "circulardb_interface.h"

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
        cdb_print(cdb);
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
    printf("cdb_read: Need at least 1 CircularDB file to read.\n");
  }

  return(0);
}

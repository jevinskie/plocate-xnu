#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

struct mntent {
  char* mnt_dir;
  char* mnt_type;
};

int endmntent(FILE *fp);
struct mntent *getmntent(FILE *fp);
FILE *setmntent(const char* filename, const char *type);

#ifdef __cplusplus
} // extern "C"
#endif

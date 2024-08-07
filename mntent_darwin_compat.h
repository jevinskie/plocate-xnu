#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

struct mntent {
	char *mnt_dir;
	char *mnt_type;
};

FILE *setmntent(const char *filename, const char *type);
struct mntent *getmntent(FILE *fp);
int endmntent(FILE *fp);

#ifdef __cplusplus
}  // extern "C"
#endif

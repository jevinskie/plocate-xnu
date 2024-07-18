// clang-format: off
#undef NDEBUG
#include <cassert>
// clang-format: on

#include "mntent_darwin_compat.h"

#include <cstdlib>
#include <cstring>
#include <sys/mount.h>

struct bsd_mntent {
	int num_mounts;
	int idx;
	struct mntent *mntents;
};

FILE *setmntent(const char *filename, const char *type)
{
	auto *ctx = static_cast<bsd_mntent *>(calloc(1, sizeof(bsd_mntent)));
	assert(ctx);
	struct statfs *mounts = nullptr;
	ctx->num_mounts = getmntinfo_r_np(&mounts, 0);
	assert(mounts);
	assert(ctx->num_mounts >= 0);
	ctx->mntents = static_cast<mntent *>(calloc(ctx->num_mounts, sizeof(mntent)));
	assert(ctx->mntents);
	for (int i = 0; i < ctx->num_mounts; ++i) {
		ctx->mntents[i].mnt_dir = strdup(mounts[i].f_mntonname);
		ctx->mntents[i].mnt_type = strdup(mounts[i].f_fstypename);
	}
	free(mounts);
	return reinterpret_cast<FILE *>(ctx);
}

struct mntent *getmntent(FILE *fp)
{
	assert(fp);
	auto *ctx = reinterpret_cast<bsd_mntent *>(fp);
	mntent *res = nullptr;
	if (ctx->idx < ctx->num_mounts) {
		res = &ctx->mntents[ctx->idx++];
	}
	assert(res);
	return res;
}

int endmntent(FILE *fp)
{
	assert(fp);
	auto *ctx = reinterpret_cast<bsd_mntent *>(fp);
	for (int i = 0; i < ctx->num_mounts; ++i) {
		free(ctx->mntents[i].mnt_dir);
		free(ctx->mntents[i].mnt_type);
	}
	free(ctx->mntents);
	free(ctx);
	return 0;
}

#include "mntent_compat.h"

#undef NDEBUG
#include <cassert>
#include <cstdint>
#include <cstring>
#include <sys/mount.h>

struct bsd_mntent {
    int num_mounts;
    int idx;
    struct mntent *mntents;
};

FILE *setmntent(const char* filename, const char *type) {
    auto *ctx = (bsd_mntent *)malloc(sizeof(bsd_mntent));
    assert(ctx);
    ctx->idx = 0;
    struct statfs *mounts;
    ctx->num_mounts = getmntinfo_r_np(&mounts, 0);
    assert(ctx->num_mounts >= 0);
    ctx->mntents = (mntent *)malloc(sizeof(mntent) * ctx->num_mounts);
    assert(ctx->mntents);
    for (int i = 0; i < ctx->num_mounts; ++i) {
        ctx->mntents[i].mnt_dir = strdup(mounts[i].f_mntonname);
        ctx->mntents[i].mnt_type = strdup(mounts[i].f_fstypename);
    }
    free(mounts);
    return (FILE *)ctx;
}

struct mntent *getmntent(FILE *fp) {
    assert(fp);
    auto *ctx = (bsd_mntent *)fp;
    mntent *res;
    if (ctx->idx < ctx->num_mounts) {
        res = &ctx->mntents[ctx->idx];
        ++ctx->idx;
    } else {
        res = nullptr;
    }
    return res;
}

int endmntent(FILE *fp) {
    assert(fp);
    auto *ctx = (bsd_mntent *)fp;
    for (int i = 0; i < ctx->num_mounts; ++i) {
        free(ctx->mntents[i].mnt_dir);
        free(ctx->mntents[i].mnt_type);
    }
    free(ctx->mntents);
    free(ctx);
    return 0;
}
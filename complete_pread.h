#ifndef COMPLETE_PREAD_H
#define COMPLETE_PREAD_H 1

#include <unistd.h>

// A wrapper around pread() that retries on short reads and EINTR,
// so you never need to call it twice. Always synchronous (no io_uring).
bool try_complete_pread(int fd, void *ptr, size_t len, off_t offset);

// Same, but exit on failure, so never returns a short read.
void complete_pread(int fd, void *ptr, size_t len, off_t offset);

#endif  // !defined(COMPLETE_PREAD_H)

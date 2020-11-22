#ifndef COMPLETE_PREAD_H
#define COMPLETE_PREAD_H 1

#include <unistd.h>

// A wrapper around pread() that returns an incomplete read.
// Always synchronous (no io_uring).
void complete_pread(int fd, void *ptr, size_t len, off_t offset);

#endif  // !defined(COMPLETE_PREAD_H)

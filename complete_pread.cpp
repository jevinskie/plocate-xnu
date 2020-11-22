#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

void complete_pread(int fd, void *ptr, size_t len, off_t offset)
{
	while (len > 0) {
		ssize_t ret = pread(fd, ptr, len, offset);
		if (ret == -1 && errno == EINTR) {
			continue;
		}
		if (ret <= 0) {
			perror("pread");
			exit(1);
		}
		ptr = reinterpret_cast<char *>(ptr) + ret;
		len -= ret;
		offset -= ret;
	}
}

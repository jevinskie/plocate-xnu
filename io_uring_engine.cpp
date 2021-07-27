#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef WITHOUT_URING
#include <liburing.h>
#endif
#include "complete_pread.h"
#include "dprintf.h"
#include "io_uring_engine.h"

#include <functional>
#include <iosfwd>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

using namespace std;

IOUringEngine::IOUringEngine(size_t slop_bytes)
	: slop_bytes(slop_bytes)
{
#ifdef WITHOUT_URING
	int ret = -1;
	dprintf("Compiled without liburing support; not using io_uring.\n");
#else
	int ret = io_uring_queue_init(queue_depth, &ring, 0);
	if (ret < 0) {
		dprintf("io_uring_queue_init() failed; not using io_uring.\n");
	}
#endif
	using_uring = (ret >= 0);

#ifndef WITHOUT_URING
	if (using_uring) {
		io_uring_probe *probe = io_uring_get_probe_ring(&ring);
		supports_stat = (probe != nullptr && io_uring_opcode_supported(probe, IORING_OP_STATX));
		if (!supports_stat) {
			dprintf("io_uring on this kernel does not support statx(); will do synchronous access checking.\n");
		}
		free(probe);
	}
#endif
}

void IOUringEngine::submit_stat(const char *path, std::function<void(bool)> cb)
{
	assert(supports_stat);

#ifndef WITHOUT_URING
	if (pending_reads < queue_depth) {
		io_uring_sqe *sqe = io_uring_get_sqe(&ring);
		if (sqe == nullptr) {
			fprintf(stderr, "io_uring_get_sqe: %s\n", strerror(errno));
			exit(1);
		}
		submit_stat_internal(sqe, strdup(path), move(cb));
	} else {
		QueuedStat qs;
		qs.cb = move(cb);
		qs.pathname = strdup(path);
		queued_stats.push(move(qs));
	}
#endif
}

void IOUringEngine::submit_read(int fd, size_t len, off_t offset, function<void(string_view)> cb)
{
	if (!using_uring) {
		// Synchronous read.
		string s;
		s.resize(len + slop_bytes);
		complete_pread(fd, &s[0], len, offset);
		cb(string_view(s.data(), len));
		return;
	}

#ifndef WITHOUT_URING
	if (pending_reads < queue_depth) {
		io_uring_sqe *sqe = io_uring_get_sqe(&ring);
		if (sqe == nullptr) {
			fprintf(stderr, "io_uring_get_sqe: %s\n", strerror(errno));
			exit(1);
		}
		submit_read_internal(sqe, fd, len, offset, move(cb));
	} else {
		queued_reads.push(QueuedRead{ fd, len, offset, move(cb) });
	}
#endif
}

#ifndef WITHOUT_URING
void IOUringEngine::submit_read_internal(io_uring_sqe *sqe, int fd, size_t len, off_t offset, function<void(string_view)> cb)
{
	void *buf;
	if (posix_memalign(&buf, /*alignment=*/4096, len + slop_bytes)) {
		fprintf(stderr, "Couldn't allocate %zu bytes: %s\n", len, strerror(errno));
		exit(1);
	}

	PendingRead *pending = new PendingRead;
	pending->op = OP_READ;
	pending->read_cb = move(cb);
	pending->read.buf = buf;
	pending->read.len = len;
	pending->read.fd = fd;
	pending->read.offset = offset;
	pending->read.iov = iovec{ buf, len };

	io_uring_prep_readv(sqe, fd, &pending->read.iov, 1, offset);
	io_uring_sqe_set_data(sqe, pending);
	++pending_reads;
}

void IOUringEngine::submit_stat_internal(io_uring_sqe *sqe, char *path, std::function<void(bool)> cb)
{
	PendingRead *pending = new PendingRead;
	pending->op = OP_STAT;
	pending->stat_cb = move(cb);
	pending->stat.pathname = path;
	pending->stat.buf = new struct statx;

	io_uring_prep_statx(sqe, /*fd=*/-1, pending->stat.pathname, AT_STATX_SYNC_AS_STAT | AT_SYMLINK_NOFOLLOW, STATX_MODE, pending->stat.buf);
	io_uring_sqe_set_data(sqe, pending);
	++pending_reads;
}
#endif

void IOUringEngine::finish()
{
	if (!using_uring) {
		return;
	}

#ifndef WITHOUT_URING
	bool anything_to_submit = true;
	while (pending_reads > 0) {
		io_uring_cqe *cqe;
		if (io_uring_peek_cqe(&ring, &cqe) != 0) {
			if (anything_to_submit) {
				// Nothing ready, so submit whatever is pending and then do a blocking wait.
				int ret = io_uring_submit_and_wait(&ring, 1);
				if (ret < 0) {
					fprintf(stderr, "io_uring_submit(queued): %s\n", strerror(-ret));
					exit(1);
				}
				anything_to_submit = false;
			} else {
				int ret = io_uring_wait_cqe(&ring, &cqe);
				if (ret < 0) {
					fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
					exit(1);
				}
			}
		}

		unsigned head;
		io_uring_for_each_cqe(&ring, head, cqe)
		{
			PendingRead *pending = reinterpret_cast<PendingRead *>(cqe->user_data);
			if (pending->op == OP_STAT) {
				io_uring_cqe_seen(&ring, cqe);
				--pending_reads;

				size_t old_pending_reads = pending_reads;
				pending->stat_cb(cqe->res == 0);
				free(pending->stat.pathname);
				delete pending->stat.buf;
				delete pending;

				if (pending_reads != old_pending_reads) {
					// A new read was made in the callback (and not queued),
					// so we need to re-submit.
					anything_to_submit = true;
				}
			} else {
				if (cqe->res <= 0) {
					fprintf(stderr, "async read failed: %s\n", strerror(-cqe->res));
					exit(1);
				}

				if (size_t(cqe->res) < pending->read.iov.iov_len) {
					// Incomplete read, so resubmit it.
					pending->read.iov.iov_base = (char *)pending->read.iov.iov_base + cqe->res;
					pending->read.iov.iov_len -= cqe->res;
					pending->read.offset += cqe->res;
					io_uring_cqe_seen(&ring, cqe);

					io_uring_sqe *sqe = io_uring_get_sqe(&ring);
					if (sqe == nullptr) {
						fprintf(stderr, "No free SQE for resubmit; this shouldn't happen.\n");
						exit(1);
					}
					io_uring_prep_readv(sqe, pending->read.fd, &pending->read.iov, 1, pending->read.offset);
					io_uring_sqe_set_data(sqe, pending);
					anything_to_submit = true;
				} else {
					io_uring_cqe_seen(&ring, cqe);
					--pending_reads;

					size_t old_pending_reads = pending_reads;
					pending->read_cb(string_view(reinterpret_cast<char *>(pending->read.buf), pending->read.len));
					free(pending->read.buf);
					delete pending;

					if (pending_reads != old_pending_reads) {
						// A new read was made in the callback (and not queued),
						// so we need to re-submit.
						anything_to_submit = true;
					}
				}
			}
		}

		// See if there are any queued stats we can submit now.
		// Running a stat means we're very close to printing out a match,
		// which is more important than reading more blocks from disk.
		// (Even if those blocks returned early, they would only generate
		// more matches that would be blocked by this one in Serializer.)
		// Thus, prioritize stats.
		while (!queued_stats.empty() && pending_reads < queue_depth) {
			io_uring_sqe *sqe = io_uring_get_sqe(&ring);
			if (sqe == nullptr) {
				fprintf(stderr, "io_uring_get_sqe: %s\n", strerror(errno));
				exit(1);
			}
			QueuedStat &qs = queued_stats.front();
			submit_stat_internal(sqe, qs.pathname, move(qs.cb));
			queued_stats.pop();
			anything_to_submit = true;
		}

		// See if there are any queued reads we can submit now.
		while (!queued_reads.empty() && pending_reads < queue_depth) {
			io_uring_sqe *sqe = io_uring_get_sqe(&ring);
			if (sqe == nullptr) {
				fprintf(stderr, "io_uring_get_sqe: %s\n", strerror(errno));
				exit(1);
			}
			QueuedRead &qr = queued_reads.front();
			submit_read_internal(sqe, qr.fd, qr.len, qr.offset, move(qr.cb));
			queued_reads.pop();
			anything_to_submit = true;
		}
	}
#endif
}

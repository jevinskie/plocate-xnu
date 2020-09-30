#include <string.h>
#ifndef WITHOUT_URING
#include <liburing.h>
#endif
#include <stdint.h>
#include <unistd.h>
#include <memory>
#include <functional>

#include "io_uring_engine.h"

using namespace std;

IOUringEngine::IOUringEngine()
{
#ifdef WITHOUT_URING
	int ret = -1;
#else
	int ret = io_uring_queue_init(queue_depth, &ring, 0);
#endif
	using_uring = (ret >= 0);
}

void IOUringEngine::submit_read(int fd, size_t len, off_t offset, function<void(string)> cb)
{
	if (!using_uring) {
		// Synchronous read.
		string s;
		s.resize(len);
		complete_pread(fd, &s[0], len, offset);
		cb(move(s));
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
void IOUringEngine::submit_read_internal(io_uring_sqe *sqe, int fd, size_t len, off_t offset, function<void(string)> cb)
{
	void *buf;
	if (posix_memalign(&buf, /*alignment=*/4096, len)) {
		fprintf(stderr, "Couldn't allocate %zu bytes: %s\n", len, strerror(errno));
		exit(1);
	}
	PendingRead *pending = new PendingRead{ buf, len, move(cb), fd, offset, { buf, len } };

	io_uring_prep_readv(sqe, fd, &pending->iov, 1, offset);
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
	int ret = io_uring_submit(&ring);
	if (ret < 0) {
		fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
		exit(1);
	}
	bool anything_to_submit = false;
	while (pending_reads > 0) {
		io_uring_cqe *cqe;
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
			exit(1);
		}

		PendingRead *pending = reinterpret_cast<PendingRead *>(cqe->user_data);
		if (cqe->res <= 0) {
			fprintf(stderr, "async read failed: %s\n", strerror(-cqe->res));
			exit(1);
		}

		if (size_t(cqe->res) < pending->iov.iov_len) {
			// Incomplete read, so resubmit it.
			pending->iov.iov_base = (char *)pending->iov.iov_base + cqe->res;
			pending->iov.iov_len -= cqe->res;
			pending->offset += cqe->res;
			io_uring_cqe_seen(&ring, cqe);

			io_uring_sqe *sqe = io_uring_get_sqe(&ring);
			if (sqe == nullptr) {
				fprintf(stderr, "No free SQE for resubmit; this shouldn't happen.\n");
				exit(1);
			}
			io_uring_prep_readv(sqe, pending->fd, &pending->iov, 1, pending->offset);
			io_uring_sqe_set_data(sqe, pending);
			anything_to_submit = true;
		} else {
			io_uring_cqe_seen(&ring, cqe);
			--pending_reads;

			size_t old_pending_reads = pending_reads;
			pending->cb(string(reinterpret_cast<char *>(pending->buf), pending->len));
			free(pending->buf);
			delete pending;

			if (pending_reads != old_pending_reads) {
				// A new read was made in the callback (and not queued),
				// so we need to re-submit.
				anything_to_submit = true;
			}
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

		if (anything_to_submit) {
			// A new read was made, so we need to re-submit.
			int ret = io_uring_submit(&ring);
			if (ret < 0) {
				fprintf(stderr, "io_uring_submit(queued): %s\n", strerror(-ret));
				exit(1);
			} else {
				anything_to_submit = false;
			}
		}
	}
#endif
}

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



#ifndef IO_URING_ENGINE_H
#define IO_URING_ENGINE_H 1

#include <functional>
#include <queue>
#include <stdint.h>
#include <string>
#ifndef WITHOUT_URING
#include <liburing.h>
#endif
#include <sys/socket.h>

class IOUringEngine {
public:
	IOUringEngine(size_t slop_bytes);
	void submit_read(int fd, size_t len, off_t offset, std::function<void(std::string_view)> cb);
	void finish();
	size_t get_waiting_reads() const { return pending_reads + queued_reads.size(); }

private:
#ifndef WITHOUT_URING
	void submit_read_internal(io_uring_sqe *sqe, int fd, size_t len, off_t offset, std::function<void(std::string_view)> cb);

	io_uring ring;
#endif
	size_t pending_reads = 0;  // Number of requests we have going in the ring.
	bool using_uring;
	const size_t slop_bytes;

	struct QueuedRead {
		int fd;
		size_t len;
		off_t offset;
		std::function<void(std::string_view)> cb;
	};
	std::queue<QueuedRead> queued_reads;

	struct PendingRead {
		void *buf;
		size_t len;
		std::function<void(std::string_view)> cb;

		// For re-submission.
		int fd;
		off_t offset;
		iovec iov;
	};

	// 256 simultaneous requests should be ample, for slow and fast media alike.
	static constexpr size_t queue_depth = 256;
};

// A wrapper around pread() that returns an incomplete read.
// Always synchronous (no io_uring).
void complete_pread(int fd, void *ptr, size_t len, off_t offset);

#endif  // !defined(IO_URING_ENGINE_H)

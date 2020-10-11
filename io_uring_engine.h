#ifndef IO_URING_ENGINE_H
#define IO_URING_ENGINE_H 1

#include <functional>
#include <queue>
#include <stddef.h>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>

struct io_uring_sqe;
#ifndef WITHOUT_URING
#include <liburing.h>
#endif

class IOUringEngine {
public:
	IOUringEngine(size_t slop_bytes);
	void submit_read(int fd, size_t len, off_t offset, std::function<void(std::string_view)> cb);

	// NOTE: We just do the stat() to get the data into the dentry cache for fast access;
	// we don't care about the return value. Thus, the callback has no parameter lists.
	// If we have no io_uring, the callback will be made immediately, with no stat() call
	// being done.
	void submit_stat(const char *path, std::function<void()> cb);
	bool get_supports_stat() { return supports_stat; }

	void finish();
	size_t get_waiting_reads() const { return pending_reads + queued_reads.size(); }

private:
#ifndef WITHOUT_URING
	void submit_read_internal(io_uring_sqe *sqe, int fd, size_t len, off_t offset, std::function<void(std::string_view)> cb);
	void submit_stat_internal(io_uring_sqe *sqe, char *path, std::function<void()> cb);

	io_uring ring;
#endif
	size_t pending_reads = 0;  // Number of requests we have going in the ring.
	bool using_uring, supports_stat = false;
	const size_t slop_bytes;

	struct QueuedRead {
		int fd;
		size_t len;
		off_t offset;
		std::function<void(std::string_view)> cb;
	};
	std::queue<QueuedRead> queued_reads;

	struct QueuedStat {
		char *pathname;  // Owned by us.
		std::function<void()> cb;
	};
	std::queue<QueuedStat> queued_stats;

	enum Op { OP_READ,
	          OP_STAT };

	struct PendingRead {
		Op op;

		std::function<void(std::string_view)> read_cb;
		std::function<void()> stat_cb;

		union {
			struct {
				void *buf;
				size_t len;

				// For re-submission.
				int fd;
				off_t offset;
				iovec iov;
			} read;
			struct {
				char *pathname;
				struct statx *buf;
			} stat;
		};
	};

	// 256 simultaneous requests should be ample, for slow and fast media alike.
	static constexpr size_t queue_depth = 256;
};

// A wrapper around pread() that returns an incomplete read.
// Always synchronous (no io_uring).
void complete_pread(int fd, void *ptr, size_t len, off_t offset);

#endif  // !defined(IO_URING_ENGINE_H)

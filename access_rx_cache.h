#ifndef _ACCESS_RX_CACHE_H
#define _ACCESS_RX_CACHE_H 1

#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

class IOUringEngine;

class AccessRXCache {
public:
	AccessRXCache(IOUringEngine *engine)
		: engine(engine) {}
	void check_access(const char *filename, bool allow_async, std::function<void(bool)> cb);

private:
	std::unordered_map<std::string, bool> cache;
	struct PendingStat {
		std::string filename;
		std::function<void(bool)> cb;
	};
	std::map<std::string, std::vector<PendingStat>> pending_stats;
	IOUringEngine *engine;
	std::mutex mu;
};

#endif  // !defined(_ACCESS_RX_CACHE_H)

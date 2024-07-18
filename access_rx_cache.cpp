#include "access_rx_cache.h"

#include "io_uring_engine.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <utility>

using namespace std;

void AccessRXCache::check_access(const char *filename, bool allow_async, function<void(bool)> cb)
{
	if (!check_visibility) {
		cb(true);
		return;
	}

	lock_guard<mutex> lock(mu);
	if (engine == nullptr || !engine->get_supports_stat()) {
		allow_async = false;
	}

	for (const char *end = strchr(filename + 1, '/'); end != nullptr; end = strchr(end + 1, '/')) {
		string parent_path(filename, end - filename);  // string_view from C++20.
		auto cache_it = cache.find(parent_path);
		if (cache_it != cache.end()) {
			// Found in the cache.
			if (!cache_it->second) {
				cb(false);
				return;
			}
			continue;
		}

		if (!allow_async) {
			bool ok = access(parent_path.c_str(), R_OK | X_OK) == 0;
			cache.emplace(parent_path, ok);
			if (!ok) {
				cb(false);
				return;
			}
			continue;
		}

		// We want to call access(), but it could block on I/O. io_uring doesn't support
		// access(), but we can do a dummy asynchonous statx() to populate the kernel's cache,
		// which nearly always makes the next access() instantaneous.

		// See if there's already a pending stat that matches this,
		// or is a subdirectory.
		auto it = pending_stats.lower_bound(parent_path);
		if (it != pending_stats.end() && it->first.size() >= parent_path.size() &&
		    it->first.compare(0, parent_path.size(), parent_path) == 0) {
			it->second.emplace_back(PendingStat{ filename, std::move(cb) });
		} else {
			it = pending_stats.emplace(filename, vector<PendingStat>{}).first;
			engine->submit_stat(filename, [this, it, filename{ strdup(filename) }, cb{ std::move(cb) }](bool) {
				// The stat returned, so now do the actual access() calls.
				// All of them should be in cache, so don't fire off new statx()
				// calls during that check.
				check_access(filename, /*allow_async=*/false, std::move(cb));
				free(filename);

				// Call all others that waited for the same stat() to finish.
				// They may fire off new stat() calls if needed.
				vector<PendingStat> pending = std::move(it->second);
				pending_stats.erase(it);
				for (PendingStat &ps : pending) {
					check_access(ps.filename.c_str(), /*allow_async=*/true, std::move(ps.cb));
				}
			});
		}
		return;  // The rest will happen in async context.
	}

	// Passed all checks.
	cb(true);
}

#include "serializer.h"

#include "dprintf.h"

#include <chrono>
#include <inttypes.h>
#include <memory>
#include <stdio.h>
#include <stdlib.h>
#include <utility>

using namespace std;
using namespace std::chrono;

extern steady_clock::time_point start;

void apply_limit()
{
	if (--limit_left > 0) {
		return;
	}
	dprintf("Done in %.1f ms, found %" PRId64 " matches.\n",
	        1e3 * duration<float>(steady_clock::now() - start).count(), limit_matches);
	if (only_count) {
		printf("%" PRId64 "\n", limit_matches);
	}
	exit(0);
}

void Serializer::print(uint64_t seq, uint64_t skip, const string msg)
{
	if (only_count) {
		if (!msg.empty()) {
			apply_limit();
		}
		return;
	}

	if (next_seq != seq) {
		pending.push(Element{ seq, skip, move(msg) });
		return;
	}

	if (!msg.empty()) {
		if (print_nul) {
			printf("%s%c", msg.c_str(), 0);
		} else {
			printf("%s\n", msg.c_str());
		}
		apply_limit();
	}
	next_seq += skip;

	// See if any delayed prints can now be dealt with.
	while (!pending.empty() && pending.top().seq == next_seq) {
		if (!pending.top().msg.empty()) {
			if (print_nul) {
				printf("%s%c", pending.top().msg.c_str(), 0);
			} else {
				printf("%s\n", pending.top().msg.c_str());
			}
			apply_limit();
		}
		next_seq += pending.top().skip;
		pending.pop();
	}
}

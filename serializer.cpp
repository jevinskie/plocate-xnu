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

void print_possibly_escaped(const string &str)
{
	if (print_nul) {
		printf("%s%c", str.c_str(), 0);
		return;
	} else if (!stdout_is_tty) {
		printf("%s\n", str.c_str());
		return;
	}

	// stdout is a terminal, so we should protect the user against
	// escapes, stray newlines and the likes. First of all, check if
	// all the characters are safe; we consider everything safe that
	// isn't a control character, ', " or \. People could make
	// filenames like "$(rm -rf)", but that's out-of-scope.
	const char *ptr = str.data();
	size_t len = str.size();

	mbtowc(nullptr, 0, 0);
	wchar_t pwc;
	bool all_safe = true;
	do {
		int ret = mbtowc(&pwc, ptr, len);
		if (ret == -1) {
			all_safe = false;  // Malformed data.
		} else if (ret == 0) {
			break;  // EOF.
		} else if (pwc < 32 || pwc == '\'' || pwc == '"' || pwc == '\\') {
			all_safe = false;
		} else {
			ptr += ret;
			len -= ret;
		}
	} while (all_safe && *ptr != '\0');

	if (all_safe) {
		printf("%s\n", str.c_str());
		return;
	}

	// Print escaped, but in such a way that the user can easily take the
	// escaped output and paste into the shell. We print much like GNU ls does,
	// ie., using the shell $'foo' construct whenever we need to print something
	// escaped.
	bool in_escaped_mode = false;
	printf("'");

	mbtowc(nullptr, 0, 0);
	ptr = str.data();
	len = str.size();
	while (*ptr != '\0') {
		int ret = mbtowc(nullptr, ptr, len);
		if (ret == -1) {
			// Malformed data.
			printf("?");
			++ptr;
			--len;
			continue;
		} else if (ret == 0) {
			break;  // EOF.
		}
		if (*ptr < 32 || *ptr == '\'' || *ptr == '"' || *ptr == '\\') {
			if (!in_escaped_mode) {
				printf("'$'");
				in_escaped_mode = true;
			}

			// The list of allowed escapes is from bash(1).
			switch (*ptr) {
			case '\a':
				printf("\\a");
				break;
			case '\b':
				printf("\\b");
				break;
			case '\f':
				printf("\\f");
				break;
			case '\n':
				printf("\\n");
				break;
			case '\r':
				printf("\\r");
				break;
			case '\t':
				printf("\\t");
				break;
			case '\v':
				printf("\\v");
				break;
			case '\\':
				printf("\\\\");
				break;
			case '\'':
				printf("\\'");
				break;
			case '"':
				printf("\\\"");
				break;
			default:
				printf("\\%03o", *ptr);
				break;
			}
		} else {
			if (in_escaped_mode) {
				printf("''");
				in_escaped_mode = false;
			}
			fwrite(ptr, ret, 1, stdout);
		}
		ptr += ret;
		len -= ret;
	}
	printf("'\n");
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
		print_possibly_escaped(msg);
		apply_limit();
	}
	next_seq += skip;

	// See if any delayed prints can now be dealt with.
	while (!pending.empty() && pending.top().seq == next_seq) {
		if (!pending.top().msg.empty()) {
			print_possibly_escaped(pending.top().msg);
			apply_limit();
		}
		next_seq += pending.top().skip;
		pending.pop();
	}
}

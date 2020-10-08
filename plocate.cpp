#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <zstd.h>
#include <getopt.h>
#include <stdlib.h>
#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <stdint.h>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <queue>
#include <string_view>
#include <utility>

#include "db.h"
#include "io_uring_engine.h"

using namespace std;
using namespace std::chrono;

#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);

#include "turbopfor.h"

const char *dbpath = "/var/lib/mlocate/plocate.db";
bool only_count = false;
bool print_nul = false;
int64_t limit_matches = numeric_limits<int64_t>::max();

class Serializer {
public:
	bool ready_to_print(int seq) { return next_seq == seq; }
	void print_delayed(int seq, const vector<string> msg);
	void release_current();

private:
	int next_seq = 0;
	struct Element {
		int seq;
		vector<string> msg;

		bool operator<(const Element &other) const
		{
			return seq > other.seq;
		}
	};
	priority_queue<Element> pending;
};

void Serializer::print_delayed(int seq, const vector<string> msg)
{
	pending.push(Element{ seq, move(msg) });
}

void Serializer::release_current()
{
	++next_seq;

	// See if any delayed prints can now be dealt with.
	while (!pending.empty() && pending.top().seq == next_seq) {
		if (limit_matches-- <= 0)
			return;
		for (const string &msg : pending.top().msg) {
			if (print_nul) {
				printf("%s%c", msg.c_str(), 0);
			} else {
				printf("%s\n", msg.c_str());
			}
		}
		pending.pop();
		++next_seq;
	}
}

static inline uint32_t read_unigram(const string &s, size_t idx)
{
	if (idx < s.size()) {
		return (unsigned char)s[idx];
	} else {
		return 0;
	}
}

static inline uint32_t read_trigram(const string &s, size_t start)
{
	return read_unigram(s, start) | (read_unigram(s, start + 1) << 8) |
		(read_unigram(s, start + 2) << 16);
}

bool has_access(const char *filename,
                unordered_map<string, bool> *access_rx_cache)
{
	const char *end = strchr(filename + 1, '/');
	while (end != nullptr) {
		string parent_path(filename, end);
		auto it = access_rx_cache->find(parent_path);
		bool ok;
		if (it == access_rx_cache->end()) {
			ok = access(parent_path.c_str(), R_OK | X_OK) == 0;
			access_rx_cache->emplace(move(parent_path), ok);
		} else {
			ok = it->second;
		}
		if (!ok) {
			return false;
		}
		end = strchr(end + 1, '/');
	}

	return true;
}

class Corpus {
public:
	Corpus(int fd, IOUringEngine *engine);
	~Corpus();
	void find_trigram(uint32_t trgm, function<void(const Trigram *trgmptr, size_t len)> cb);
	void get_compressed_filename_block(uint32_t docid, function<void(string_view)> cb) const;
	size_t get_num_filename_blocks() const;
	off_t offset_for_block(uint32_t docid) const
	{
		return hdr.filename_index_offset_bytes + docid * sizeof(uint64_t);
	}

public:
	const int fd;
	IOUringEngine *const engine;

	Header hdr;
};

Corpus::Corpus(int fd, IOUringEngine *engine)
	: fd(fd), engine(engine)
{
	// Enable to test cold-cache behavior (except for access()).
	if (false) {
		off_t len = lseek(fd, 0, SEEK_END);
		if (len == -1) {
			perror("lseek");
			exit(1);
		}
		posix_fadvise(fd, 0, len, POSIX_FADV_DONTNEED);
	}

	complete_pread(fd, &hdr, sizeof(hdr), /*offset=*/0);
	if (memcmp(hdr.magic, "\0plocate", 8) != 0) {
		fprintf(stderr, "plocate.db is corrupt or an old version; please rebuild it.\n");
		exit(1);
	}
	if (hdr.version != 0) {
		fprintf(stderr, "plocate.db has version %u, expected 0; please rebuild it.\n", hdr.version);
		exit(1);
	}
}

Corpus::~Corpus()
{
	close(fd);
}

void Corpus::find_trigram(uint32_t trgm, function<void(const Trigram *trgmptr, size_t len)> cb)
{
	uint32_t bucket = hash_trigram(trgm, hdr.hashtable_size);
	engine->submit_read(fd, sizeof(Trigram) * (hdr.extra_ht_slots + 2), hdr.hash_table_offset_bytes + sizeof(Trigram) * bucket, [this, trgm, cb{ move(cb) }](string_view s) {
		const Trigram *trgmptr = reinterpret_cast<const Trigram *>(s.data());
		for (unsigned i = 0; i < hdr.extra_ht_slots + 1; ++i) {
			if (trgmptr[i].trgm == trgm) {
				cb(trgmptr + i, trgmptr[i + 1].offset - trgmptr[i].offset);
				return;
			}
		}

		// Not found.
		cb(nullptr, 0);
	});
}

void Corpus::get_compressed_filename_block(uint32_t docid, function<void(string_view)> cb) const
{
	// Read the file offset from this docid and the next one.
	// This is always allowed, since we have a sentinel block at the end.
	engine->submit_read(fd, sizeof(uint64_t) * 2, offset_for_block(docid), [this, cb{ move(cb) }](string_view s) {
		const uint64_t *ptr = reinterpret_cast<const uint64_t *>(s.data());
		off_t offset = ptr[0];
		size_t len = ptr[1] - ptr[0];
		engine->submit_read(fd, len, offset, cb);
	});
}

size_t Corpus::get_num_filename_blocks() const
{
	return hdr.num_docids;
}

uint64_t scan_file_block(const vector<string> &needles, string_view compressed,
                         unordered_map<string, bool> *access_rx_cache, int seq,
                         Serializer *serializer)
{
	uint64_t matched = 0;

	unsigned long long uncompressed_len = ZSTD_getFrameContentSize(compressed.data(), compressed.size());
	if (uncompressed_len == ZSTD_CONTENTSIZE_UNKNOWN || uncompressed_len == ZSTD_CONTENTSIZE_ERROR) {
		fprintf(stderr, "ZSTD_getFrameContentSize() failed\n");
		exit(1);
	}

	string block;
	block.resize(uncompressed_len + 1);

	size_t err = ZSTD_decompress(&block[0], block.size(), compressed.data(),
	                             compressed.size());
	if (ZSTD_isError(err)) {
		fprintf(stderr, "ZSTD_decompress(): %s\n", ZSTD_getErrorName(err));
		exit(1);
	}
	block[block.size() - 1] = '\0';

	bool immediate_print = (serializer == nullptr || serializer->ready_to_print(seq));
	vector<string> delayed;

	for (const char *filename = block.data();
	     filename != block.data() + block.size();
	     filename += strlen(filename) + 1) {
		bool found = true;
		for (const string &needle : needles) {
			if (strstr(filename, needle.c_str()) == nullptr) {
				found = false;
				break;
			}
		}
		if (found && has_access(filename, access_rx_cache)) {
			if (limit_matches-- <= 0)
				break;
			++matched;
			if (only_count)
				continue;
			if (immediate_print) {
				if (print_nul) {
					printf("%s%c", filename, 0);
				} else {
					printf("%s\n", filename);
				}
			} else {
				delayed.push_back(filename);
			}
		}
	}
	if (serializer != nullptr && !only_count) {
		if (immediate_print) {
			serializer->release_current();
		} else {
			serializer->print_delayed(seq, move(delayed));
		}
	}
	return matched;
}

size_t scan_docids(const vector<string> &needles, const vector<uint32_t> &docids, const Corpus &corpus, IOUringEngine *engine)
{
	Serializer docids_in_order;
	unordered_map<string, bool> access_rx_cache;
	uint64_t matched = 0;
	for (size_t i = 0; i < docids.size(); ++i) {
		uint32_t docid = docids[i];
		corpus.get_compressed_filename_block(docid, [i, &matched, &needles, &access_rx_cache, &docids_in_order](string_view compressed) {
			matched += scan_file_block(needles, compressed, &access_rx_cache, i, &docids_in_order);
		});
	}
	engine->finish();
	return matched;
}

// We do this sequentially, as it's faster than scattering
// a lot of I/O through io_uring and hoping the kernel will
// coalesce it plus readahead for us.
uint64_t scan_all_docids(const vector<string> &needles, int fd, const Corpus &corpus, IOUringEngine *engine)
{
	unordered_map<string, bool> access_rx_cache;
	uint32_t num_blocks = corpus.get_num_filename_blocks();
	unique_ptr<uint64_t[]> offsets(new uint64_t[num_blocks + 1]);
	complete_pread(fd, offsets.get(), (num_blocks + 1) * sizeof(uint64_t), corpus.offset_for_block(0));
	string compressed;
	uint64_t matched = 0;
	for (uint32_t io_docid = 0; io_docid < num_blocks; io_docid += 32) {
		uint32_t last_docid = std::min(io_docid + 32, num_blocks);
		size_t io_len = offsets[last_docid] - offsets[io_docid];
		if (compressed.size() < io_len) {
			compressed.resize(io_len);
		}
		complete_pread(fd, &compressed[0], io_len, offsets[io_docid]);

		for (uint32_t docid = io_docid; docid < last_docid; ++docid) {
			size_t relative_offset = offsets[docid] - offsets[io_docid];
			size_t len = offsets[docid + 1] - offsets[docid];
			matched += scan_file_block(needles, { &compressed[relative_offset], len }, &access_rx_cache, 0, nullptr);
			if (limit_matches <= 0)
				return matched;
		}
	}
	return matched;
}

void do_search_file(const vector<string> &needles, const char *filename)
{
	int fd = open(filename, O_RDONLY);
	if (fd == -1) {
		perror(filename);
		exit(1);
	}

	// Drop privileges.
	if (setgid(getgid()) != 0) {
		perror("setgid");
		exit(EXIT_FAILURE);
	}

	steady_clock::time_point start __attribute__((unused)) = steady_clock::now();
	if (access("/", R_OK | X_OK)) {
		// We can't find anything, no need to bother...
		return;
	}

	IOUringEngine engine(/*slop_bytes=*/16);  // 16 slop bytes as described in turbopfor.h.
	Corpus corpus(fd, &engine);
	dprintf("Corpus init done after %.1f ms.\n", 1e3 * duration<float>(steady_clock::now() - start).count());

	vector<pair<Trigram, size_t>> trigrams;
	uint64_t shortest_so_far = numeric_limits<uint32_t>::max();
	for (const string &needle : needles) {
		if (needle.size() < 3)
			continue;
		for (size_t i = 0; i < needle.size() - 2; ++i) {
			uint32_t trgm = read_trigram(needle, i);
			corpus.find_trigram(trgm, [trgm, &trigrams, &shortest_so_far](const Trigram *trgmptr, size_t len) {
				if (trgmptr == nullptr) {
					dprintf("trigram '%c%c%c' isn't found, we abort the search\n",
					        trgm & 0xff, (trgm >> 8) & 0xff, (trgm >> 16) & 0xff);
					if (only_count) {
						printf("0\n");
					}
					exit(0);
				}
				if (trgmptr->num_docids > shortest_so_far * 100) {
					dprintf("not loading trigram '%c%c%c' with %u docids, it would be ignored later anyway\n",
					        trgm & 0xff, (trgm >> 8) & 0xff, (trgm >> 16) & 0xff,
					        trgmptr->num_docids);
				} else {
					trigrams.emplace_back(*trgmptr, len);
					shortest_so_far = std::min<uint64_t>(shortest_so_far, trgmptr->num_docids);
				}
			});
		}
	}
	engine.finish();
	dprintf("Hashtable lookups done after %.1f ms.\n", 1e3 * duration<float>(steady_clock::now() - start).count());

	if (trigrams.empty()) {
		// Too short for trigram matching. Apply brute force.
		// (We could have searched through all trigrams that matched
		// the pattern and done a union of them, but that's a lot of
		// work for fairly unclear gain.)
		uint64_t matched = scan_all_docids(needles, fd, corpus, &engine);
		printf("%zu\n", matched);
		return;
	}
	sort(trigrams.begin(), trigrams.end());
	{
		auto last = unique(trigrams.begin(), trigrams.end());
		trigrams.erase(last, trigrams.end());
	}
	sort(trigrams.begin(), trigrams.end(),
	     [&](const pair<Trigram, size_t> &a, const pair<Trigram, size_t> &b) {
		     return a.first.num_docids < b.first.num_docids;
	     });

	vector<uint32_t> in1, in2, out;
	bool done = false;
	for (auto [trgmptr, len] : trigrams) {
		if (!in1.empty() && trgmptr.num_docids > in1.size() * 100) {
			uint32_t trgm __attribute__((unused)) = trgmptr.trgm;
			dprintf("trigram '%c%c%c' (%zu bytes) has %u entries, ignoring the rest (will "
			        "weed out false positives later)\n",
			        trgm & 0xff, (trgm >> 8) & 0xff, (trgm >> 16) & 0xff,
			        len, trgmptr.num_docids);
			break;
		}

		// Only stay a certain amount ahead, so that we don't spend I/O
		// on reading the latter, large posting lists. We are unlikely
		// to need them anyway, even if they should come in first.
		if (engine.get_waiting_reads() >= 5) {
			engine.finish();
			if (done)
				break;
		}
		engine.submit_read(fd, len, trgmptr.offset, [trgmptr{ trgmptr }, len{ len }, &done, &in1, &in2, &out](string_view s) {
			if (done)
				return;
			uint32_t trgm __attribute__((unused)) = trgmptr.trgm;
			size_t num = trgmptr.num_docids;
			const unsigned char *pldata = reinterpret_cast<const unsigned char *>(s.data());
			if (in1.empty()) {
				in1.resize(num + 128);
				decode_pfor_delta1_128(pldata, num, /*interleaved=*/true, &in1[0]);
				in1.resize(num);
				dprintf("trigram '%c%c%c' (%zu bytes) decoded to %zu entries\n", trgm & 0xff,
				        (trgm >> 8) & 0xff, (trgm >> 16) & 0xff, len, num);
			} else {
				if (in2.size() < num + 128) {
					in2.resize(num + 128);
				}
				decode_pfor_delta1_128(pldata, num, /*interleaved=*/true, &in2[0]);

				out.clear();
				set_intersection(in1.begin(), in1.end(), in2.begin(), in2.begin() + num,
				                 back_inserter(out));
				swap(in1, out);
				dprintf("trigram '%c%c%c' (%zu bytes) decoded to %zu entries, %zu left\n",
				        trgm & 0xff, (trgm >> 8) & 0xff, (trgm >> 16) & 0xff,
				        len, num, in1.size());
				if (in1.empty()) {
					dprintf("no matches (intersection list is empty)\n");
					done = true;
				}
			}
		});
	}
	engine.finish();
	if (done) {
		return;
	}
	dprintf("Intersection done after %.1f ms. Doing final verification and printing:\n",
	        1e3 * duration<float>(steady_clock::now() - start).count());

	uint64_t matched = scan_docids(needles, in1, corpus, &engine);
	dprintf("Done in %.1f ms, found %zu matches.\n",
	        1e3 * duration<float>(steady_clock::now() - start).count(), matched);

	if (only_count) {
		printf("%zu\n", matched);
	}
}

void usage()
{
	// The help text comes from mlocate.
	printf("Usage: plocate [OPTION]... PATTERN...\n");
	printf("\n");
	printf("  -c, --count            only print number of found entries\n");
	printf("  -d, --database DBPATH  use DBPATH instead of default database (which is\n");
	printf("                         %s)\n", dbpath);
	printf("  -h, --help             print this help\n");
	printf("  -l, --limit, -n LIMIT  limit output (or counting) to LIMIT entries\n");
	printf("  -0, --null             separate entries with NUL on output\n");
}

int main(int argc, char **argv)
{
	static const struct option long_options[] = {
		{ "help", no_argument, 0, 'h' },
		{ "count", no_argument, 0, 'c' },
		{ "database", required_argument, 0, 'd' },
		{ "limit", required_argument, 0, 'l' },
		{ nullptr, required_argument, 0, 'n' },
		{ "null", no_argument, 0, '0' },
		{ 0, 0, 0, 0 }
	};

	for (;;) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "cd:hl:n:0", long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch (c) {
		case 'c':
			only_count = true;
			break;
		case 'd':
			dbpath = strdup(optarg);
			break;
		case 'h':
			usage();
			exit(0);
		case 'l':
		case 'n':
			limit_matches = atoll(optarg);
			break;
		case '0':
			print_nul = true;
			break;
		default:
			exit(1);
		}
	}

	vector<string> needles;
	for (int i = optind; i < argc; ++i) {
		needles.push_back(argv[i]);
	}
	if (needles.empty()) {
		fprintf(stderr, "plocate: no pattern to search for specified\n");
		exit(0);
	}
	do_search_file(needles, dbpath);
}

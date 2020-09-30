#include "vp4.h"
#include "io_uring_engine.h"

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <endian.h>
#include <fcntl.h>
#include <functional>
#include <memory>
#include <stdio.h>
#include <string.h>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <zstd.h>

using namespace std;
using namespace std::chrono;

#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);

class Serializer {
public:
	void do_or_wait(int seq, function<void()> cb);

private:
	int next_seq = 0;
	struct Element {
		int seq;
		function<void()> cb;

		bool operator<(const Element &other) const
		{
			return seq > other.seq;
		}
	};
	priority_queue<Element> pending;
};

void Serializer::do_or_wait(int seq, function<void()> cb)
{
	if (seq != next_seq) {
		pending.emplace(Element{ seq, move(cb) });
		return;
	}

	cb();
	++next_seq;

	while (!pending.empty() && pending.top().seq == next_seq) {
		pending.top().cb();
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

struct Trigram {
	uint32_t trgm;
	uint32_t num_docids;
	uint64_t offset;

	bool operator==(const Trigram &other) const
	{
		return trgm == other.trgm;
	}
	bool operator<(const Trigram &other) const
	{
		return trgm < other.trgm;
	}
};

class Corpus {
public:
	Corpus(int fd, IOUringEngine *engine);
	~Corpus();
	void find_trigram(uint32_t trgm, function<void(const Trigram *trgmptr, size_t len)> cb);
	void get_compressed_filename_block(uint32_t docid, function<void(string)> cb) const;
	size_t get_num_filename_blocks() const;
	off_t offset_for_block(uint32_t docid) const {
		return filename_index_offset + docid * sizeof(uint64_t);
	}

public:
	const int fd;
	IOUringEngine *const engine;

	off_t len;
	uint64_t filename_index_offset;

	uint64_t num_trigrams;
	const off_t trigram_offset = sizeof(uint64_t) * 2;

	void binary_search_trigram(uint32_t trgm, uint32_t left, uint32_t right, function<void(const Trigram *trgmptr, size_t len)> cb);
};

Corpus::Corpus(int fd, IOUringEngine *engine)
	: fd(fd), engine(engine)
{
	len = lseek(fd, 0, SEEK_END);
	if (len == -1) {
		perror("lseek");
		exit(1);
	}

	// Uncomment to test cold-cache behavior (except for access()).
	// posix_fadvise(fd, 0, len, POSIX_FADV_DONTNEED);

	uint64_t vals[2];
	complete_pread(fd, vals, sizeof(vals), /*offset=*/0);

	num_trigrams = vals[0];
	filename_index_offset = vals[1];
}

Corpus::~Corpus()
{
	close(fd);
}

void Corpus::find_trigram(uint32_t trgm, function<void(const Trigram *trgmptr, size_t len)> cb)
{
	binary_search_trigram(trgm, 0, num_trigrams - 1, move(cb));
}

void Corpus::binary_search_trigram(uint32_t trgm, uint32_t left, uint32_t right, function<void(const Trigram *trgmptr, size_t len)> cb)
{
	if (left > right) {
		cb(nullptr, 0);
		return;
	}
	uint32_t mid = (left + right) / 2;
	engine->submit_read(fd, sizeof(Trigram) * 2, trigram_offset + sizeof(Trigram) * mid, [this, trgm, left, mid, right, cb{ move(cb) }](string s) {
		const Trigram *trgmptr = reinterpret_cast<const Trigram *>(s.data());
		const Trigram *next_trgmptr = trgmptr + 1;
		if (trgmptr->trgm < trgm) {
			binary_search_trigram(trgm, mid + 1, right, move(cb));
		} else if (trgmptr->trgm > trgm) {
			binary_search_trigram(trgm, left, mid - 1, move(cb));
		} else {
			cb(trgmptr, next_trgmptr->offset - trgmptr->offset);
		}
	});
}

void Corpus::get_compressed_filename_block(uint32_t docid, function<void(string)> cb) const
{
	// Read the file offset from this docid and the next one.
	// This is always allowed, since we have a sentinel block at the end.
	engine->submit_read(fd, sizeof(uint64_t) * 2, offset_for_block(docid), [this, cb{ move(cb) }](string s) {
		const uint64_t *ptr = reinterpret_cast<const uint64_t *>(s.data());
		off_t offset = ptr[0];
		size_t len = ptr[1] - ptr[0];
		engine->submit_read(fd, len, offset, cb);
	});
}

size_t Corpus::get_num_filename_blocks() const
{
	// The beginning of the filename blocks is the end of the filename index blocks.
	uint64_t end;
	complete_pread(fd, &end, sizeof(end), filename_index_offset);

	// Subtract the sentinel block.
	return (end - filename_index_offset) / sizeof(uint64_t) - 1;
}

size_t scan_file_block(const string &needle, string_view compressed,
                       unordered_map<string, bool> *access_rx_cache)
{
	size_t matched = 0;

	string block;
	block.resize(ZSTD_getFrameContentSize(compressed.data(), compressed.size()) +
	             1);

	ZSTD_decompress(&block[0], block.size(), compressed.data(),
	                compressed.size());
	block[block.size() - 1] = '\0';

	for (const char *filename = block.data();
	     filename != block.data() + block.size();
	     filename += strlen(filename) + 1) {
		if (strstr(filename, needle.c_str()) == nullptr) {
			continue;
		}
		if (has_access(filename, access_rx_cache)) {
			++matched;
			printf("%s\n", filename);
		}
	}
	return matched;
}

size_t scan_docids(const string &needle, const vector<uint32_t> &docids, const Corpus &corpus, IOUringEngine *engine)
{
	Serializer docids_in_order;
	unordered_map<string, bool> access_rx_cache;
	size_t matched = 0;
	for (size_t i = 0; i < docids.size(); ++i) {
		uint32_t docid = docids[i];
		corpus.get_compressed_filename_block(docid, [i, &matched, &needle, &access_rx_cache, &docids_in_order](string compressed) {
			docids_in_order.do_or_wait(i, [&matched, &needle, compressed{ move(compressed) }, &access_rx_cache] {
				matched += scan_file_block(needle, compressed, &access_rx_cache);
			});
		});
	}
	engine->finish();
	return matched;
}

// We do this sequentially, as it's faster than scattering
// a lot of I/O through io_uring and hoping the kernel will
// coalesce it plus readahead for us.
void scan_all_docids(const string &needle, int fd, const Corpus &corpus, IOUringEngine *engine)
{
	unordered_map<string, bool> access_rx_cache;
	uint32_t num_blocks = corpus.get_num_filename_blocks();
	unique_ptr<uint64_t[]> offsets(new uint64_t[num_blocks + 1]);
	complete_pread(fd, offsets.get(), (num_blocks + 1) * sizeof(uint64_t), corpus.offset_for_block(0));
	string compressed;
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
			scan_file_block(needle, {&compressed[relative_offset], len}, &access_rx_cache);
		}
	}
}

void do_search_file(const string &needle, const char *filename)
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

	IOUringEngine engine;
	Corpus corpus(fd, &engine);
	dprintf("Corpus init took %.1f ms.\n", 1e3 * duration<float>(steady_clock::now() - start).count());

	if (needle.size() < 3) {
		// Too short for trigram matching. Apply brute force.
		// (We could have searched through all trigrams that matched
		// the pattern and done a union of them, but that's a lot of
		// work for fairly unclear gain.)
		scan_all_docids(needle, fd, corpus, &engine);
		return;
	}

	vector<pair<Trigram, size_t>> trigrams;
	for (size_t i = 0; i < needle.size() - 2; ++i) {
		uint32_t trgm = read_trigram(needle, i);
		pair<uint32_t, uint32_t> range{ 0, corpus.num_trigrams - 1 };
		corpus.find_trigram(trgm, [trgm, &trigrams](const Trigram *trgmptr, size_t len) {
			if (trgmptr == nullptr) {
				dprintf("trigram %06x isn't found, we abort the search\n", trgm);
				return;
			}
			trigrams.emplace_back(*trgmptr, len);
		});
	}
	engine.finish();
	dprintf("Binary search took %.1f ms.\n", 1e3 * duration<float>(steady_clock::now() - start).count());

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
		engine.submit_read(fd, len, trgmptr.offset, [trgmptr, len, &done, &in1, &in2, &out](string s) {
			uint32_t trgm __attribute__((unused)) = trgmptr.trgm;
			size_t num = trgmptr.num_docids;
			unsigned char *pldata = reinterpret_cast<unsigned char *>(s.data());
			if (in1.empty()) {
				in1.resize(num + 128);
				p4nd1dec128v32(pldata, num, &in1[0]);
				in1.resize(num);
				dprintf("trigram '%c%c%c' (%zu bytes) decoded to %zu entries\n", trgm & 0xff,
				        (trgm >> 8) & 0xff, (trgm >> 16) & 0xff, len, num);
			} else {
				if (in2.size() < num + 128) {
					in2.resize(num + 128);
				}
				p4nd1dec128v32(pldata, num, &in2[0]);

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
	dprintf("Intersection took %.1f ms. Doing final verification and printing:\n",
	        1e3 * duration<float>(steady_clock::now() - start).count());

	size_t matched __attribute__((unused)) = scan_docids(needle, in1, corpus, &engine);
	dprintf("Done in %.1f ms, found %zu matches.\n",
	        1e3 * duration<float>(steady_clock::now() - start).count(), matched);
}

int main(int argc, char **argv)
{
	do_search_file(argv[1], "/var/lib/mlocate/plocate.db");
}

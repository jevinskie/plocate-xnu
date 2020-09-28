#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <unordered_map>
#include <string>
#include <vector>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <arpa/inet.h>
#include <endian.h>
#include <zstd.h>

#include "vp4.h"

#define P4NENC_BOUND(n) ((n+127)/128+(n+32)*sizeof(uint32_t))

using namespace std;
using namespace std::chrono;

#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);
	
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
	return read_unigram(s, start) |
		(read_unigram(s, start + 1) << 8) |
		(read_unigram(s, start + 2) << 16);
}

bool has_access(const char *filename, unordered_map<string, bool> *access_rx_cache)
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

#if 0
	// Check for rx first in the cache; if that isn't true, check R_OK uncached.
	// This is roughly the same thing as mlocate does.	
	auto it = access_rx_cache->find(filename);
	if (it != access_rx_cache->end() && it->second) {
		return true;
	}

	return access(filename, R_OK) == 0;
#endif
	return true;
}

struct Trigram {
	uint32_t trgm;
	uint32_t num_docids;
	uint64_t offset;
};

class Corpus {
public:
	Corpus(int fd);
	~Corpus();
	const Trigram *find_trigram(uint32_t trgm) const;
	const unsigned char *get_compressed_posting_list(const Trigram *trigram) const;
	string_view get_compressed_filename_block(uint32_t docid) const;

private:
	const int fd;
	off_t len;
	const char *data;
	const uint64_t *filename_offsets;
	const Trigram *trgm_begin, *trgm_end;
};

Corpus::Corpus(int fd)
	: fd(fd)
{
	len = lseek(fd, 0, SEEK_END);
	if (len == -1) {
		perror("lseek");
		exit(1);
	}
	data = (char *)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, /*offset=*/0);
	if (data == MAP_FAILED) {
		perror("mmap");
		exit(1);
	}

	uint64_t num_trigrams = *(const uint64_t *)data;
	uint64_t filename_index_offset = *(const uint64_t *)(data + sizeof(uint64_t));
	filename_offsets = (const uint64_t *)(data + filename_index_offset);

	trgm_begin = (Trigram *)(data + sizeof(uint64_t) * 2);
	trgm_end = trgm_begin + num_trigrams;
}

Corpus::~Corpus()
{
	munmap((void *)data, len);
	close(fd);
}

const Trigram *Corpus::find_trigram(uint32_t trgm) const
{
	const Trigram *trgmptr = lower_bound(trgm_begin, trgm_end, trgm, [](const Trigram &trgm, uint32_t t) {
		return trgm.trgm < t;
	});
	if (trgmptr == trgm_end || trgmptr->trgm != trgm) {
		return nullptr;
	}
	return trgmptr;
}

const unsigned char *Corpus::get_compressed_posting_list(const Trigram *trgmptr) const
{
	return reinterpret_cast<const unsigned char *>(data + trgmptr->offset);
}

string_view Corpus::get_compressed_filename_block(uint32_t docid) const
{
	const char *compressed = (const char *)(data + filename_offsets[docid]);
	size_t compressed_size = filename_offsets[docid + 1] - filename_offsets[docid];  // Allowed we have a sentinel block at the end.
	return {compressed, compressed_size};
}

size_t scan_docid(const string &needle, uint32_t docid, const Corpus &corpus, unordered_map<string, bool> *access_rx_cache)
{
	string_view compressed = corpus.get_compressed_filename_block(docid);
	size_t matched = 0;

	string block;
	block.resize(ZSTD_getFrameContentSize(compressed.data(), compressed.size()) + 1);

	ZSTD_decompress(&block[0], block.size(), compressed.data(), compressed.size());
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

	//steady_clock::time_point start = steady_clock::now();
	if (access("/", R_OK | X_OK)) {
		// We can't find anything, no need to bother...
		return;
	}

	Corpus corpus(fd);

	vector<const Trigram *> trigrams;
	for (size_t i = 0; i < needle.size() - 2; ++i) {
		uint32_t trgm = read_trigram(needle, i);
		const Trigram *trgmptr = corpus.find_trigram(trgm);
		if (trgmptr == nullptr) {
			dprintf("trigram %06x isn't found, we abort the search\n", trgm);
			return;
		}
		trigrams.push_back(trgmptr);
	}
	sort(trigrams.begin(), trigrams.end());
	{
		auto last = unique(trigrams.begin(), trigrams.end());
		trigrams.erase(last, trigrams.end());
	}
	sort(trigrams.begin(), trigrams.end(), [&](const Trigram *a, const Trigram *b) {
		return a->num_docids < b->num_docids;
	});

	vector<uint32_t> in1, in2, out;
	for (const Trigram *trgmptr : trigrams) {
		//uint32_t trgm = trgmptr->trgm;
		size_t num = trgmptr->num_docids;
		const unsigned char *pldata = corpus.get_compressed_posting_list(trgmptr);
		if (in1.empty()) {
			in1.resize(num + 128);
			p4nd1dec128v32(const_cast<unsigned char *>(pldata), num, &in1[0]);
			in1.resize(num);
			dprintf("trigram '%c%c%c' decoded to %zu entries\n", trgm & 0xff, (trgm >> 8) & 0xff, (trgm >> 16) & 0xff, num);
		} else {
			if (num > in1.size() * 100) {
				dprintf("trigram '%c%c%c' has %zu entries, ignoring the rest (will weed out false positives later)\n",
					trgm & 0xff, (trgm >> 8) & 0xff, (trgm >> 16) & 0xff, num);
				break;
			}

			if (in2.size() < num + 128) {
				in2.resize(num + 128);
			}
			p4nd1dec128v32(const_cast<unsigned char *>(pldata), num, &in2[0]);

			out.clear();
			set_intersection(in1.begin(), in1.end(), in2.begin(), in2.begin() + num, back_inserter(out));
			swap(in1, out);
			dprintf("trigram '%c%c%c' decoded to %zu entries, %zu left\n", trgm & 0xff, (trgm >> 8) & 0xff, (trgm >> 16) & 0xff, num, in1.size());
			if (in1.empty()) {
				dprintf("no matches (intersection list is empty)\n");
				break;
			}
		}
	}
	steady_clock::time_point end = steady_clock::now();

	dprintf("Intersection took %.1f ms. Doing final verification and printing:\n",
		1e3 * duration<float>(end - start).count());

	unordered_map<string, bool> access_rx_cache;

	int matched = 0;
	for (uint32_t docid : in1) {
		matched += scan_docid(needle, docid, corpus, &access_rx_cache);
	}
	end = steady_clock::now();
	dprintf("Done in %.1f ms, found %d matches.\n",
		1e3 * duration<float>(end - start).count(), matched);
}

int main(int argc, char **argv)
{
	//do_search_file(argv[1], "all.trgm");
	do_search_file(argv[1], "/var/lib/mlocate/plocate.db");
}

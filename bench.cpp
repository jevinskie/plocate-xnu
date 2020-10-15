#include <chrono>
#include <fcntl.h>
#include <memory>
#include <stdio.h>
#include <string>
#include <unistd.h>

#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);

#include "db.h"
#include "io_uring_engine.h"
#include "turbopfor-encode.h"
#include "turbopfor.h"
#include "vp4.h"

using namespace std;
using namespace std::chrono;

bool use_debug = false;

int main(void)
{
	int fd = open("plocate.db", O_RDONLY);
	if (fd == -1) {
		perror("plocate.db");
		exit(1);
	}

	Header hdr;
	complete_pread(fd, &hdr, sizeof(hdr), /*offset=*/0);

	unique_ptr<Trigram[]> ht(new Trigram[hdr.hashtable_size + hdr.extra_ht_slots + 1]);
	complete_pread(fd, ht.get(), (hdr.hashtable_size + hdr.extra_ht_slots + 1) * sizeof(Trigram), hdr.hash_table_offset_bytes);

	size_t posting_list_bytes = 0, own_posting_list_bytes = 0, total_elements = 0, most_bytes_pl = 0;
	uint32_t longest_pl = 0;
	vector<pair<string, unsigned>> posting_lists;
	for (unsigned i = 0; i < hdr.hashtable_size + hdr.extra_ht_slots; ++i) {
		if (ht[i].num_docids == 0) {
			continue;
		}
		size_t len = ht[i + 1].offset - ht[i].offset;
		string str;
		str.resize(len);
		complete_pread(fd, &str[0], len, ht[i].offset);
		posting_lists.emplace_back(move(str), ht[i].num_docids);
		longest_pl = std::max(ht[i].num_docids, longest_pl);
		most_bytes_pl = std::max(len, most_bytes_pl);
		posting_list_bytes += len;
		total_elements += ht[i].num_docids;
	}
	ht.reset();
	fprintf(stderr, "Read %zu posting lists.\n", posting_lists.size());

	string encoded_pl;
	encoded_pl.resize(longest_pl * 2 + 16384);  // Lots of margin.

	size_t num_decode_errors = 0, num_encode_errors = 0;
	for (auto &[pl, num_docids] : posting_lists) {
		//fprintf(stderr, "%zu bytes, %u docids\n", pl.size(), num_docids);
		vector<uint32_t> out1, out2;
		out1.resize(num_docids + 128);
		out2.resize(num_docids + 128);
		unsigned char *pldata = reinterpret_cast<unsigned char *>(&pl[0]);
		p4nd1dec128v32(pldata, num_docids, &out1[0]);
		decode_pfor_delta1_128(pldata, num_docids, /*interleaved=*/true, &out2[0]);
		for (unsigned i = 0; i < num_docids; ++i) {
			if (out1[i] != out2[i]) {
				if (++num_decode_errors < 10) {
					fprintf(stderr, "Decode error:\n");
					for (unsigned j = 0; j < num_docids; ++j) {
						fprintf(stderr, "%3u: reference=%u ours=%u  (diff=%d)\n", j, out1[j], out2[j], out1[j] - out2[j]);
					}
				}
				break;
			}
		}

		// Test encoding, by encoding with out own implementation
		// and checking that decoding with the reference gives
		// the same result. We do not measure performance (we're slow).
		uint32_t deltas[128];
		unsigned char *ptr = reinterpret_cast<unsigned char *>(&encoded_pl[0]);
		ptr = write_baseval(out1[0], ptr);
		for (unsigned i = 1; i < num_docids; i += 128) {
			unsigned num_docids_this_block = std::min(num_docids - i, 128u);
			for (unsigned j = 0; j < num_docids_this_block; ++j) {
				deltas[j] = out1[i + j] - out1[i + j - 1] - 1;
			}
			bool interleaved = (num_docids_this_block == 128);
			ptr = encode_pfor_single_block<128>(deltas, num_docids_this_block, interleaved, ptr);
		}
		own_posting_list_bytes += ptr - reinterpret_cast<unsigned char *>(&encoded_pl[0]);

		pldata = reinterpret_cast<unsigned char *>(&encoded_pl[0]);
		p4nd1dec128v32(pldata, num_docids, &out2[0]);
		for (unsigned i = 0; i < num_docids; ++i) {
			if (out1[i] != out2[i]) {
				if (++num_encode_errors < 10) {
					fprintf(stderr, "Encode error:\n");
					for (unsigned j = 0; j < num_docids; ++j) {
						fprintf(stderr, "%3u: reference=%u ours=%u  (diff=%d)\n", j, out1[j], out2[j], out1[j] - out2[j]);
					}
				}
				break;
			}
		}
	}
	fprintf(stderr, "%zu/%zu posting lists had errors in decoding.\n", num_decode_errors, posting_lists.size());
	fprintf(stderr, "%zu/%zu posting lists had errors in encoding.\n", num_encode_errors, posting_lists.size());

	// Benchmark.
	vector<uint32_t> dummy;
	dummy.resize(longest_pl + 128);
	steady_clock::time_point start = steady_clock::now();
	for (auto &[pl, num_docids] : posting_lists) {
		unsigned char *pldata = reinterpret_cast<unsigned char *>(&pl[0]);
		p4nd1dec128v32(pldata, num_docids, &dummy[0]);
	}
	steady_clock::time_point end = steady_clock::now();
	double reference_sec = duration<double>(end - start).count();
	fprintf(stderr, "Decoding with reference implementation: %.1f ms\n", 1e3 * reference_sec);

	start = steady_clock::now();
	for (auto &[pl, num_docids] : posting_lists) {
		unsigned char *pldata = reinterpret_cast<unsigned char *>(&pl[0]);
		decode_pfor_delta1_128(pldata, num_docids, /*interleaved=*/true, &dummy[0]);
	}
	end = steady_clock::now();
	double own_sec = duration<double>(end - start).count();
	fprintf(stderr, "Decoding with own implementation: %.3f ms (%.2f%% speed)\n", 1e3 * own_sec, 100.0 * reference_sec / own_sec);
	fprintf(stderr, "Size with own implementation: %.1f MB (%.2f%% of reference, %+d bytes)\n", own_posting_list_bytes / 1048576.0, 100.0 * own_posting_list_bytes / posting_list_bytes, int(own_posting_list_bytes) - int(posting_list_bytes));

	// Three numbers giving rules of thumb for judging our own implementation:
	//
	// - Compressed speed is easy to compare to disk I/O, to see the relative importance
	// - Uncompressed speed is easy to compare to intersection speeds and memory bandwidth
	//   (also very roughly comparable to the benchmark numbers in the TurboPFor README)
	// - ns/element gives an absolute measure for plocate (e.g. if we can decompress at
	//   1 ns/element, a 10k-element posting list goes by in 0.01 ms, which is way beyond
	//   instantaneous in practice).
	fprintf(stderr, "%.1f MB/sec (compressed), %.1f MB/sec (uncompressed), %.1f ns/element\n", posting_list_bytes / own_sec / 1048576.0,
	        (total_elements * sizeof(uint32_t)) / own_sec / 1048576.0, 1e9 * own_sec / total_elements);
}

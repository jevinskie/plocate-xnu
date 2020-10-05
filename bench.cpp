#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <chrono>
#include <memory>

#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);

#include "turbopfor.h"
#include "vp4.h"
#include "db.h"
#include "io_uring_engine.h"

using namespace std;
using namespace std::chrono;

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
	}
	ht.reset();
	fprintf(stderr, "Read %zu posting lists.\n", posting_lists.size());

	size_t num_errors = 0;
	for (auto &[pl, num_docids] : posting_lists) {
		//fprintf(stderr, "%zu bytes, %u docids\n", pl.size(), num_docids);
		vector<uint32_t> out1, out2;
		out1.resize(num_docids + 128);
		out2.resize(num_docids + 128);
		unsigned char *pldata = reinterpret_cast<unsigned char *>(&pl[0]);
		p4nd1dec128v32(pldata, num_docids, &out1[0]);
		decode_pfor_delta1<128>(pldata, num_docids, /*interleaved=*/true, &out2[0]);
		for (unsigned i = 0; i < num_docids; ++i) {
			if (out1[i] != out2[i]) {
				if (++num_errors < 10) {
					for (unsigned j = 0; j < num_docids; ++j) {
						fprintf(stderr, "%3u: reference=%u ours=%u  (diff=%d)\n", j, out1[j], out2[j], out1[j] - out2[j]);
					}
				}
				break;
			}
		}
	}
	fprintf(stderr, "%zu/%zu posting lists had errors in decoding.\n", num_errors, posting_lists.size());

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
		decode_pfor_delta1<128>(pldata, num_docids, /*interleaved=*/true, &dummy[0]);
	}
	end = steady_clock::now();
	double own_sec = duration<double>(end - start).count();
	fprintf(stderr, "Decoding with own implementation: %.3f ms (%.2f%% speed)\n", 1e3 * own_sec, 100.0 * reference_sec / own_sec);
}

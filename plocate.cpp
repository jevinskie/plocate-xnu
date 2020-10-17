#include "access_rx_cache.h"
#include "db.h"
#include "dprintf.h"
#include "io_uring_engine.h"
#include "needle.h"
#include "parse_trigrams.h"
#include "serializer.h"
#include "turbopfor.h"
#include "unique_sort.h"

#include <algorithm>
#include <assert.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fcntl.h>
#include <functional>
#include <getopt.h>
#include <inttypes.h>
#include <iterator>
#include <limits>
#include <locale.h>
#include <memory>
#include <mutex>
#include <regex.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <zstd.h>

using namespace std;
using namespace std::chrono;

#define DEFAULT_DBPATH "/var/lib/mlocate/plocate.db"

const char *dbpath = DEFAULT_DBPATH;
bool ignore_case = false;
bool only_count = false;
bool print_nul = false;
bool use_debug = false;
bool flush_cache = false;
bool patterns_are_regex = false;
bool use_extended_regex = false;
bool match_basename = false;
int64_t limit_matches = numeric_limits<int64_t>::max();
int64_t limit_left = numeric_limits<int64_t>::max();

steady_clock::time_point start;
ZSTD_DDict *ddict = nullptr;

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
	const Header &get_hdr() const { return hdr; }

public:
	const int fd;
	IOUringEngine *const engine;

	Header hdr;
};

Corpus::Corpus(int fd, IOUringEngine *engine)
	: fd(fd), engine(engine)
{
	if (flush_cache) {
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
	if (hdr.version != 0 && hdr.version != 1) {
		fprintf(stderr, "plocate.db has version %u, expected 0 or 1; please rebuild it.\n", hdr.version);
		exit(1);
	}
	if (hdr.version == 0) {
		// These will be junk data.
		hdr.zstd_dictionary_offset_bytes = 0;
		hdr.zstd_dictionary_length_bytes = 0;
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

void scan_file_block(const vector<Needle> &needles, string_view compressed,
                     AccessRXCache *access_rx_cache, uint64_t seq, ResultReceiver *serializer,
                     atomic<uint64_t> *matched)
{
	unsigned long long uncompressed_len = ZSTD_getFrameContentSize(compressed.data(), compressed.size());
	if (uncompressed_len == ZSTD_CONTENTSIZE_UNKNOWN || uncompressed_len == ZSTD_CONTENTSIZE_ERROR) {
		fprintf(stderr, "ZSTD_getFrameContentSize() failed\n");
		exit(1);
	}

	string block;
	block.resize(uncompressed_len + 1);

	static thread_local ZSTD_DCtx *ctx = ZSTD_createDCtx();  // Reused across calls.
	size_t err;

	if (ddict != nullptr) {
		err = ZSTD_decompress_usingDDict(ctx, &block[0], block.size(), compressed.data(),
		                                 compressed.size(), ddict);
	} else {
		err = ZSTD_decompressDCtx(ctx, &block[0], block.size(), compressed.data(),
		                          compressed.size());
	}
	if (ZSTD_isError(err)) {
		fprintf(stderr, "ZSTD_decompress(): %s\n", ZSTD_getErrorName(err));
		exit(1);
	}
	block[block.size() - 1] = '\0';

	auto test_candidate = [&](const char *filename, uint64_t local_seq, uint64_t next_seq) {
		access_rx_cache->check_access(filename, /*allow_async=*/true, [matched, serializer, local_seq, next_seq, filename{ strdup(filename) }](bool ok) {
			if (ok) {
				++*matched;
				serializer->print(local_seq, next_seq - local_seq, filename);
			} else {
				serializer->print(local_seq, next_seq - local_seq, "");
			}
			free(filename);
		});
	};

	// We need to know the next sequence number before inserting into Serializer,
	// so always buffer one candidate.
	const char *pending_candidate = nullptr;

	uint64_t local_seq = seq << 32;
	for (const char *filename = block.data();
	     filename != block.data() + block.size();
	     filename += strlen(filename) + 1) {
		const char *haystack = filename;
		if (match_basename) {
			haystack = strrchr(filename, '/');
			if (haystack == nullptr) {
				haystack = filename;
			} else {
				++haystack;
			}
		}

		bool found = true;
		for (const Needle &needle : needles) {
			if (!matches(needle, haystack)) {
				found = false;
				break;
			}
		}
		if (found) {
			if (pending_candidate != nullptr) {
				test_candidate(pending_candidate, local_seq, local_seq + 1);
				++local_seq;
			}
			pending_candidate = filename;
		}
	}
	if (pending_candidate == nullptr) {
		serializer->print(seq << 32, 1ULL << 32, "");
	} else {
		test_candidate(pending_candidate, local_seq, (seq + 1) << 32);
	}
}

size_t scan_docids(const vector<Needle> &needles, const vector<uint32_t> &docids, const Corpus &corpus, IOUringEngine *engine)
{
	Serializer docids_in_order;
	AccessRXCache access_rx_cache(engine);
	atomic<uint64_t> matched{ 0 };
	for (size_t i = 0; i < docids.size(); ++i) {
		uint32_t docid = docids[i];
		corpus.get_compressed_filename_block(docid, [i, &matched, &needles, &access_rx_cache, &docids_in_order](string_view compressed) {
			scan_file_block(needles, compressed, &access_rx_cache, i, &docids_in_order, &matched);
		});
	}
	engine->finish();
	return matched;
}

struct WorkerThread {
	thread t;

	// We use a result queue instead of synchronizing Serializer,
	// since a lock on it becomes a huge choke point if there are
	// lots of threads.
	mutex result_mu;
	struct Result {
		uint64_t seq;
		uint64_t skip;
		string msg;
	};
	vector<Result> results;
};

class WorkerThreadReceiver : public ResultReceiver {
public:
	WorkerThreadReceiver(WorkerThread *wt)
		: wt(wt) {}

	void print(uint64_t seq, uint64_t skip, const string msg) override
	{
		lock_guard<mutex> lock(wt->result_mu);
		if (msg.empty() && !wt->results.empty() && wt->results.back().seq + wt->results.back().skip == seq) {
			wt->results.back().skip += skip;
		} else {
			wt->results.emplace_back(WorkerThread::Result{ seq, skip, move(msg) });
		}
	}

private:
	WorkerThread *wt;
};

void deliver_results(WorkerThread *wt, Serializer *serializer)
{
	vector<WorkerThread::Result> results;
	{
		lock_guard<mutex> lock(wt->result_mu);
		results = move(wt->results);
	}
	for (const WorkerThread::Result &result : results) {
		serializer->print(result.seq, result.skip, move(result.msg));
	}
}

// We do this sequentially, as it's faster than scattering
// a lot of I/O through io_uring and hoping the kernel will
// coalesce it plus readahead for us. Since we assume that
// we will primarily be CPU-bound, we'll be firing up one
// worker thread for each spare core (the last one will
// only be doing I/O). access() is still synchronous.
uint64_t scan_all_docids(const vector<Needle> &needles, int fd, const Corpus &corpus)
{
	{
		const Header &hdr = corpus.get_hdr();
		if (hdr.zstd_dictionary_length_bytes > 0) {
			string dictionary;
			dictionary.resize(hdr.zstd_dictionary_length_bytes);
			complete_pread(fd, &dictionary[0], hdr.zstd_dictionary_length_bytes, hdr.zstd_dictionary_offset_bytes);
			ddict = ZSTD_createDDict(dictionary.data(), dictionary.size());
		}
	}

	AccessRXCache access_rx_cache(nullptr);
	Serializer serializer;
	uint32_t num_blocks = corpus.get_num_filename_blocks();
	unique_ptr<uint64_t[]> offsets(new uint64_t[num_blocks + 1]);
	complete_pread(fd, offsets.get(), (num_blocks + 1) * sizeof(uint64_t), corpus.offset_for_block(0));
	atomic<uint64_t> matched{ 0 };

	mutex mu;
	condition_variable queue_added, queue_removed;
	deque<tuple<int, int, string>> work_queue;  // Under mu.
	bool done = false;  // Under mu.

	unsigned num_threads = max<int>(sysconf(_SC_NPROCESSORS_ONLN) - 1, 1);
	dprintf("Using %u worker threads for linear scan.\n", num_threads);
	unique_ptr<WorkerThread[]> threads(new WorkerThread[num_threads]);
	for (unsigned i = 0; i < num_threads; ++i) {
		threads[i].t = thread([&threads, &mu, &queue_added, &queue_removed, &work_queue, &done, &offsets, &needles, &access_rx_cache, &matched, i] {
			// regcomp() takes a lock on the regex, so each thread will need its own.
			const vector<Needle> *use_needles = &needles;
			vector<Needle> recompiled_needles;
			if (i != 0 && patterns_are_regex) {
				recompiled_needles = needles;
				for (Needle &needle : recompiled_needles) {
					needle.re = compile_regex(needle.str);
				}
				use_needles = &recompiled_needles;
			}

			WorkerThreadReceiver receiver(&threads[i]);
			for (;;) {
				uint32_t io_docid, last_docid;
				string compressed;

				{
					unique_lock<mutex> lock(mu);
					queue_added.wait(lock, [&work_queue, &done] { return !work_queue.empty() || done; });
					if (done && work_queue.empty()) {
						return;
					}
					tie(io_docid, last_docid, compressed) = move(work_queue.front());
					work_queue.pop_front();
					queue_removed.notify_all();
				}

				for (uint32_t docid = io_docid; docid < last_docid; ++docid) {
					size_t relative_offset = offsets[docid] - offsets[io_docid];
					size_t len = offsets[docid + 1] - offsets[docid];
					scan_file_block(*use_needles, { &compressed[relative_offset], len }, &access_rx_cache, docid, &receiver, &matched);
				}
			}
		});
	}

	string compressed;
	for (uint32_t io_docid = 0; io_docid < num_blocks; io_docid += 32) {
		uint32_t last_docid = std::min(io_docid + 32, num_blocks);
		size_t io_len = offsets[last_docid] - offsets[io_docid];
		if (compressed.size() < io_len) {
			compressed.resize(io_len);
		}
		complete_pread(fd, &compressed[0], io_len, offsets[io_docid]);

		{
			unique_lock<mutex> lock(mu);
			queue_removed.wait(lock, [&work_queue] { return work_queue.size() < 256; });  // Allow ~2MB of data queued up.
			work_queue.emplace_back(io_docid, last_docid, move(compressed));
			queue_added.notify_one();  // Avoid the thundering herd.
		}

		// Pick up some results, so that we are sure that we won't just overload.
		// (Seemingly, going through all of these causes slowness with many threads,
		// but taking only one is OK.)
		unsigned i = io_docid / 32;
		deliver_results(&threads[i % num_threads], &serializer);
	}
	{
		lock_guard<mutex> lock(mu);
		done = true;
		queue_added.notify_all();
	}
	for (unsigned i = 0; i < num_threads; ++i) {
		threads[i].t.join();
		deliver_results(&threads[i], &serializer);
	}
	return matched;
}

// Takes the given posting list, unions it into the parts of the trigram disjunction
// already read; if the list is complete, intersects with “cur_candidates”.
//
// Returns true if the search should be aborted (we are done).
bool new_posting_list_read(TrigramDisjunction *td, vector<uint32_t> decoded, vector<uint32_t> *cur_candidates, vector<uint32_t> *tmp)
{
	if (td->docids.empty()) {
		td->docids = move(decoded);
	} else {
		tmp->clear();
		set_union(decoded.begin(), decoded.end(), td->docids.begin(), td->docids.end(), back_inserter(*tmp));
		swap(*tmp, td->docids);
	}
	if (--td->remaining_trigrams_to_read > 0) {
		// Need to wait for more.
		if (ignore_case) {
			dprintf("  ... %u reads left in OR group %u (%zu docids in list)\n",
			        td->remaining_trigrams_to_read, td->index, td->docids.size());
		}
		return false;
	}
	if (cur_candidates->empty()) {
		if (ignore_case) {
			dprintf("  ... all reads done for OR group %u (%zu docids)\n",
			        td->index, td->docids.size());
		}
		*cur_candidates = move(td->docids);
	} else {
		tmp->clear();
		set_intersection(cur_candidates->begin(), cur_candidates->end(),
		                 td->docids.begin(), td->docids.end(),
		                 back_inserter(*tmp));
		swap(*cur_candidates, *tmp);
		if (ignore_case) {
			if (cur_candidates->empty()) {
				dprintf("  ... all reads done for OR group %u (%zu docids), intersected (none left, search is done)\n",
				        td->index, td->docids.size());
				return true;
			} else {
				dprintf("  ... all reads done for OR group %u (%zu docids), intersected (%zu left)\n",
				        td->index, td->docids.size(), cur_candidates->size());
			}
		}
	}
	return false;
}

void do_search_file(const vector<Needle> &needles, const char *filename)
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

	start = steady_clock::now();
	if (access("/", R_OK | X_OK)) {
		// We can't find anything, no need to bother...
		return;
	}

	IOUringEngine engine(/*slop_bytes=*/16);  // 16 slop bytes as described in turbopfor.h.
	Corpus corpus(fd, &engine);
	dprintf("Corpus init done after %.1f ms.\n", 1e3 * duration<float>(steady_clock::now() - start).count());

	vector<TrigramDisjunction> trigram_groups;
	if (patterns_are_regex) {
		// We could parse the regex to find trigrams that have to be there
		// (there are actually known algorithms to deal with disjunctions
		// and such, too), but for now, we just go brute force.
		// Using locate with regexes is pretty niche.
	} else {
		for (const Needle &needle : needles) {
			parse_trigrams(needle.str, ignore_case, &trigram_groups);
		}
	}

	unique_sort(
		&trigram_groups,
		[](const TrigramDisjunction &a, const TrigramDisjunction &b) { return a.trigram_alternatives < b.trigram_alternatives; },
		[](const TrigramDisjunction &a, const TrigramDisjunction &b) { return a.trigram_alternatives == b.trigram_alternatives; });

	// Give them names for debugging.
	unsigned td_index = 0;
	for (TrigramDisjunction &td : trigram_groups) {
		td.index = td_index++;
	}

	// Collect which trigrams we need to look up in the hash table.
	unordered_map<uint32_t, vector<TrigramDisjunction *>> trigrams_to_lookup;
	for (TrigramDisjunction &td : trigram_groups) {
		for (uint32_t trgm : td.trigram_alternatives) {
			trigrams_to_lookup[trgm].push_back(&td);
		}
	}
	if (trigrams_to_lookup.empty()) {
		// Too short for trigram matching. Apply brute force.
		// (We could have searched through all trigrams that matched
		// the pattern and done a union of them, but that's a lot of
		// work for fairly unclear gain.)
		uint64_t matched = scan_all_docids(needles, fd, corpus);
		dprintf("Done in %.1f ms, found %" PRId64 " matches.\n",
		        1e3 * duration<float>(steady_clock::now() - start).count(), matched);
		if (only_count) {
			printf("%" PRId64 "\n", matched);
		}
		return;
	}

	// Sneak in fetching the dictionary, if present. It's not necessarily clear
	// exactly where it would be cheapest to get it, but it needs to be present
	// before we can decode any of the posting lists. Most likely, it's
	// in the same filesystem block as the header anyway, so it should be
	// present in the cache.
	{
		const Header &hdr = corpus.get_hdr();
		if (hdr.zstd_dictionary_length_bytes > 0) {
			engine.submit_read(fd, hdr.zstd_dictionary_length_bytes, hdr.zstd_dictionary_offset_bytes, [](string_view s) {
				ddict = ZSTD_createDDict(s.data(), s.size());
				dprintf("Dictionary initialized after %.1f ms.\n", 1e3 * duration<float>(steady_clock::now() - start).count());
			});
		}
	}

	// Look them all up on disk.
	for (auto &[trgm, trigram_groups] : trigrams_to_lookup) {
		corpus.find_trigram(trgm, [trgm{ trgm }, trigram_groups{ &trigram_groups }](const Trigram *trgmptr, size_t len) {
			if (trgmptr == nullptr) {
				dprintf("trigram %s isn't found\n", print_trigram(trgm).c_str());
				for (TrigramDisjunction *td : *trigram_groups) {
					--td->remaining_trigrams_to_read;
					if (td->remaining_trigrams_to_read == 0 && td->read_trigrams.empty()) {
						dprintf("zero matches in %s, so we are done\n", print_td(*td).c_str());
						if (only_count) {
							printf("0\n");
						}
						exit(0);
					}
				}
				return;
			}
			for (TrigramDisjunction *td : *trigram_groups) {
				--td->remaining_trigrams_to_read;
				td->max_num_docids += trgmptr->num_docids;
				td->read_trigrams.emplace_back(*trgmptr, len);
			}
		});
	}
	engine.finish();
	dprintf("Hashtable lookups done after %.1f ms.\n", 1e3 * duration<float>(steady_clock::now() - start).count());

	for (TrigramDisjunction &td : trigram_groups) {
		// Reset for reads.
		td.remaining_trigrams_to_read = td.read_trigrams.size();

		if (ignore_case) {  // If case-sensitive, they'll all be pretty obvious single-entry groups.
			dprintf("OR group %u (max_num_docids=%u): %s\n", td.index, td.max_num_docids, print_td(td).c_str());
		}
	}

	// TODO: For case-insensitive (ie. more than one alternative in each),
	// prioritize the ones with fewer seeks?
	sort(trigram_groups.begin(), trigram_groups.end(),
	     [&](const TrigramDisjunction &a, const TrigramDisjunction &b) {
		     return a.max_num_docids < b.max_num_docids;
	     });

	unordered_map<uint32_t, vector<TrigramDisjunction *>> uses_trigram;
	for (TrigramDisjunction &td : trigram_groups) {
		for (uint32_t trgm : td.trigram_alternatives) {
			uses_trigram[trgm].push_back(&td);
		}
	}

	unordered_set<uint32_t> trigrams_submitted_read;
	vector<uint32_t> cur_candidates, tmp, decoded;
	bool done = false;
	for (TrigramDisjunction &td : trigram_groups) {
		if (!cur_candidates.empty() && td.max_num_docids > cur_candidates.size() * 100) {
			dprintf("%s has up to %u entries, ignoring the rest (will "
			        "weed out false positives later)\n",
			        print_td(td).c_str(), td.max_num_docids);
			break;
		}

		for (auto &[trgmptr, len] : td.read_trigrams) {
			if (trigrams_submitted_read.count(trgmptr.trgm) != 0) {
				continue;
			}
			trigrams_submitted_read.insert(trgmptr.trgm);
			// Only stay a certain amount ahead, so that we don't spend I/O
			// on reading the latter, large posting lists. We are unlikely
			// to need them anyway, even if they should come in first.
			if (engine.get_waiting_reads() >= 5) {
				engine.finish();
				if (done)
					break;
			}
			engine.submit_read(fd, len, trgmptr.offset, [trgmptr{ trgmptr }, len{ len }, &done, &cur_candidates, &tmp, &decoded, &uses_trigram](string_view s) {
				if (done)
					return;

				uint32_t trgm = trgmptr.trgm;
				const unsigned char *pldata = reinterpret_cast<const unsigned char *>(s.data());
				size_t num = trgmptr.num_docids;
				decoded.resize(num);
				decode_pfor_delta1_128(pldata, num, /*interleaved=*/true, &decoded[0]);

				assert(uses_trigram.count(trgm) != 0);
				bool was_empty = cur_candidates.empty();
				if (ignore_case) {
					dprintf("trigram %s (%zu bytes) decoded to %zu entries\n", print_trigram(trgm).c_str(), len, num);
				}

				for (TrigramDisjunction *td : uses_trigram[trgm]) {
					done |= new_posting_list_read(td, decoded, &cur_candidates, &tmp);
					if (done)
						break;
				}
				if (!ignore_case) {
					if (was_empty) {
						dprintf("trigram %s (%zu bytes) decoded to %zu entries\n", print_trigram(trgm).c_str(), len, num);
					} else if (cur_candidates.empty()) {
						dprintf("trigram %s (%zu bytes) decoded to %zu entries (none left, search is done)\n", print_trigram(trgm).c_str(), len, num);
					} else {
						dprintf("trigram %s (%zu bytes) decoded to %zu entries (%zu left)\n", print_trigram(trgm).c_str(), len, num, cur_candidates.size());
					}
				}
			});
		}
	}
	engine.finish();
	if (done) {
		return;
	}
	dprintf("Intersection done after %.1f ms. Doing final verification and printing:\n",
	        1e3 * duration<float>(steady_clock::now() - start).count());

	uint64_t matched = scan_docids(needles, cur_candidates, corpus, &engine);
	dprintf("Done in %.1f ms, found %" PRId64 " matches.\n",
	        1e3 * duration<float>(steady_clock::now() - start).count(), matched);

	if (only_count) {
		printf("%" PRId64 "\n", matched);
	}
}

void usage()
{
	printf(
		"Usage: plocate [OPTION]... PATTERN...\n"
		"\n"
		"  -b, --basename         search only the file name portion of path names\n"
		"  -c, --count            print number of matches instead of the matches\n"
		"  -d, --database DBPATH  search for files in DBPATH\n"
		"                         (default is " DEFAULT_DBPATH ")\n"
		"  -i, --ignore-case      search case-insensitively\n"
		"  -l, --limit LIMIT      stop after LIMIT matches\n"
		"  -0, --null             delimit matches by NUL instead of newline\n"
		"  -r, --regexp           interpret patterns as basic regexps (slow)\n"
		"      --regex            interpret patterns as extended regexps (slow)\n"
		"  -w, --wholename        search the entire path name (default; see -b)\n"
		"      --help             print this help\n"
		"      --version          print version information\n");
}

void version()
{
	printf("plocate %s\n", PLOCATE_VERSION);
	printf("Copyright 2020 Steinar H. Gunderson\n");
	printf("License GPLv2+: GNU GPL version 2 or later <https://gnu.org/licenses/gpl.html>.\n");
	printf("This is free software: you are free to change and redistribute it.\n");
	printf("There is NO WARRANTY, to the extent permitted by law.\n");
	exit(0);
}

int main(int argc, char **argv)
{
	constexpr int EXTENDED_REGEX = 1000;
	constexpr int FLUSH_CACHE = 1001;
	static const struct option long_options[] = {
		{ "help", no_argument, 0, 'h' },
		{ "count", no_argument, 0, 'c' },
		{ "basename", no_argument, 0, 'b' },
		{ "database", required_argument, 0, 'd' },
		{ "ignore-case", no_argument, 0, 'i' },
		{ "limit", required_argument, 0, 'l' },
		{ "null", no_argument, 0, '0' },
		{ "version", no_argument, 0, 'V' },
		{ "regexp", no_argument, 0, 'r' },
		{ "regex", no_argument, 0, EXTENDED_REGEX },
		{ "wholename", no_argument, 0, 'w' },
		{ "debug", no_argument, 0, 'D' },  // Not documented.
		// Enable to test cold-cache behavior (except for access()). Not documented.
		{ "flush-cache", no_argument, 0, FLUSH_CACHE },
		{ 0, 0, 0, 0 }
	};

	setlocale(LC_ALL, "");
	for (;;) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "bcd:hil:n:0rwVD", long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch (c) {
		case 'b':
			match_basename = true;
			break;
		case 'c':
			only_count = true;
			break;
		case 'd':
			dbpath = strdup(optarg);
			break;
		case 'h':
			usage();
			exit(0);
		case 'i':
			ignore_case = true;
			break;
		case 'l':
		case 'n':
			limit_matches = limit_left = atoll(optarg);
			if (limit_matches <= 0) {
				fprintf(stderr, "Error: limit must be a strictly positive number.\n");
				exit(1);
			}
			break;
		case '0':
			print_nul = true;
			break;
		case 'r':
			patterns_are_regex = true;
			break;
		case EXTENDED_REGEX:
			patterns_are_regex = true;
			use_extended_regex = true;
			break;
		case 'w':
			match_basename = false;  // No-op unless -b is given first.
			break;
		case 'D':
			use_debug = true;
			break;
		case FLUSH_CACHE:
			flush_cache = true;
			break;
		case 'V':
			version();
			break;
		default:
			exit(1);
		}
	}

	if (use_debug || flush_cache) {
		// Debug information would leak information about which files exist,
		// so drop setgid before we open the file; one would either need to run
		// as root, or use a locally-built file. Doing the same thing for
		// flush_cache is mostly paranoia, in an attempt to prevent random users
		// from making plocate slow for everyone else.
		if (setgid(getgid()) != 0) {
			perror("setgid");
			exit(EXIT_FAILURE);
		}
	}

	vector<Needle> needles;
	for (int i = optind; i < argc; ++i) {
		Needle needle;
		needle.str = argv[i];

		// See if there are any wildcard characters, which indicates we should treat it
		// as an (anchored) glob.
		bool any_wildcard = false;
		for (size_t i = 0; i < needle.str.size(); i += read_unigram(needle.str, i).second) {
			if (read_unigram(needle.str, i).first == WILDCARD_UNIGRAM) {
				any_wildcard = true;
				break;
			}
		}

		if (patterns_are_regex) {
			needle.type = Needle::REGEX;
			needle.re = compile_regex(needle.str);
		} else if (any_wildcard) {
			needle.type = Needle::GLOB;
		} else if (ignore_case) {
			// strcasestr() doesn't handle locales correctly (even though LSB
			// claims it should), but somehow, fnmatch() does, and it's about
			// the same speed as using a regex.
			needle.type = Needle::GLOB;
			needle.str = "*" + needle.str + "*";
		} else {
			needle.type = Needle::STRSTR;
			needle.str = unescape_glob_to_plain_string(needle.str);
		}
		needles.push_back(move(needle));
	}
	if (needles.empty()) {
		fprintf(stderr, "plocate: no pattern to search for specified\n");
		exit(0);
	}
	do_search_file(needles, dbpath);
}

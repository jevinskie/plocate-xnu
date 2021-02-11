#include "database-builder.h"

#include "dprintf.h"
#include "turbopfor-encode.h"

#include <algorithm>
#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <string_view>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <zdict.h>
#include <zstd.h>

#define P4NENC_BOUND(n) ((n + 127) / 128 + (n + 32) * sizeof(uint32_t))

#define NUM_TRIGRAMS 16777216

using namespace std;
using namespace std::chrono;

constexpr unsigned num_overflow_slots = 16;

string zstd_compress(const string &src, ZSTD_CDict *cdict, string *tempbuf);

static inline uint32_t read_unigram(const string_view s, size_t idx)
{
	if (idx < s.size()) {
		return (unsigned char)s[idx];
	} else {
		return 0;
	}
}

static inline uint32_t read_trigram(const string_view s, size_t start)
{
	return read_unigram(s, start) |
		(read_unigram(s, start + 1) << 8) |
		(read_unigram(s, start + 2) << 16);
}

class PostingListBuilder {
public:
	inline void add_docid(uint32_t docid);
	inline void add_first_docid(uint32_t docid);
	void finish();

	string encoded;
	size_t num_docids = 0;

private:
	void write_header(uint32_t docid);
	void append_block();

	vector<uint32_t> pending_deltas;

	uint32_t last_docid = -1;
};

void PostingListBuilder::add_docid(uint32_t docid)
{
	// Deduplicate against the last inserted value, if any.
	if (docid == last_docid) {
		return;
	}

	assert(num_docids != 0);

	pending_deltas.push_back(docid - last_docid - 1);
	last_docid = docid;
	if (pending_deltas.size() == 128) {
		append_block();
		pending_deltas.clear();
	}
	++num_docids;
}

void PostingListBuilder::add_first_docid(uint32_t docid)
{
	write_header(docid);
	++num_docids;
	last_docid = docid;
}

void PostingListBuilder::finish()
{
	if (pending_deltas.empty()) {
		return;
	}

	assert(!encoded.empty());  // write_header() should already have run.

	// No interleaving for partial blocks.
	unsigned char buf[P4NENC_BOUND(128)];
	unsigned char *end = encode_pfor_single_block<128>(pending_deltas.data(), pending_deltas.size(), /*interleaved=*/false, buf);
	encoded.append(reinterpret_cast<char *>(buf), reinterpret_cast<char *>(end));
}

void PostingListBuilder::append_block()
{
	unsigned char buf[P4NENC_BOUND(128)];
	assert(pending_deltas.size() == 128);
	unsigned char *end = encode_pfor_single_block<128>(pending_deltas.data(), 128, /*interleaved=*/true, buf);
	encoded.append(reinterpret_cast<char *>(buf), reinterpret_cast<char *>(end));
}

void PostingListBuilder::write_header(uint32_t docid)
{
	unsigned char buf[P4NENC_BOUND(1)];
	unsigned char *end = write_baseval(docid, buf);
	encoded.append(reinterpret_cast<char *>(buf), end - buf);
}

void DictionaryBuilder::add_file(string filename, dir_time)
{
	if (keep_current_block) {  // Only bother saving the filenames if we're actually keeping the block.
		if (!current_block.empty()) {
			current_block.push_back('\0');
		}
		current_block += filename;
	}
	if (++num_files_in_block == block_size) {
		flush_block();
	}
}

void DictionaryBuilder::flush_block()
{
	if (keep_current_block) {
		if (slot_for_current_block == -1) {
			lengths.push_back(current_block.size());
			sampled_blocks.push_back(move(current_block));
		} else {
			lengths[slot_for_current_block] = current_block.size();
			sampled_blocks[slot_for_current_block] = move(current_block);
		}
	}
	current_block.clear();
	num_files_in_block = 0;
	++block_num;

	if (block_num < blocks_to_keep) {
		keep_current_block = true;
		slot_for_current_block = -1;
	} else {
		// Keep every block with equal probability (reservoir sampling).
		uint64_t idx = uniform_int_distribution<uint64_t>(0, block_num)(reservoir_rand);
		keep_current_block = (idx < blocks_to_keep);
		slot_for_current_block = idx;
	}
}

string DictionaryBuilder::train(size_t buf_size)
{
	string dictionary_buf;
	sort(sampled_blocks.begin(), sampled_blocks.end());  // Seemingly important for decompression speed.
	for (const string &block : sampled_blocks) {
		dictionary_buf += block;
	}

	string buf;
	buf.resize(buf_size);
	size_t ret = ZDICT_trainFromBuffer(&buf[0], buf_size, dictionary_buf.data(), lengths.data(), lengths.size());
	if (ZDICT_isError(ret)) {
		return "";
	}
	dprintf("Sampled %zu bytes in %zu blocks, built a dictionary of size %zu\n", dictionary_buf.size(), lengths.size(), ret);
	buf.resize(ret);

	sampled_blocks.clear();
	lengths.clear();

	return buf;
}

class EncodingCorpus : public DatabaseReceiver {
public:
	EncodingCorpus(FILE *outfp, size_t block_size, ZSTD_CDict *cdict, bool store_dir_times);
	~EncodingCorpus();

	void add_file(std::string filename, dir_time dt) override;
	void flush_block() override;
	void finish() override;

	std::vector<uint64_t> filename_blocks;
	size_t num_files = 0, num_files_in_block = 0, num_blocks = 0;
	bool seen_trigram(uint32_t trgm)
	{
		return invindex[trgm] != nullptr;
	}
	size_t num_files_seen() const override { return num_files; }
	PostingListBuilder &get_pl_builder(uint32_t trgm)
	{
		return *invindex[trgm];
	}

	void add_docid(uint32_t trgm, uint32_t docid)
	{
		if (invindex[trgm] == nullptr) {
			invindex[trgm] = new PostingListBuilder;
			invindex[trgm]->add_first_docid(docid);
		} else {
			invindex[trgm]->add_docid(docid);
		}
	}

	size_t num_trigrams() const;
	std::string get_compressed_dir_times();

private:
	void compress_dir_times(size_t allowed_slop);

	std::unique_ptr<PostingListBuilder *[]> invindex;
	FILE *outfp;
	std::string current_block;
	std::string tempbuf;
	const size_t block_size;
	const bool store_dir_times;
	ZSTD_CDict *cdict;

	ZSTD_CStream *dir_time_ctx = nullptr;
	std::string dir_times;  // Buffer of still-uncompressed data.
	std::string dir_times_compressed;
};


EncodingCorpus::EncodingCorpus(FILE *outfp, size_t block_size, ZSTD_CDict *cdict, bool store_dir_times)
	: invindex(new PostingListBuilder *[NUM_TRIGRAMS]), outfp(outfp), block_size(block_size), store_dir_times(store_dir_times), cdict(cdict)
{
	fill(invindex.get(), invindex.get() + NUM_TRIGRAMS, nullptr);
	if (store_dir_times) {
		dir_time_ctx = ZSTD_createCStream();
		ZSTD_initCStream(dir_time_ctx, /*level=*/6);
	}
}

EncodingCorpus::~EncodingCorpus()
{
	for (unsigned i = 0; i < NUM_TRIGRAMS; ++i) {
		delete invindex[i];
	}
}

void EncodingCorpus::add_file(string filename, dir_time dt)
{
	++num_files;
	if (!current_block.empty()) {
		current_block.push_back('\0');
	}
	current_block += filename;
	if (++num_files_in_block == block_size) {
		flush_block();
	}

	if (store_dir_times) {
		if (dt.sec == -1) {
			// Not a directory.
			dir_times.push_back('\0');
		} else {
			dir_times.push_back('\1');
			dir_times.append(reinterpret_cast<char *>(&dt.sec), sizeof(dt.sec));
			dir_times.append(reinterpret_cast<char *>(&dt.nsec), sizeof(dt.nsec));
		}
		compress_dir_times(/*allowed_slop=*/4096);
	}
}

void EncodingCorpus::compress_dir_times(size_t allowed_slop)
{
	while (dir_times.size() >= allowed_slop) {
		size_t old_size = dir_times_compressed.size();
		dir_times_compressed.resize(old_size + 4096);

		ZSTD_outBuffer outbuf;
		outbuf.dst = dir_times_compressed.data() + old_size;
		outbuf.size = 4096;
		outbuf.pos = 0;

		ZSTD_inBuffer inbuf;
		inbuf.src = dir_times.data();
		inbuf.size = dir_times.size();
		inbuf.pos = 0;

		int ret = ZSTD_compressStream(dir_time_ctx, &outbuf, &inbuf);
		if (ret < 0) {
			fprintf(stderr, "ZSTD_compressStream() failed\n");
			exit(1);
		}

		dir_times_compressed.resize(old_size + outbuf.pos);
		dir_times.erase(dir_times.begin(), dir_times.begin() + inbuf.pos);

		if (outbuf.pos == 0 && inbuf.pos == 0) {
			// Nothing happened (not enough data?), try again later.
			return;
		}
	}
}

void EncodingCorpus::flush_block()
{
	if (current_block.empty()) {
		return;
	}

	uint32_t docid = num_blocks;

	// Create trigrams.
	const char *ptr = current_block.c_str();
	while (ptr < current_block.c_str() + current_block.size()) {
		string_view s(ptr);
		if (s.size() >= 3) {
			for (size_t j = 0; j < s.size() - 2; ++j) {
				uint32_t trgm = read_trigram(s, j);
				add_docid(trgm, docid);
			}
		}
		ptr += s.size() + 1;
	}

	// Compress and add the filename block.
	filename_blocks.push_back(ftell(outfp));
	string compressed = zstd_compress(current_block, cdict, &tempbuf);
	if (fwrite(compressed.data(), compressed.size(), 1, outfp) != 1) {
		perror("fwrite()");
		exit(1);
	}

	current_block.clear();
	num_files_in_block = 0;
	++num_blocks;
}

void EncodingCorpus::finish()
{
	flush_block();
}

size_t EncodingCorpus::num_trigrams() const
{
	size_t num = 0;
	for (unsigned trgm = 0; trgm < NUM_TRIGRAMS; ++trgm) {
		if (invindex[trgm] != nullptr) {
			++num;
		}
	}
	return num;
}

string EncodingCorpus::get_compressed_dir_times()
{
	if (!store_dir_times) {
		return "";
	}
	compress_dir_times(/*allowed_slop=*/0);
	assert(dir_times.empty());

	for (;;) {
		size_t old_size = dir_times_compressed.size();
		dir_times_compressed.resize(old_size + 4096);

		ZSTD_outBuffer outbuf;
		outbuf.dst = dir_times_compressed.data() + old_size;
		outbuf.size = 4096;
		outbuf.pos = 0;

		int ret = ZSTD_endStream(dir_time_ctx, &outbuf);
		if (ret < 0) {
			fprintf(stderr, "ZSTD_compressStream() failed\n");
			exit(1);
		}

		dir_times_compressed.resize(old_size + outbuf.pos);

		if (ret == 0) {
			// All done.
			break;
		}
	}

	return dir_times_compressed;
}

string zstd_compress(const string &src, ZSTD_CDict *cdict, string *tempbuf)
{
	static ZSTD_CCtx *ctx = nullptr;
	if (ctx == nullptr) {
		ctx = ZSTD_createCCtx();
	}

	size_t max_size = ZSTD_compressBound(src.size());
	if (tempbuf->size() < max_size) {
		tempbuf->resize(max_size);
	}
	size_t size;
	if (cdict == nullptr) {
		size = ZSTD_compressCCtx(ctx, &(*tempbuf)[0], max_size, src.data(), src.size(), /*level=*/6);
	} else {
		size = ZSTD_compress_usingCDict(ctx, &(*tempbuf)[0], max_size, src.data(), src.size(), cdict);
	}
	return string(tempbuf->data(), size);
}

bool is_prime(uint32_t x)
{
	if ((x % 2) == 0 || (x % 3) == 0) {
		return false;
	}
	uint32_t limit = ceil(sqrt(x));
	for (uint32_t factor = 5; factor <= limit; ++factor) {
		if ((x % factor) == 0) {
			return false;
		}
	}
	return true;
}

uint32_t next_prime(uint32_t x)
{
	if ((x % 2) == 0) {
		++x;
	}
	while (!is_prime(x)) {
		x += 2;
	}
	return x;
}

unique_ptr<Trigram[]> create_hashtable(EncodingCorpus &corpus, const vector<uint32_t> &all_trigrams, uint32_t ht_size, uint32_t num_overflow_slots)
{
	unique_ptr<Trigram[]> ht(new Trigram[ht_size + num_overflow_slots + 1]);  // 1 for the sentinel element at the end.
	for (unsigned i = 0; i < ht_size + num_overflow_slots + 1; ++i) {
		ht[i].trgm = uint32_t(-1);
		ht[i].num_docids = 0;
		ht[i].offset = 0;
	}
	for (uint32_t trgm : all_trigrams) {
		// We don't know offset yet, so set it to zero.
		Trigram to_insert{ trgm, uint32_t(corpus.get_pl_builder(trgm).num_docids), 0 };

		uint32_t bucket = hash_trigram(trgm, ht_size);
		unsigned distance = 0;
		while (ht[bucket].num_docids != 0) {
			// Robin Hood hashing; reduces the longest distance by a lot.
			unsigned other_distance = bucket - hash_trigram(ht[bucket].trgm, ht_size);
			if (distance > other_distance) {
				swap(to_insert, ht[bucket]);
				distance = other_distance;
			}

			++bucket, ++distance;
			if (distance > num_overflow_slots) {
				return nullptr;
			}
		}
		ht[bucket] = to_insert;
	}
	return ht;
}

DatabaseBuilder::DatabaseBuilder(const char *outfile, gid_t owner, int block_size, string dictionary, bool check_visibility)
	: outfile(outfile), block_size(block_size)
{
	umask(0027);

	string path = outfile;
	path.resize(path.find_last_of('/') + 1);
	if (path.empty()) {
		path = ".";
	}
#ifdef O_TMPFILE
	int fd = open(path.c_str(), O_WRONLY | O_TMPFILE, 0640);
	if (fd == -1) {
		perror(path.c_str());
		exit(1);
	}
#else
	temp_filename = string(outfile) + ".XXXXXX";
	int fd = mkstemp(&temp_filename[0]);
	if (fd == -1) {
		perror(temp_filename.c_str());
		exit(1);
	}
	if (fchmod(fd, 0640) == -1) {
		perror("fchmod");
		exit(1);
	}
#endif

	if (owner != (gid_t)-1) {
		if (fchown(fd, (uid_t)-1, owner) == -1) {
			perror("fchown");
			exit(1);
		}
	}

	outfp = fdopen(fd, "wb");
	if (outfp == nullptr) {
		perror(outfile);
		exit(1);
	}

	// Write the header.
	memcpy(hdr.magic, "\0plocate", 8);
	hdr.version = -1;  // Mark as broken.
	hdr.hashtable_size = 0;  // Not known yet.
	hdr.extra_ht_slots = num_overflow_slots;
	hdr.num_docids = 0;
	hdr.hash_table_offset_bytes = -1;  // We don't know these offsets yet.
	hdr.max_version = 2;
	hdr.filename_index_offset_bytes = -1;
	hdr.zstd_dictionary_length_bytes = -1;
	hdr.check_visibility = check_visibility;
	fwrite(&hdr, sizeof(hdr), 1, outfp);

	if (dictionary.empty()) {
		hdr.zstd_dictionary_offset_bytes = 0;
		hdr.zstd_dictionary_length_bytes = 0;
	} else {
		hdr.zstd_dictionary_offset_bytes = ftell(outfp);
		fwrite(dictionary.data(), dictionary.size(), 1, outfp);
		hdr.zstd_dictionary_length_bytes = dictionary.size();
		cdict = ZSTD_createCDict(dictionary.data(), dictionary.size(), /*level=*/6);
	}

	hdr.directory_data_length_bytes = 0;
	hdr.directory_data_offset_bytes = 0;
	hdr.next_zstd_dictionary_length_bytes = 0;
	hdr.next_zstd_dictionary_offset_bytes = 0;
	hdr.conf_block_length_bytes = 0;
	hdr.conf_block_offset_bytes = 0;
}

DatabaseReceiver *DatabaseBuilder::start_corpus(bool store_dir_times)
{
	corpus_start = steady_clock::now();
	corpus = new EncodingCorpus(outfp, block_size, cdict, store_dir_times);
	return corpus;
}

void DatabaseBuilder::set_next_dictionary(std::string next_dictionary)
{
	this->next_dictionary = move(next_dictionary);
}

void DatabaseBuilder::set_conf_block(std::string conf_block)
{
	this->conf_block = move(conf_block);
}

void DatabaseBuilder::finish_corpus()
{
	corpus->finish();
	hdr.num_docids = corpus->filename_blocks.size();

	// Stick an empty block at the end as sentinel.
	corpus->filename_blocks.push_back(ftell(outfp));
	const size_t bytes_for_filenames = corpus->filename_blocks.back() - corpus->filename_blocks.front();

	// Write the offsets to the filenames.
	hdr.filename_index_offset_bytes = ftell(outfp);
	const size_t bytes_for_filename_index = corpus->filename_blocks.size() * sizeof(uint64_t);
	fwrite(corpus->filename_blocks.data(), corpus->filename_blocks.size(), sizeof(uint64_t), outfp);
	corpus->filename_blocks.clear();
	corpus->filename_blocks.shrink_to_fit();

	// Finish up encoding the posting lists.
	size_t trigrams = 0, longest_posting_list = 0;
	size_t bytes_for_posting_lists = 0;
	for (unsigned trgm = 0; trgm < NUM_TRIGRAMS; ++trgm) {
		if (!corpus->seen_trigram(trgm))
			continue;
		PostingListBuilder &pl_builder = corpus->get_pl_builder(trgm);
		pl_builder.finish();
		longest_posting_list = max(longest_posting_list, pl_builder.num_docids);
		trigrams += pl_builder.num_docids;
		bytes_for_posting_lists += pl_builder.encoded.size();
	}
	size_t num_trigrams = corpus->num_trigrams();
	dprintf("%zu files, %zu different trigrams, %zu entries, avg len %.2f, longest %zu\n",
	        corpus->num_files, num_trigrams, trigrams, double(trigrams) / num_trigrams, longest_posting_list);
	dprintf("%zu bytes used for posting lists (%.2f bits/entry)\n", bytes_for_posting_lists, 8 * bytes_for_posting_lists / double(trigrams));

	dprintf("Building posting lists took %.1f ms.\n\n", 1e3 * duration<float>(steady_clock::now() - corpus_start).count());

	// Find the used trigrams.
	vector<uint32_t> all_trigrams;
	for (unsigned trgm = 0; trgm < NUM_TRIGRAMS; ++trgm) {
		if (corpus->seen_trigram(trgm)) {
			all_trigrams.push_back(trgm);
		}
	}

	// Create the hash table.
	unique_ptr<Trigram[]> hashtable;
	uint32_t ht_size = next_prime(all_trigrams.size());
	for (;;) {
		hashtable = create_hashtable(*corpus, all_trigrams, ht_size, num_overflow_slots);
		if (hashtable == nullptr) {
			dprintf("Failed creating hash table of size %u, increasing by 5%% and trying again.\n", ht_size);
			ht_size = next_prime(ht_size * 1.05);
		} else {
			dprintf("Created hash table of size %u.\n\n", ht_size);
			break;
		}
	}

	// Find the offsets for each posting list.
	size_t bytes_for_hashtable = (ht_size + num_overflow_slots + 1) * sizeof(Trigram);
	uint64_t offset = ftell(outfp) + bytes_for_hashtable;
	for (unsigned i = 0; i < ht_size + num_overflow_slots + 1; ++i) {
		hashtable[i].offset = offset;  // Needs to be there even for empty slots.
		if (hashtable[i].num_docids == 0) {
			continue;
		}

		const string &encoded = corpus->get_pl_builder(hashtable[i].trgm).encoded;
		offset += encoded.size();
	}

	// Write the hash table.
	hdr.hash_table_offset_bytes = ftell(outfp);
	hdr.hashtable_size = ht_size;
	fwrite(hashtable.get(), ht_size + num_overflow_slots + 1, sizeof(Trigram), outfp);

	// Write the actual posting lists.
	for (unsigned i = 0; i < ht_size + num_overflow_slots + 1; ++i) {
		if (hashtable[i].num_docids == 0) {
			continue;
		}
		const string &encoded = corpus->get_pl_builder(hashtable[i].trgm).encoded;
		fwrite(encoded.data(), encoded.size(), 1, outfp);
	}

	// Finally, write the directory times (for updatedb).
	string compressed_dir_times = corpus->get_compressed_dir_times();
	size_t bytes_for_compressed_dir_times = 0;
	if (!compressed_dir_times.empty()) {
		hdr.directory_data_offset_bytes = ftell(outfp);
		hdr.directory_data_length_bytes = compressed_dir_times.size();
		fwrite(compressed_dir_times.data(), compressed_dir_times.size(), 1, outfp);
		bytes_for_compressed_dir_times = compressed_dir_times.size();
		compressed_dir_times.clear();
	}

	// Write the recommended dictionary for next update.
	if (!next_dictionary.empty()) {
		hdr.next_zstd_dictionary_offset_bytes = ftell(outfp);
		hdr.next_zstd_dictionary_length_bytes = next_dictionary.size();
		fwrite(next_dictionary.data(), next_dictionary.size(), 1, outfp);
	}

	// And the configuration block.
	if (!conf_block.empty()) {
		hdr.conf_block_offset_bytes = ftell(outfp);
		hdr.conf_block_length_bytes = conf_block.size();
		fwrite(conf_block.data(), conf_block.size(), 1, outfp);
	}

	// Rewind, and write the updated header.
	hdr.version = 1;
	fseek(outfp, 0, SEEK_SET);
	fwrite(&hdr, sizeof(hdr), 1, outfp);

#ifdef O_TMPFILE
	// Give the file a proper name, making it visible in the file system.
	// TODO: It would be nice to be able to do this atomically, like with rename.
	unlink(outfile.c_str());
	char procpath[256];
	snprintf(procpath, sizeof(procpath), "/proc/self/fd/%d", fileno(outfp));
	if (linkat(AT_FDCWD, procpath, AT_FDCWD, outfile.c_str(), AT_SYMLINK_FOLLOW) == -1) {
		perror("linkat");
		exit(1);
	}
#else
	if (rename(temp_filename.c_str(), outfile.c_str()) == -1) {
		perror("rename");
		exit(1);
	}
#endif

	fclose(outfp);

	size_t total_bytes = (bytes_for_hashtable + bytes_for_posting_lists + bytes_for_filename_index + bytes_for_filenames + bytes_for_compressed_dir_times);

	dprintf("Block size:     %7d files\n", block_size);
	dprintf("Dictionary:     %'7.1f MB\n", hdr.zstd_dictionary_length_bytes / 1048576.0);
	dprintf("Hash table:     %'7.1f MB\n", bytes_for_hashtable / 1048576.0);
	dprintf("Posting lists:  %'7.1f MB\n", bytes_for_posting_lists / 1048576.0);
	dprintf("Filename index: %'7.1f MB\n", bytes_for_filename_index / 1048576.0);
	dprintf("Filenames:      %'7.1f MB\n", bytes_for_filenames / 1048576.0);
	if (bytes_for_compressed_dir_times != 0) {
		dprintf("Modify times:   %'7.1f MB\n", bytes_for_compressed_dir_times / 1048576.0);
	}
	dprintf("Total:          %'7.1f MB\n", total_bytes / 1048576.0);
	dprintf("\n");
}

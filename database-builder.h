#ifndef _DATABASE_BUILDER_H
#define _DATABASE_BUILDER_H 1

#include "db.h"

#include <chrono>
#include <fcntl.h>
#include <memory>
#include <random>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>
#include <zstd.h>

class PostingListBuilder;

// {0,0} means unknown or so current that it should never match.
// {-1,0} means it's not a directory.
struct dir_time {
	int64_t sec;
	int32_t nsec;

	bool operator<(const dir_time &other) const
	{
		if (sec != other.sec)
			return sec < other.sec;
		return nsec < other.nsec;
	}
	bool operator>=(const dir_time &other) const
	{
		return !(other < *this);
	}
};
constexpr dir_time unknown_dir_time{ 0, 0 };
constexpr dir_time not_a_dir{ -1, 0 };

class DatabaseReceiver {
public:
	virtual ~DatabaseReceiver() = default;
	virtual void add_file(std::string filename, dir_time dt) = 0;
	virtual void flush_block() = 0;
	virtual void finish() { flush_block(); }
};

class DictionaryBuilder : public DatabaseReceiver {
public:
	DictionaryBuilder(size_t blocks_to_keep, size_t block_size)
		: blocks_to_keep(blocks_to_keep), block_size(block_size) {}
	void add_file(std::string filename, dir_time dt) override;
	void flush_block() override;
	std::string train(size_t buf_size);

private:
	const size_t blocks_to_keep, block_size;
	std::string current_block;
	uint64_t block_num = 0;
	size_t num_files_in_block = 0;

	std::mt19937 reservoir_rand{ 1234 };  // Fixed seed for reproducibility.
	bool keep_current_block = true;
	int64_t slot_for_current_block = -1;

	std::vector<std::string> sampled_blocks;
	std::vector<size_t> lengths;
};

class Corpus : public DatabaseReceiver {
public:
	Corpus(FILE *outfp, size_t block_size, ZSTD_CDict *cdict, bool store_dir_times);
	~Corpus();

	void add_file(std::string filename, dir_time dt) override;
	void flush_block() override;
	void finish() override;

	std::vector<uint64_t> filename_blocks;
	size_t num_files = 0, num_files_in_block = 0, num_blocks = 0;
	bool seen_trigram(uint32_t trgm)
	{
		return invindex[trgm] != nullptr;
	}
	PostingListBuilder &get_pl_builder(uint32_t trgm);
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

class DatabaseBuilder {
public:
	DatabaseBuilder(const char *outfile, gid_t owner, int block_size, std::string dictionary, bool check_visibility);
	Corpus *start_corpus(bool store_dir_times);
	void set_next_dictionary(std::string next_dictionary);
	void set_conf_block(std::string conf_block);
	void finish_corpus();

private:
	FILE *outfp;
	std::string outfile;
#ifndef O_TMPFILE
	std::string temp_filename;
#endif
	Header hdr;
	const int block_size;
	std::chrono::steady_clock::time_point corpus_start;
	Corpus *corpus = nullptr;
	ZSTD_CDict *cdict = nullptr;
	std::string next_dictionary, conf_block;
};

#endif  // !defined(_DATABASE_BUILDER_H)

#ifndef _DATABASE_BUILDER_H
#define _DATABASE_BUILDER_H 1

#include "db.h"

#include <chrono>
#include <memory>
#include <random>
#include <stddef.h>
#include <string>
#include <vector>
#include <zstd.h>

class PostingListBuilder;

class DatabaseReceiver {
public:
	virtual ~DatabaseReceiver() = default;
	virtual void add_file(std::string filename) = 0;
	virtual void flush_block() = 0;
	virtual void finish() { flush_block(); }
};

class DictionaryBuilder : public DatabaseReceiver {
public:
	DictionaryBuilder(size_t blocks_to_keep, size_t block_size)
		: blocks_to_keep(blocks_to_keep), block_size(block_size) {}
	void add_file(std::string filename) override;
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
	Corpus(FILE *outfp, size_t block_size, ZSTD_CDict *cdict);
	~Corpus();

	void add_file(std::string filename) override;
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

private:
	std::unique_ptr<PostingListBuilder *[]> invindex;
	FILE *outfp;
	std::string current_block;
	std::string tempbuf;
	const size_t block_size;
	ZSTD_CDict *cdict;
	std::string directory_data;
};

class DatabaseBuilder {
public:
	DatabaseBuilder(const char *outfile, int block_size, std::string dictionary);
	Corpus *start_corpus();
	void finish_corpus();

private:
	FILE *outfp;
	Header hdr;
	const int block_size;
	std::chrono::steady_clock::time_point corpus_start;
	Corpus *corpus = nullptr;
	ZSTD_CDict *cdict = nullptr;
};

#endif  // !defined(_DATABASE_BUILDER_H)

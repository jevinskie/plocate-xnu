#include "vp4.h"

#include <algorithm>
#include <arpa/inet.h>
#include <assert.h>
#include <chrono>
#include <endian.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <zstd.h>

#define P4NENC_BOUND(n) ((n + 127) / 128 + (n + 32) * sizeof(uint32_t))
#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);

using namespace std;
using namespace std::chrono;

string zstd_compress(const string &src, string *tempbuf);

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

enum {
	DBE_NORMAL = 0, /* A non-directory file */
	DBE_DIRECTORY = 1, /* A directory */
	DBE_END = 2 /* End of directory contents; contains no name */
};

// From mlocate.
struct db_header {
	uint8_t magic[8];
	uint32_t conf_size;
	uint8_t version;
	uint8_t check_visibility;
	uint8_t pad[2];
};

// From mlocate.
struct db_directory {
	uint64_t time_sec;
	uint32_t time_nsec;
	uint8_t pad[4];
};

class PostingListBuilder {
public:
	void add_docid(uint32_t docid);
	void finish();

	string encoded;
	size_t num_docids = 0;

private:
	void write_header(uint32_t docid);
	void append_block();

	vector<uint32_t> pending_docids;

	uint32_t last_block_end;
};

void PostingListBuilder::add_docid(uint32_t docid)
{
	// Deduplicate against the last inserted value, if any.
	if (pending_docids.empty()) {
		if (encoded.empty()) {
			// Very first docid.
			write_header(docid);
			++num_docids;
			last_block_end = docid;
			return;
		} else if (docid == last_block_end) {
			return;
		}
	} else {
		if (docid == pending_docids.back()) {
			return;
		}
	}

	pending_docids.push_back(docid);
	if (pending_docids.size() == 128) {
		append_block();
		pending_docids.clear();
		last_block_end = docid;
	}
	++num_docids;
}

void PostingListBuilder::finish()
{
	if (pending_docids.empty()) {
		return;
	}

	assert(!encoded.empty());  // write_header() should already have run.

	// No interleaving for partial blocks.
	unsigned char buf[P4NENC_BOUND(128)];
	unsigned char *end = p4d1enc32(pending_docids.data(), pending_docids.size(), buf, last_block_end);
	encoded.append(reinterpret_cast<char *>(buf), reinterpret_cast<char *>(end));
}

void PostingListBuilder::append_block()
{
	unsigned char buf[P4NENC_BOUND(128)];
	assert(pending_docids.size() == 128);
	unsigned char *end = p4d1enc128v32(pending_docids.data(), 128, buf, last_block_end);
	encoded.append(reinterpret_cast<char *>(buf), reinterpret_cast<char *>(end));
}

void PostingListBuilder::write_header(uint32_t docid)
{
	unsigned char buf[P4NENC_BOUND(1)];
	size_t bytes = p4nd1enc128v32(&docid, 1, buf);
	encoded.append(reinterpret_cast<char *>(buf), bytes);
}

class Corpus {
public:
	Corpus(size_t block_size)
		: block_size(block_size) {}
	void add_file(string filename);
	void flush_block();

	vector<string> filename_blocks;
	unordered_map<uint32_t, PostingListBuilder> invindex;
	size_t num_files = 0, num_files_in_block = 0, num_blocks = 0;

private:
	string current_block;
	string tempbuf;
	const size_t block_size;
};

void Corpus::add_file(string filename)
{
	++num_files;
	if (!current_block.empty()) {
		current_block.push_back('\0');
	}
	current_block += filename;
	if (++num_files_in_block == block_size) {
		flush_block();
	}
}

void Corpus::flush_block()
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
				invindex[trgm].add_docid(docid);
			}
		}
		ptr += s.size() + 1;
	}

	// Compress and add the filename block.
	filename_blocks.push_back(zstd_compress(current_block, &tempbuf));

	current_block.clear();
	num_files_in_block = 0;
	++num_blocks;
}

const char *handle_directory(const char *ptr, Corpus *corpus)
{
	ptr += sizeof(db_directory);

	string dir_path = ptr;
	ptr += dir_path.size() + 1;
	if (dir_path == "/") {
		dir_path = "";
	}

	for (;;) {
		uint8_t type = *ptr++;
		if (type == DBE_NORMAL) {
			string filename = ptr;
			corpus->add_file(dir_path + "/" + filename);
			ptr += filename.size() + 1;
		} else if (type == DBE_DIRECTORY) {
			string dirname = ptr;
			corpus->add_file(dir_path + "/" + dirname);
			ptr += dirname.size() + 1;
		} else {
			return ptr;
		}
	}
}

void read_mlocate(const char *filename, Corpus *corpus)
{
	int fd = open(filename, O_RDONLY);
	if (fd == -1) {
		perror(filename);
		exit(1);
	}
	off_t len = lseek(fd, 0, SEEK_END);
	if (len == -1) {
		perror("lseek");
		exit(1);
	}
	const char *data = (char *)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, /*offset=*/0);
	if (data == MAP_FAILED) {
		perror("mmap");
		exit(1);
	}

	const db_header *hdr = (const db_header *)data;

	// TODO: Care about the base path.
	string path = data + sizeof(db_header);
	uint64_t offset = sizeof(db_header) + path.size() + 1 + ntohl(hdr->conf_size);

	const char *ptr = data + offset;
	while (ptr < data + len) {
		ptr = handle_directory(ptr, corpus);
	}

	munmap((void *)data, len);
	close(fd);
}

string zstd_compress(const string &src, string *tempbuf)
{
	size_t max_size = ZSTD_compressBound(src.size());
	if (tempbuf->size() < max_size) {
		tempbuf->resize(max_size);
	}
	size_t size = ZSTD_compress(&(*tempbuf)[0], max_size, src.data(), src.size(), /*level=*/6);
	return string(tempbuf->data(), size);
}

void do_build(const char *infile, const char *outfile, int block_size)
{
	steady_clock::time_point start __attribute__((unused)) = steady_clock::now();

	Corpus corpus(block_size);

	read_mlocate(infile, &corpus);
	if (false) {  // To read a plain text file.
		FILE *fp = fopen(infile, "r");
		while (!feof(fp)) {
			char buf[1024];
			if (fgets(buf, 1024, fp) == nullptr || feof(fp)) {
				break;
			}
			string s(buf);
			if (s.back() == '\n')
				s.pop_back();
			corpus.add_file(move(s));
		}
		fclose(fp);
	}
	corpus.flush_block();
	dprintf("Read %zu files from %s\n", corpus.num_files, infile);

	size_t trigrams = 0, longest_posting_list = 0;
	size_t bytes_used = 0;
	for (auto &[trigram, pl_builder] : corpus.invindex) {
		pl_builder.finish();
		longest_posting_list = max(longest_posting_list, pl_builder.num_docids);
		trigrams += pl_builder.num_docids;
		bytes_used += pl_builder.encoded.size();
	}
	dprintf("%zu files, %zu different trigrams, %zu entries, avg len %.2f, longest %zu\n",
	        corpus.num_files, corpus.invindex.size(), trigrams, double(trigrams) / corpus.invindex.size(), longest_posting_list);
	dprintf("%zu bytes used for posting lists (%.2f bits/entry)\n", bytes_used, 8 * bytes_used / double(trigrams));

	dprintf("Building posting lists took %.1f ms.\n\n", 1e3 * duration<float>(steady_clock::now() - start).count());

	vector<uint32_t> all_trigrams;
	for (auto &[trigram, pl_builder] : corpus.invindex) {
		all_trigrams.push_back(trigram);
	}
	sort(all_trigrams.begin(), all_trigrams.end());

	umask(0027);
	FILE *outfp = fopen(outfile, "wb");

	// Write the header.
	uint64_t num_trigrams = corpus.invindex.size();  // 64 to get alignment.
	fwrite(&num_trigrams, sizeof(num_trigrams), 1, outfp);

	// Find out where the posting lists will end.
	uint64_t offset = sizeof(uint64_t) * 2 + 16 * all_trigrams.size();
	uint64_t filename_index_offset = offset;
	for (auto &[trigram, pl_builder] : corpus.invindex) {
		filename_index_offset += pl_builder.encoded.size();
	}
	fwrite(&filename_index_offset, sizeof(filename_index_offset), 1, outfp);

	size_t bytes_for_trigrams = 0, bytes_for_posting_lists = 0, bytes_for_filename_index = 0, bytes_for_filenames = 0;
	for (uint32_t trgm : all_trigrams) {
		// 16 bytes: trgm (4), number of docids (4), byte offset (8)
		fwrite(&trgm, sizeof(trgm), 1, outfp);  // One byte wasted, but OK.
		const PostingListBuilder &pl_builder = corpus.invindex[trgm];
		uint32_t num_docids = pl_builder.num_docids;
		fwrite(&num_docids, sizeof(num_docids), 1, outfp);
		fwrite(&offset, sizeof(offset), 1, outfp);
		offset += pl_builder.encoded.size();
		bytes_for_trigrams += 16;
	}

	// Write the actual posting lists.
	for (uint32_t trgm : all_trigrams) {
		const string &encoded = corpus.invindex[trgm].encoded;
		fwrite(encoded.data(), encoded.size(), 1, outfp);
		bytes_for_posting_lists += encoded.size();
	}

	// Stick an empty block at the end as sentinel.
	corpus.filename_blocks.push_back("");

	// Write the offsets to the filenames.
	offset = filename_index_offset + corpus.filename_blocks.size() * sizeof(offset);
	for (const string &filename : corpus.filename_blocks) {
		fwrite(&offset, sizeof(offset), 1, outfp);
		offset += filename.size();
		bytes_for_filename_index += sizeof(offset);
		bytes_for_filenames += filename.size();
	}

	// Write the actual filenames.
	for (const string &filename : corpus.filename_blocks) {
		fwrite(filename.data(), filename.size(), 1, outfp);
	}

	fclose(outfp);

	size_t total_bytes __attribute__((unused)) = (bytes_for_trigrams + bytes_for_posting_lists + bytes_for_filename_index + bytes_for_filenames);

	dprintf("Block size:     %7d files\n", block_size);
	dprintf("Trigrams:       %'7.1f MB\n", bytes_for_trigrams / 1048576.0);
	dprintf("Posting lists:  %'7.1f MB\n", bytes_for_posting_lists / 1048576.0);
	dprintf("Filename index: %'7.1f MB\n", bytes_for_filename_index / 1048576.0);
	dprintf("Filenames:      %'7.1f MB\n", bytes_for_filenames / 1048576.0);
	dprintf("Total:          %'7.1f MB\n", total_bytes / 1048576.0);
	dprintf("\n");
}

int main(int argc, char **argv)
{
	do_build(argv[1], argv[2], 32);
	exit(EXIT_SUCCESS);
}

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
#include <sys/types.h>
#include <sys/stat.h>

#include "vp4.h"

#define P4NENC_BOUND(n) ((n+127)/128+(n+32)*sizeof(uint32_t))
#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);

using namespace std;
using namespace std::chrono;
	
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

enum
{
	DBE_NORMAL          = 0,    /* A non-directory file */
	DBE_DIRECTORY       = 1,    /* A directory */
	DBE_END             = 2   /* End of directory contents; contains no name */
};

// From mlocate.
struct db_header
{
	uint8_t magic[8];
	uint32_t conf_size;
	uint8_t version;
	uint8_t check_visibility;
	uint8_t pad[2];
};

// From mlocate.
struct db_directory
{
	uint64_t time_sec;
	uint32_t time_nsec;
	uint8_t pad[4];
};

const char *handle_directory(const char *ptr, vector<string> *files)
{
	ptr += sizeof(db_directory);

	string dir_path = ptr;
	ptr += dir_path.size() + 1;
	if (dir_path == "/") {
		dir_path = "";
	}
		
	for ( ;; ) {
		uint8_t type = *ptr++;
		if (type == DBE_NORMAL) {
			string filename = ptr;
			files->push_back(dir_path + "/" + filename);
			ptr += filename.size() + 1;
		} else if (type == DBE_DIRECTORY) {
			string dirname = ptr;
			files->push_back(dir_path + "/" + dirname);
			ptr += dirname.size() + 1;
		} else {
			return ptr;
		}
	}
}

void read_mlocate(const char *filename, vector<string> *files)
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
		ptr = handle_directory(ptr, files);
	}

	munmap((void *)data, len);
	close(fd);
}

void do_build(const char *infile, const char *outfile)
{
	//steady_clock::time_point start = steady_clock::now();

	vector<string> files;
	read_mlocate(infile, &files);
	if (false) {  // To read a plain text file.
		FILE *fp = fopen(infile, "r");
		while (!feof(fp)) {
			char buf[1024];
			if (fgets(buf, 1024, fp) == nullptr || feof(fp)) {
				break;
			}
			string s(buf);
			if (s.back() == '\n') s.pop_back();
			files.push_back(move(s));
		}
		fclose(fp);
	}
	dprintf("Read %zu files from %s\n", files.size(), infile);
	
	unordered_map<uint32_t, string> pl;
	size_t trigrams = 0, longest_posting_list = 0;
	unordered_map<uint32_t, vector<uint32_t>> invindex;
	for (size_t i = 0; i < files.size(); ++i) {
		const string &s = files[i];
		if (s.size() >= 3) {
			for (size_t j = 0; j < s.size() - 2; ++j) {
				uint32_t trgm = read_trigram(s, j);
				invindex[trgm].push_back(i);
			}
		}
	}
	string buf;
	size_t bytes_used = 0;
	for (auto &[trigram, docids] : invindex) {
		auto last = unique(docids.begin(), docids.end());
		docids.erase(last, docids.end());
		longest_posting_list = max(longest_posting_list, docids.size());
		trigrams += docids.size();

		size_t bytes_needed = P4NENC_BOUND(docids.size());
		if (buf.size() < bytes_needed) buf.resize(bytes_needed);
		size_t bytes = p4nd1enc128v32(&docids[0], docids.size(), reinterpret_cast<unsigned char *>(&buf[0]));
		pl[trigram] = string(buf.data(), bytes);
		bytes_used += bytes;
	}
	dprintf("%zu files, %zu different trigrams, %zu entries, avg len %.2f, longest %zu\n",
		files.size(), invindex.size(), trigrams, double(trigrams) / invindex.size(), longest_posting_list);

	dprintf("%zu bytes used for posting lists (%.2f bits/entry)\n", bytes_used, 8 * bytes_used / double(trigrams));
	//steady_clock::time_point end = steady_clock::now();
	dprintf("Building posting lists took %.1f ms.\n\n", 1e3 * duration<float>(end - start).count());

	vector<uint32_t> all_trigrams;
	for (auto &[trigram, docids] : invindex) {
		all_trigrams.push_back(trigram);
	}
	sort(all_trigrams.begin(), all_trigrams.end());

	umask(0027);
	FILE *outfp = fopen(outfile, "wb");

	// Write the header.
	uint64_t num_trigrams = invindex.size();  // 64 to get alignment.
	fwrite(&num_trigrams, sizeof(num_trigrams), 1, outfp);

	// Find out where the posting lists will end.
	uint64_t offset = sizeof(uint64_t) * 2 + 16 * all_trigrams.size();
	uint64_t filename_index_offset = offset;
	for (uint32_t trgm : all_trigrams) {
		filename_index_offset += pl[trgm].size();
	}
	fwrite(&filename_index_offset, sizeof(filename_index_offset), 1, outfp);

	size_t bytes_for_trigrams = 0, bytes_for_posting_lists = 0, bytes_for_filename_index = 0, bytes_for_filenames = 0;

	for (uint32_t trgm : all_trigrams) {
		// 16 bytes: trgm (4), number of docids (4), byte offset (8)
		fwrite(&trgm, sizeof(trgm), 1, outfp);  // One byte wasted, but OK.
		uint32_t num_docids = invindex[trgm].size();
		fwrite(&num_docids, sizeof(num_docids), 1, outfp);
		fwrite(&offset, sizeof(offset), 1, outfp);
		offset += pl[trgm].size();
		bytes_for_trigrams += 16;
	}

	// Write the actual posting lists.
	for (uint32_t trgm : all_trigrams) {
		fwrite(pl[trgm].data(), pl[trgm].size(), 1, outfp);
		bytes_for_posting_lists += pl[trgm].size();
	}

	// Write the offsets to the filenames.
	offset = filename_index_offset + files.size() * sizeof(offset);
	for (const string &filename : files) {
		fwrite(&offset, sizeof(offset), 1, outfp);
		offset += filename.size() + 1;
		bytes_for_filename_index += sizeof(offset);
		bytes_for_filenames += filename.size() + 1;
	}
	
	// Write the actual filenames.
	for (const string &filename : files) {
		fwrite(filename.c_str(), filename.size() + 1, 1, outfp);
	}

	fclose(outfp);

	size_t total_bytes = (bytes_for_trigrams + bytes_for_posting_lists + bytes_for_filename_index + bytes_for_filenames);

	dprintf("Trigrams:       %'7.1f MB\n", bytes_for_trigrams / 1048576.0);
	dprintf("Posting lists:  %'7.1f MB\n", bytes_for_posting_lists / 1048576.0);
	dprintf("Filename index: %'7.1f MB\n", bytes_for_filename_index / 1048576.0);
	dprintf("Filenames:      %'7.1f MB\n", bytes_for_filenames / 1048576.0);
	dprintf("Total:          %'7.1f MB\n", total_bytes / 1048576.0);
	dprintf("\n");
}

int main(int argc, char **argv)
{
	do_build(argv[1], argv[2]);
	exit(EXIT_SUCCESS);
}

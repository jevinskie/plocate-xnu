#include "database-builder.h"
#include "db.h"
#include "dprintf.h"

#include <algorithm>
#include <arpa/inet.h>
#include <assert.h>
#include <chrono>
#include <getopt.h>
#include <iosfwd>
#include <locale.h>
#include <math.h>
#include <memory>
#include <random>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <utility>
#include <vector>

using namespace std;
using namespace std::chrono;

bool use_debug = false;

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

string read_cstr(FILE *fp)
{
	string ret;
	for (;;) {
		int ch = getc(fp);
		if (ch == -1) {
			perror("getc");
			exit(1);
		}
		if (ch == 0) {
			return ret;
		}
		ret.push_back(ch);
	}
}

void handle_directory(FILE *fp, DatabaseReceiver *receiver)
{
	db_directory dummy;
	if (fread(&dummy, sizeof(dummy), 1, fp) != 1) {
		if (feof(fp)) {
			return;
		} else {
			perror("fread");
		}
	}

	string dir_path = read_cstr(fp);
	if (dir_path == "/") {
		dir_path = "";
	}

	for (;;) {
		int type = getc(fp);
		if (type == DBE_NORMAL) {
			string filename = read_cstr(fp);
			receiver->add_file(dir_path + "/" + filename, unknown_dir_time);
		} else if (type == DBE_DIRECTORY) {
			string dirname = read_cstr(fp);
			receiver->add_file(dir_path + "/" + dirname, unknown_dir_time);
		} else {
			return;  // Probably end.
		}
	}
}

void read_plaintext(FILE *fp, DatabaseReceiver *receiver)
{
	if (fseek(fp, 0, SEEK_SET) != 0) {
		perror("fseek");
		exit(1);
	}

	while (!feof(fp)) {
		char buf[1024];
		if (fgets(buf, sizeof(buf), fp) == nullptr) {
			break;
		}
		string s(buf);
		assert(!s.empty());
		while (s.back() != '\n' && !feof(fp)) {
			// The string was longer than the buffer, so read again.
			if (fgets(buf, sizeof(buf), fp) == nullptr) {
				break;
			}
			s += buf;
		}
		if (!s.empty() && s.back() == '\n')
			s.pop_back();
		receiver->add_file(move(s), unknown_dir_time);
	}
}

void read_mlocate(FILE *fp, DatabaseReceiver *receiver)
{
	if (fseek(fp, 0, SEEK_SET) != 0) {
		perror("fseek");
		exit(1);
	}

	db_header hdr;
	if (fread(&hdr, sizeof(hdr), 1, fp) != 1) {
		perror("short read");
		exit(1);
	}

	// TODO: Care about the base path.
	string path = read_cstr(fp);

	if (fseek(fp, ntohl(hdr.conf_size), SEEK_CUR) != 0) {
		perror("skip conf block");
		exit(1);
	}

	while (!feof(fp)) {
		handle_directory(fp, receiver);
	}
}

void do_build(const char *infile, const char *outfile, int block_size, bool plaintext)
{
	FILE *infp = fopen(infile, "rb");
	if (infp == nullptr) {
		perror(infile);
		exit(1);
	}

	// Train the dictionary by sampling real blocks.
	// The documentation for ZDICT_trainFromBuffer() claims that a reasonable
	// dictionary size is ~100 kB, but 1 kB seems to actually compress better for us,
	// and decompress just as fast.
	DictionaryBuilder builder(/*blocks_to_keep=*/1000, block_size);
	if (plaintext) {
		read_plaintext(infp, &builder);
	} else {
		read_mlocate(infp, &builder);
	}
	string dictionary = builder.train(1024);

	DatabaseBuilder db(outfile, /*owner=*/-1, block_size, dictionary, /*check_visibility=*/true);
	DatabaseReceiver *corpus = db.start_corpus(/*store_dir_times=*/false);
	if (plaintext) {
		read_plaintext(infp, corpus);
	} else {
		read_mlocate(infp, corpus);
	}
	fclose(infp);

	dprintf("Read %zu files from %s\n", corpus->num_files_seen(), infile);
	db.finish_corpus();
}

void usage()
{
	printf(
		"Usage: plocate-build MLOCATE_DB PLOCATE_DB\n"
		"\n"
		"Generate plocate index from mlocate.db, typically /var/lib/mlocate/mlocate.db.\n"
		"Normally, the destination should be /var/lib/mlocate/plocate.db.\n"
		"\n"
		"  -b, --block-size SIZE  number of filenames to store in each block (default 32)\n"
		"  -p, --plaintext        input is a plaintext file, not an mlocate database\n"
		"      --help             print this help\n"
		"      --version          print version information\n");
}

void version()
{
	printf("plocate-build %s\n", PACKAGE_VERSION);
	printf("Copyright 2020 Steinar H. Gunderson\n");
	printf("License GPLv2+: GNU GPL version 2 or later <https://gnu.org/licenses/gpl.html>.\n");
	printf("This is free software: you are free to change and redistribute it.\n");
	printf("There is NO WARRANTY, to the extent permitted by law.\n");
}

int main(int argc, char **argv)
{
	static const struct option long_options[] = {
		{ "block-size", required_argument, 0, 'b' },
		{ "plaintext", no_argument, 0, 'p' },
		{ "help", no_argument, 0, 'h' },
		{ "version", no_argument, 0, 'V' },
		{ "debug", no_argument, 0, 'D' },  // Not documented.
		{ 0, 0, 0, 0 }
	};

	int block_size = 32;
	bool plaintext = false;

	setlocale(LC_ALL, "");
	for (;;) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "b:hpVD", long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch (c) {
		case 'b':
			block_size = atoi(optarg);
			break;
		case 'p':
			plaintext = true;
			break;
		case 'h':
			usage();
			exit(0);
		case 'V':
			version();
			exit(0);
		case 'D':
			use_debug = true;
			break;
		default:
			exit(1);
		}
	}

	if (argc - optind != 2) {
		usage();
		exit(1);
	}

	do_build(argv[optind], argv[optind + 1], block_size, plaintext);
	exit(EXIT_SUCCESS);
}

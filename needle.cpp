#include "needle.h"

#include "options.h"
#include "parse_trigrams.h"

#include <assert.h>
#include <fnmatch.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utility>

using namespace std;

bool matches(const Needle &needle, const char *haystack)
{
	if (needle.type == Needle::STRSTR) {
		return strstr(haystack, needle.str.c_str()) != nullptr;
	} else if (needle.type == Needle::GLOB) {
		int flags = ignore_case ? FNM_CASEFOLD : 0;
		return fnmatch(needle.str.c_str(), haystack, flags) == 0;
	} else {
		assert(needle.type == Needle::REGEX);
		return regexec(&needle.re, haystack, /*nmatch=*/0, /*pmatch=*/nullptr, /*flags=*/0) == 0;
	}
}

string unescape_glob_to_plain_string(const string &needle)
{
	string unescaped;
	for (size_t i = 0; i < needle.size(); i += read_unigram(needle, i).second) {
		uint32_t ch = read_unigram(needle, i).first;
		assert(ch != WILDCARD_UNIGRAM);
		if (ch == PREMATURE_END_UNIGRAM) {
			fprintf(stderr, "Pattern '%s' ended prematurely\n", needle.c_str());
			exit(1);
		}
		unescaped.push_back(ch);
	}
	return unescaped;
}

regex_t compile_regex(const string &needle)
{
	regex_t re;
	int flags = REG_NOSUB;
	if (ignore_case) {
		flags |= REG_ICASE;
	}
	if (use_extended_regex) {
		flags |= REG_EXTENDED;
	}
	int err = regcomp(&re, needle.c_str(), flags);
	if (err != 0) {
		char errbuf[256];
		regerror(err, &re, errbuf, sizeof(errbuf));
		fprintf(stderr, "Error when compiling regex '%s': %s\n", needle.c_str(), errbuf);
		exit(1);
	}
	return re;
}

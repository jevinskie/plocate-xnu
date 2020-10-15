#ifndef _NEEDLE_H
#define _NEEDLE_H 1

#include <regex.h>
#include <string>

struct Needle {
	enum { STRSTR,
	       REGEX,
	       GLOB } type;
	std::string str;  // Filled in no matter what.
	regex_t re;  // For REGEX.
};

bool matches(const Needle &needle, const char *haystack);
std::string unescape_glob_to_plain_string(const std::string &needle);
regex_t compile_regex(const std::string &needle);

#endif  // !defined(_NEEDLE_H)

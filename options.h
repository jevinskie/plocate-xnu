#ifndef _OPTIONS_H
#define _OPTIONS_H

#include <stdint.h>

extern const char *dbpath;
extern bool ignore_case;
extern bool only_count;
extern bool print_nul;
extern bool use_debug;
extern bool flush_cache;
extern bool patterns_are_regex;
extern bool use_extended_regex;
extern int64_t limit_matches;
extern int64_t limit_left;  // Not strictly an option.

#endif  // !defined(_OPTIONS_H)

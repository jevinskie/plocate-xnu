#ifndef _PARSE_TRIGRAMS_H
#define _PARSE_TRIGRAMS_H 1

#include "db.h"

#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

// One or more trigrams, with an implicit OR between them. For case-sensitive searches,
// this is just e.g. “abc”, but for case-insensitive, it would be “abc OR abC or aBc ...” etc.
struct TrigramDisjunction {
	unsigned index;  // For debugging only.

	// The alternatives as determined by parse_trigrams().
	std::vector<uint32_t> trigram_alternatives;

	// Like trigram_alternatives, but only the ones we've actually read from the
	// hash table (the non-existent ones are filtered out). The second member is
	// the length in bytes. Incomplete if remaining_trigrams_to_read > 0.
	std::vector<std::pair<Trigram, size_t>> read_trigrams;

	// Sum of num_docids in all trigrams. This is usually a fairly good indicator
	// of the real number of docids, since there are few files that would have e.g.
	// both abc and abC in them (but of course, with multiple files in the same
	// docid block, it is far from unheard of).
	uint32_t max_num_docids;

	// While reading posting lists: Holds the union of the posting lists read
	// so far. Once remaining_trigrams_to_read == 0 (all are read), will be taken
	// out and used for intersections against the other disjunctions.
	std::vector<uint32_t> docids;

	// While looking up in the hash table (filling out read_trigrams): Number of
	// lookups in the hash table remaining. While reading actual posting lists
	// (filling out docids): Number of posting lists left to read.
	unsigned remaining_trigrams_to_read;
};

// Take the given needle (search string) and break it down into a set of trigrams
// (or trigram alternatives; see TrigramDisjunction) that must be present for the
// string to match. (Note: They are not _sufficient_ for the string to match;
// false positives might very well occur and must be weeded out later.)
//
// For the case-sensitive case, this is straightforward; just take every trigram
// present in the needle and add them (e.g. abcd -> abc AND bcd).
// For case-insensitivity, it's trickier; see the comments in the function.
//
// Note that our trigrams are on the basis of bytes, not Unicode code points.
// This both simplifies table structure (everything is the same length), and
// guards us against trigram explosion (imagine every combination of CJK characters
// getting their own trigram).
void parse_trigrams(const std::string &needle, bool ignore_case, std::vector<TrigramDisjunction> *trigram_groups);

uint32_t read_trigram(const std::string &s, size_t start);

// For debugging.
std::string print_td(const TrigramDisjunction &td);
std::string print_trigram(uint32_t trgm);

#endif  // !defined(_PARSE_TRIGRAMS_H)

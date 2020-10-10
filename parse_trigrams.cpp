#include "parse_trigrams.h"

#include "unique_sort.h"

#include <string.h>
#include <wctype.h>

using namespace std;

string print_td(const TrigramDisjunction &td)
{
	if (td.read_trigrams.size() == 0) {
		// Before we've done hash lookups (or none matched), so print all alternatives.
		if (td.trigram_alternatives.size() == 1) {
			return print_trigram(td.trigram_alternatives[0]);
		} else {
			string ret;
			ret = "(";
			bool first = true;
			for (uint32_t trgm : td.trigram_alternatives) {
				if (!first)
					ret += " OR ";
				ret += print_trigram(trgm);
				first = false;
			}
			return ret + ")";
		}
	} else {
		// Print only those that we actually have in the index.
		if (td.read_trigrams.size() == 1) {
			return print_trigram(td.read_trigrams[0].first.trgm);
		} else {
			string ret;
			ret = "(";
			bool first = true;
			for (auto &[trgmptr, len] : td.read_trigrams) {
				if (!first)
					ret += " OR ";
				ret += print_trigram(trgmptr.trgm);
				first = false;
			}
			return ret + ")";
		}
	}
}

string print_trigram(uint32_t trgm)
{
	char ch[3] = {
		char(trgm & 0xff), char((trgm >> 8) & 0xff), char((trgm >> 16) & 0xff)
	};

	string str = "'";
	for (unsigned i = 0; i < 3;) {
		if (ch[i] == '\\') {
			str.push_back('\\');
			str.push_back(ch[i]);
			++i;
		} else if (int(ch[i]) >= 32 && int(ch[i]) <= 127) {  // Holds no matter whether char is signed or unsigned.
			str.push_back(ch[i]);
			++i;
		} else {
			// See if we have an entire UTF-8 codepoint, and that it's reasonably printable.
			mbtowc(nullptr, 0, 0);
			wchar_t pwc;
			int ret = mbtowc(&pwc, ch + i, 3 - i);
			if (ret >= 1 && pwc >= 32) {
				str.append(ch + i, ret);
				i += ret;
			} else {
				char buf[16];
				snprintf(buf, sizeof(buf), "\\x{%02x}", (unsigned char)ch[i]);
				str += buf;
				++i;
			}
		}
	}
	str += "'";
	return str;
}

uint32_t read_unigram(const string &s, size_t idx)
{
	if (idx < s.size()) {
		return (unsigned char)s[idx];
	} else {
		return 0;
	}
}

uint32_t read_trigram(const string &s, size_t start)
{
	return read_unigram(s, start) | (read_unigram(s, start + 1) << 8) |
		(read_unigram(s, start + 2) << 16);
}

struct TrigramState {
	string buffered;
	unsigned next_codepoint;

	bool operator<(const TrigramState &other) const
	{
		if (next_codepoint != other.next_codepoint)
			return next_codepoint < other.next_codepoint;
		return buffered < other.buffered;
	}
	bool operator==(const TrigramState &other) const
	{
		return next_codepoint == other.next_codepoint &&
			buffered == other.buffered;
	}
};

void parse_trigrams_ignore_case(const string &needle, vector<TrigramDisjunction> *trigram_groups)
{
	vector<vector<string>> alternatives_for_cp;

	// Parse the needle into Unicode code points, and do inverse case folding
	// on each to find legal alternatives. This is far from perfect (e.g. ß
	// will not become ss), but it's generally the best we can do without
	// involving ICU or the likes.
	mbtowc(nullptr, 0, 0);
	const char *ptr = needle.c_str();
	while (*ptr != '\0') {
		wchar_t ch;
		int ret = mbtowc(&ch, ptr, strlen(ptr));
		if (ret == -1) {
			perror(ptr);
			exit(1);
		}

		char buf[MB_CUR_MAX];
		vector<string> alt;
		alt.push_back(string(ptr, ret));
		ptr += ret;
		if (towlower(ch) != wint_t(ch)) {
			ret = wctomb(buf, towlower(ch));
			alt.push_back(string(buf, ret));
		}
		if (towupper(ch) != wint_t(ch) && towupper(ch) != towlower(ch)) {
			ret = wctomb(buf, towupper(ch));
			alt.push_back(string(buf, ret));
		}
		alternatives_for_cp.push_back(move(alt));
	}

	// Now generate all possible byte strings from those code points in order;
	// e.g., from abc, we'd create a and A, then extend those to ab aB Ab AB,
	// then abc abC aBc aBC and so on. Since we don't want to have 2^n
	// (or even 3^n) strings, we only extend them far enough to cover at
	// least three bytes; this will give us a set of candidate trigrams
	// (the filename must have at least one of those), and then we can
	// chop off the first byte, deduplicate states and continue extending
	// and generating trigram sets.
	//
	// There are a few special cases, notably the dotted i (İ), where the
	// UTF-8 versions of upper and lower case have different number of bytes.
	// If this happens, we can have combinatorial explosion and get many more
	// than the normal 8 states. We detect this and simply bomb out; it will
	// never really happen in real strings, and stopping trigram generation
	// really only means our pruning of candidates will be less effective.
	vector<TrigramState> states;
	states.push_back(TrigramState{ "", 0 });

	for (;;) {
		// Extend every state so that it has buffered at least three bytes.
		// If this isn't possible, we are done with the string (can generate
		// no more trigrams).
		bool need_another_pass;
		do {
			need_another_pass = false;
			vector<TrigramState> new_states;
			for (const TrigramState &state : states) {
				if (state.buffered.size() >= 3) {
					// No need to extend this further.
					new_states.push_back(state);
					continue;
				}
				if (state.next_codepoint == alternatives_for_cp.size()) {
					// We can't form a complete trigram from this alternative,
					// so we're done.
					return;
				}
				for (const string &rune : alternatives_for_cp[state.next_codepoint]) {
					TrigramState new_state{ state.buffered + rune, state.next_codepoint + 1 };
					if (new_state.buffered.size() < 3) {
						need_another_pass = true;
					}
					new_states.push_back(move(new_state));
				}
			}
			states = move(new_states);
		} while (need_another_pass);

		// OK, so now we have a bunch of states, and all of them are at least
		// three bytes long. This means we have a complete set of trigrams,
		// and the destination filename must contain at least one of them.
		// Output those trigrams, cut out the first byte and then deduplicate
		// the states before we continue.
		vector<uint32_t> trigram_alternatives;
		for (TrigramState &state : states) {
			trigram_alternatives.push_back(read_trigram(state.buffered, 0));
			state.buffered.erase(0, 1);
		}
		unique_sort(&trigram_alternatives);  // Could have duplicates, although it's rare.
		unique_sort(&states);

		TrigramDisjunction new_pt;
		new_pt.remaining_trigrams_to_read = trigram_alternatives.size();
		new_pt.trigram_alternatives = move(trigram_alternatives);
		new_pt.max_num_docids = 0;
		trigram_groups->push_back(move(new_pt));

		if (states.size() > 100) {
			// A completely crazy pattern with lots of those special characters.
			// We just give up; this isn't a realistic scenario anyway.
			// We already have lots of trigrams that should reduce the amount of
			// candidates.
			return;
		}
	}
}

void parse_trigrams(const string &needle, bool ignore_case, vector<TrigramDisjunction> *trigram_groups)
{
	if (ignore_case) {
		parse_trigrams_ignore_case(needle, trigram_groups);
		return;
	}

	// The case-sensitive case is straightforward.
	if (needle.size() >= 3) {
		for (size_t i = 0; i < needle.size() - 2; ++i) {
			uint32_t trgm = read_trigram(needle, i);
			TrigramDisjunction new_pt;
			new_pt.remaining_trigrams_to_read = 1;
			new_pt.trigram_alternatives.push_back(trgm);
			new_pt.max_num_docids = 0;
			trigram_groups->push_back(move(new_pt));
		}
	}
}

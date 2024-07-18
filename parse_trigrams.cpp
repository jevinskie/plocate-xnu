#include "parse_trigrams.h"

#include "unique_sort.h"

#include <assert.h>
#include <memory>
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

pair<uint32_t, size_t> read_unigram(const string &s, size_t start)
{
	if (start >= s.size()) {
		return { PREMATURE_END_UNIGRAM, 0 };
	}
	if (s[start] == '\\') {
		// Escaped character.
		if (start + 1 >= s.size()) {
			return { PREMATURE_END_UNIGRAM, 1 };
		} else {
			return { (unsigned char)s[start + 1], 2 };
		}
	}
	if (s[start] == '*' || s[start] == '?') {
		// Wildcard.
		return { WILDCARD_UNIGRAM, 1 };
	}
	if (s[start] == '[') {
		// Character class; search to find the end.
		size_t len = 1;
		if (start + len >= s.size()) {
			return { PREMATURE_END_UNIGRAM, len };
		}
		if (s[start + len] == '!') {
			++len;
		}
		if (start + len >= s.size()) {
			return { PREMATURE_END_UNIGRAM, len };
		}
		if (s[start + len] == ']') {
			++len;
		}
		for (;;) {
			if (start + len >= s.size()) {
				return { PREMATURE_END_UNIGRAM, len };
			}
			if (s[start + len] == ']') {
				return { WILDCARD_UNIGRAM, len + 1 };
			}
			++len;
		}
	}

	// Regular letter.
	return { (unsigned char)s[start], 1 };
}

uint32_t read_trigram(const string &s, size_t start)
{
	pair<uint32_t, size_t> u1 = read_unigram(s, start);
	if (u1.first == WILDCARD_UNIGRAM || u1.first == PREMATURE_END_UNIGRAM) {
		return u1.first;
	}
	pair<uint32_t, size_t> u2 = read_unigram(s, start + u1.second);
	if (u2.first == WILDCARD_UNIGRAM || u2.first == PREMATURE_END_UNIGRAM) {
		return u2.first;
	}
	pair<uint32_t, size_t> u3 = read_unigram(s, start + u1.second + u2.second);
	if (u3.first == WILDCARD_UNIGRAM || u3.first == PREMATURE_END_UNIGRAM) {
		return u3.first;
	}
	return u1.first | (u2.first << 8) | (u3.first << 16);
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
	unique_ptr<char[]> buf(new char[MB_CUR_MAX]);
	while (*ptr != '\0') {
		wchar_t ch;
		int ret = mbtowc(&ch, ptr, strlen(ptr));
		if (ret == -1) {
			perror(ptr);
			exit(1);
		}

		vector<string> alt;
		alt.push_back(string(ptr, ret));
		ptr += ret;
		if (towlower(ch) != wint_t(ch)) {
			ret = wctomb(buf.get(), towlower(ch));
			alt.push_back(string(buf.get(), ret));
		}
		if (towupper(ch) != wint_t(ch) && towupper(ch) != towlower(ch)) {
			ret = wctomb(buf.get(), towupper(ch));
			alt.push_back(string(buf.get(), ret));
		}
		alternatives_for_cp.push_back(std::move(alt));
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
				if (read_trigram(state.buffered, 0) != PREMATURE_END_UNIGRAM) {
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
					if (read_trigram(state.buffered, 0) == PREMATURE_END_UNIGRAM) {
						need_another_pass = true;
					}
					new_states.push_back(std::move(new_state));
				}
			}
			states = std::move(new_states);
		} while (need_another_pass);

		// OK, so now we have a bunch of states, and all of them are at least
		// three bytes long. This means we have a complete set of trigrams,
		// and the destination filename must contain at least one of them.
		// Output those trigrams, cut out the first byte and then deduplicate
		// the states before we continue.
		bool any_wildcard = false;
		vector<uint32_t> trigram_alternatives;
		for (TrigramState &state : states) {
			trigram_alternatives.push_back(read_trigram(state.buffered, 0));
			state.buffered.erase(0, read_unigram(state.buffered, 0).second);
			assert(trigram_alternatives.back() != PREMATURE_END_UNIGRAM);
			if (trigram_alternatives.back() == WILDCARD_UNIGRAM) {
				// If any of the candidates are wildcards, we need to drop the entire OR group.
				// (Most likely, all of them would be anyway.) We need to keep stripping out
				// the first unigram from each state.
				any_wildcard = true;
			}
		}
		unique_sort(&trigram_alternatives);  // Could have duplicates, although it's rare.
		unique_sort(&states);

		if (!any_wildcard) {
			TrigramDisjunction new_pt;
			new_pt.remaining_trigrams_to_read = trigram_alternatives.size();
			new_pt.trigram_alternatives = std::move(trigram_alternatives);
			new_pt.max_num_docids = 0;
			trigram_groups->push_back(std::move(new_pt));
		}

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
	for (size_t i = 0; i < needle.size(); i += read_unigram(needle, i).second) {
		uint32_t trgm = read_trigram(needle, i);
		if (trgm == WILDCARD_UNIGRAM || trgm == PREMATURE_END_UNIGRAM) {
			// Invalid trigram, so skip.
			continue;
		}

		TrigramDisjunction new_pt;
		new_pt.remaining_trigrams_to_read = 1;
		new_pt.trigram_alternatives.push_back(trgm);
		new_pt.max_num_docids = 0;
		trigram_groups->push_back(std::move(new_pt));
	}
}

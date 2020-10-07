#ifndef _TURBOPFOR_COMMON_H
#define _TURBOPFOR_COMMON_H 1

// Common definitions and utilities between turbopfor.h (decode)
// and turbopfor-encode.h (encode).

#include <limits.h>

enum BlockType {
	FOR = 0,
	PFOR_VB = 1,
	PFOR_BITMAP = 2,
	CONSTANT = 3
};

// Does not properly account for overflow.
inline unsigned div_round_up(unsigned val, unsigned div)
{
	return (val + div - 1) / div;
}

inline unsigned bytes_for_packed_bits(unsigned num, unsigned bit_width)
{
	return div_round_up(num * bit_width, CHAR_BIT);
}

constexpr uint32_t mask_for_bits(unsigned bit_width)
{
	if (bit_width == 32) {
		return 0xFFFFFFFF;
	} else {
		return (1U << bit_width) - 1;
	}
}

#endif  // !defined(_TURBOPFOR_COMMON_H)

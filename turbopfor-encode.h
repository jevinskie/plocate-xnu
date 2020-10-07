#ifndef _TURBOPFOR_ENCODE_H
#define _TURBOPFOR_ENCODE_H

// Much like turbopfor.h (and shares all of the same caveats), except this is
// for encoding. It is _much_ slower than the reference implementation, but we
// encode only during build, and most time in build is spent in other things
// than encoding posting lists, so it only costs ~5-10% overall. Does not use
// any special character sets, and generally isn't optimized at all.
//
// It encodes about 0.01% denser than the reference encoder (averaged over
// a real plocate corpus), probably since it has a slower but more precise
// method for estimating the cost of a PFOR + varbyte block.

#include "turbopfor-common.h"

#include <algorithm>

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

template<class Docid>
void write_le(Docid val, void *out)
{
	if constexpr (sizeof(Docid) == 8) {
		val = htole64(val);
	} else if constexpr (sizeof(Docid) == 4) {
		val = htole32(val);
	} else if constexpr (sizeof(Docid) == 2) {
		val = htole16(val);
	} else if constexpr (sizeof(Docid) == 1) {
		// No change.
	} else {
		assert(false);
	}
	memcpy(out, &val, sizeof(val));
}

// Corresponds to read_baseval.
template<class Docid>
unsigned char *write_baseval(Docid in, unsigned char *out)
{
	if (in < 128) {
		*out = in;
		return out + 1;
	} else if (in < 0x4000) {
		out[0] = (in >> 8) | 0x80;
		out[1] = in & 0xff;
		return out + 2;
	} else if (in < 0x200000) {
		out[0] = (in >> 16) | 0xc0;
		out[1] = in & 0xff;
		out[2] = (in >> 8) & 0xff;
		return out + 3;
	} else {
		assert(false);  // Not implemented.
	}
}

// Writes a varbyte-encoded exception.
template<class Docid>
unsigned char *write_vb(Docid val, unsigned char *out)
{
	if (val <= 176) {
		*out++ = val;
		return out;
	} else if (val <= 16560) {
		val -= 177;
		*out++ = (val >> 8) + 177;
		*out++ = val & 0xff;
		return out;
	} else if (val <= 540848) {
		val -= 16561;
		*out = (val >> 16) + 241;
		write_le<uint16_t>(val & 0xffff, out + 1);
		return out + 3;
	} else if (val <= 16777215) {
		*out = 249;
		write_le<uint32_t>(val, out + 1);
		return out + 4;
	} else {
		*out = 250;
		write_le<uint32_t>(val, out + 1);
		return out + 5;
	}
}

template<class Docid>
inline unsigned num_bits(Docid x)
{
	if (x == 0) {
		return 0;
	} else {
		return sizeof(Docid) * CHAR_BIT - __builtin_clz(x);
	}
}

struct BitWriter {
public:
	BitWriter(unsigned char *out, unsigned bits)
		: out(out), bits(bits) {}
	void write(uint32_t val)
	{
		cur_val |= val << bits_used;
		write_le<uint32_t>(cur_val, out);

		bits_used += bits;
		cur_val >>= (bits_used / 8) * 8;
		out += bits_used / 8;
		bits_used %= 8;
	}

private:
	unsigned char *out;
	const unsigned bits;
	unsigned bits_used = 0;
	unsigned cur_val = 0;
};

template<unsigned NumStreams>
struct InterleavedBitWriter {
public:
	InterleavedBitWriter(unsigned char *out, unsigned bits)
		: out(out), bits(bits) {}
	void write(uint32_t val)
	{
		cur_val |= uint64_t(val) << bits_used;
		if (bits_used + bits >= 32) {
			write_le<uint32_t>(cur_val & 0xffffffff, out);
			out += Stride;
			cur_val >>= 32;
			bits_used -= 32;  // Underflow, but will be fixed below.
		}
		write_le<uint32_t>(cur_val, out);
		bits_used += bits;
	}

private:
	static constexpr unsigned Stride = NumStreams * sizeof(uint32_t);
	unsigned char *out;
	const unsigned bits;
	unsigned bits_used = 0;
	uint64_t cur_val = 0;
};

// Bitpacks a set of values (making sure the top bits are lopped off).
// If interleaved is set, makes SSE2-compatible interleaving (this is
// only allowed for full blocks).
template<class Docid>
unsigned char *encode_bitmap(const Docid *in, unsigned num, unsigned bit_width, bool interleaved, unsigned char *out)
{
	unsigned mask = mask_for_bits(bit_width);
	if (interleaved) {
		InterleavedBitWriter<4> bs0(out + 0 * sizeof(uint32_t), bit_width);
		InterleavedBitWriter<4> bs1(out + 1 * sizeof(uint32_t), bit_width);
		InterleavedBitWriter<4> bs2(out + 2 * sizeof(uint32_t), bit_width);
		InterleavedBitWriter<4> bs3(out + 3 * sizeof(uint32_t), bit_width);
		assert(num % 4 == 0);
		for (unsigned i = 0; i < num / 4; ++i) {
			bs0.write(in[i * 4 + 0] & mask);
			bs1.write(in[i * 4 + 1] & mask);
			bs2.write(in[i * 4 + 2] & mask);
			bs3.write(in[i * 4 + 3] & mask);
		}
	} else {
		BitWriter bs(out, bit_width);
		for (unsigned i = 0; i < num; ++i) {
			bs.write(in[i] & mask);
		}
	}
	return out + bytes_for_packed_bits(num, bit_width);
}

// See decode_for() for the format.
template<class Docid>
unsigned char *encode_for(const Docid *in, unsigned num, unsigned bit_width, bool interleaved, unsigned char *out)
{
	return encode_bitmap(in, num, bit_width, interleaved, out);
}

// See decode_pfor_bitmap() for the format.
template<class Docid>
unsigned char *encode_pfor_bitmap(const Docid *in, unsigned num, unsigned bit_width, unsigned exception_bit_width, bool interleaved, unsigned char *out)
{
	*out++ = exception_bit_width;

	// Bitmap of exceptions.
	{
		BitWriter bs(out, 1);
		for (unsigned i = 0; i < num; ++i) {
			bs.write((in[i] >> bit_width) != 0);
		}
		out += bytes_for_packed_bits(num, 1);
	}

	// Exceptions.
	{
		BitWriter bs(out, exception_bit_width);
		unsigned num_exceptions = 0;
		for (unsigned i = 0; i < num; ++i) {
			if ((in[i] >> bit_width) != 0) {
				bs.write(in[i] >> bit_width);
				++num_exceptions;
			}
		}
		out += bytes_for_packed_bits(num_exceptions, exception_bit_width);
	}

	// Base values.
	out = encode_bitmap(in, num, bit_width, interleaved, out);

	return out;
}

// See decode_pfor_vb() for the format.
template<class Docid>
unsigned char *encode_pfor_vb(const Docid *in, unsigned num, unsigned bit_width, bool interleaved, unsigned char *out)
{
	unsigned num_exceptions = 0;
	for (unsigned i = 0; i < num; ++i) {
		if ((in[i] >> bit_width) != 0) {
			++num_exceptions;
		}
	}
	*out++ = num_exceptions;

	// Base values.
	out = encode_bitmap(in, num, bit_width, interleaved, out);

	// Exceptions.
	for (unsigned i = 0; i < num; ++i) {
		unsigned val = in[i] >> bit_width;
		if (val != 0) {
			out = write_vb(val, out);
		}
	}

	// Exception indexes.
	for (unsigned i = 0; i < num; ++i) {
		unsigned val = in[i] >> bit_width;
		if (val != 0) {
			*out++ = i;
		}
	}

	return out;
}

// Find out which block type would be the smallest for the given data.
template<class Docid>
BlockType decide_block_type(const Docid *in, unsigned num, unsigned *bit_width, unsigned *exception_bit_width)
{
	// Check if the block is constant.
	bool constant = true;
	for (unsigned i = 1; i < num; ++i) {
		if (in[i] != in[0]) {
			constant = false;
			break;
		}
	}
	if (constant) {
		*bit_width = num_bits(in[0]);
		return BlockType::CONSTANT;
	}

	// Build up a histogram of bit sizes.
	unsigned histogram[sizeof(Docid) * CHAR_BIT + 1] = { 0 };
	unsigned max_bits = 0;
	for (unsigned i = 0; i < num; ++i) {
		unsigned bits = num_bits(in[i]);
		++histogram[bits];
		max_bits = std::max(max_bits, bits);
	}

	// Straight-up FOR.
	unsigned best_cost = bytes_for_packed_bits(num, max_bits);
	unsigned best_bit_width = max_bits;

	// Try PFOR with bitmap exceptions.
	const unsigned bitmap_cost = bytes_for_packed_bits(num, 1);
	unsigned num_exceptions = 0;
	for (unsigned exception_bit_width = 1; exception_bit_width <= max_bits; ++exception_bit_width) {
		unsigned test_bit_width = max_bits - exception_bit_width;
		num_exceptions += histogram[test_bit_width + 1];

		// 1 byte for signaling exception bit width, then the bitmap,
		// then the base values, then the exceptions.
		unsigned cost = 1 + bitmap_cost + bytes_for_packed_bits(num, test_bit_width) +
			bytes_for_packed_bits(num_exceptions, exception_bit_width);
		if (cost < best_cost) {
			best_cost = cost;
			best_bit_width = test_bit_width;
		}
	}

	// Try PFOR with varbyte exceptions.
	bool best_is_varbyte = false;
	for (unsigned test_bit_width = 0; test_bit_width < max_bits; ++test_bit_width) {
		// 1 byte for signaling number of exceptions, plus the base values,
		// and then we count up the varbytes and indexes. (This is precise
		// but very slow.)
		unsigned cost = 1 + bytes_for_packed_bits(num, test_bit_width);
		for (unsigned i = 0; i < num && cost < best_cost; ++i) {
			unsigned val = in[i] >> test_bit_width;
			if (val == 0) {
				// Not stored, and then also no index.
			} else if (val <= 176) {
				cost += 2;
			} else if (val <= 16560) {
				cost += 3;
			} else if (val <= 540848) {
				cost += 4;
			} else if (val <= 16777215) {
				cost += 5;
			} else {
				cost += 6;
			}
		}
		if (cost < best_cost) {
			best_cost = cost;
			best_bit_width = test_bit_width;
			best_is_varbyte = true;
		}
	}

	// TODO: Consider the last-resort option of just raw storage (255).

	if (best_is_varbyte) {
		*bit_width = best_bit_width;
		return BlockType::PFOR_VB;
	} else if (best_bit_width == max_bits) {
		*bit_width = max_bits;
		return BlockType::FOR;
	} else {
		*bit_width = best_bit_width;
		*exception_bit_width = max_bits - best_bit_width;
		return BlockType::PFOR_BITMAP;
	}
}

// The basic entry point. Takes one block of integers (which already must
// be delta-minus-1-encoded) and packs it into TurboPFor format.
// interleaved corresponds to the interleaved parameter in decode_pfor_delta1()
// or the “128v” infix in the reference code's function names; such formats
// are much faster to decode, so for full blocks, you probably want it.
// The interleaved flag isn't stored anywhere; it's implicit whether you
// want to use it for full blocks or not.
//
// The first value must already be written using write_baseval() (so the delta
// coding starts from the second value). Returns the end of the string.
// May write 4 bytes past the end.
template<unsigned BlockSize, class Docid>
unsigned char *encode_pfor_single_block(const Docid *in, unsigned num, bool interleaved, unsigned char *out)
{
	assert(num > 0);
	if (interleaved) {
		assert(num == BlockSize);
	}

	unsigned bit_width, exception_bit_width;
	BlockType block_type = decide_block_type(in, num, &bit_width, &exception_bit_width);
	*out++ = (block_type << 6) | bit_width;

	switch (block_type) {
	case BlockType::CONSTANT: {
		unsigned bit_width = num_bits(in[0]);
		write_le<Docid>(in[0], out);
		return out + div_round_up(bit_width, 8);
	}
	case BlockType::FOR:
		return encode_for(in, num, bit_width, interleaved, out);
	case BlockType::PFOR_BITMAP:
		return encode_pfor_bitmap(in, num, bit_width, exception_bit_width, interleaved, out);
	case BlockType::PFOR_VB:
		return encode_pfor_vb(in, num, bit_width, interleaved, out);
	default:
		assert(false);
	}
}

#endif  // !defined(_TURBOPFOR_ENCODE_H)

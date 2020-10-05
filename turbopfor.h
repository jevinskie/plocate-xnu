#ifndef _TURBOPFOR_H
#define _TURBOPFOR_H 1

// A reimplementation of parts of the TurboPFor codecs, using the same
// storage format. These are not as fast as the reference implementation
// (about 60% of the performance, averaged over a real plocate corpus),
// and do not support the same breadth of codecs (in particular, only
// delta-plus-1 is implemented, and only 32-bit docids are tested),
// but aim to be more portable and (ideally) easier-to-understand.
// In particular, they will compile on x86 without SSE4.1 or AVX support.
//
// The main reference is https://michael.stapelberg.ch/posts/2019-02-05-turbopfor-analysis/,
// although some implementation details have been worked out by studying the
// TurboPFor code.

#include <assert.h>
#include <endian.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>

#include <algorithm>

#if defined(__i386__) || defined(__x86_64__)
#define COULD_HAVE_SSE2
#include <immintrin.h>
#endif

// Forward declarations to declare to the template code below that they exist.
// (These must seemingly be non-templates for function multiversioning to work.)
__attribute__((target("default")))
const unsigned char *decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out);
__attribute__((target("default")))
const unsigned char *decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out);
__attribute__((target("default")))
const unsigned char *decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out);

#ifdef COULD_HAVE_SSE2
__attribute__((target("sse2")))
const unsigned char *decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out);
__attribute__((target("sse2")))
const unsigned char *decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out);
__attribute__((target("sse2")))
const unsigned char *decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out);
#endif

template<class Docid>
Docid read_le(const void *in)
{
	Docid val;
	memcpy(&val, in, sizeof(val));
	if constexpr (sizeof(Docid) == 8) {
		return le64toh(val);
	} else if constexpr (sizeof(Docid) == 4) {
		return le32toh(val);
	} else if constexpr (sizeof(Docid) == 2) {
		return le16toh(val);
	} else if constexpr (sizeof(Docid) == 1) {
		return val;
	} else {
		assert(false);
	}
}

// Reads a single value with an encoding that looks a bit like PrefixVarint.
// It's unclear why this doesn't use the varbyte encoding.
template<class Docid>
const unsigned char *read_baseval(const unsigned char *in, Docid *out)
{
	//fprintf(stderr, "baseval: 0x%02x 0x%02x 0x%02x 0x%02x\n", in[0], in[1], in[2], in[3]);
	if (*in < 128) {
		*out = *in;
		return in + 1;
	} else if (*in < 192) {
		*out = ((uint32_t(in[0]) << 8) | uint32_t(in[1])) & 0x3fff;
		return in + 2;
	} else if (*in < 224) {
		*out = ((uint32_t(in[0]) << 16) |
		        (uint32_t(in[2]) << 8) |
		        (uint32_t(in[1]))) & 0x1fffff;
		return in + 3;
	} else {
		assert(false);  // Not implemented.
	}
}

template<class Docid>
const unsigned char *read_vb(const unsigned char *in, Docid *out)
{
	if (*in <= 176) {
		*out = *in;
		return in + 1;
	} else if (*in <= 240) {
		*out = ((uint32_t(in[0] - 177) << 8) | uint32_t(in[1])) + 177;
		return in + 2;
	} else if (*in <= 248) {
		*out = ((uint32_t(in[0] - 241) << 16) | read_le<uint16_t>(in + 1)) + 16561;
		return in + 3;
	} else if (*in == 249) {
		*out = (uint32_t(in[1])) |
			(uint32_t(in[2]) << 8) |
			(uint32_t(in[3]) << 16);
		return in + 4;
	} else if (*in == 250) {
		*out = read_le<uint32_t>(in + 1);
		return in + 5;
	} else {
		assert(false);
	}
}

struct BitReader {
public:
	BitReader(const unsigned char *in, unsigned bits)
		: in(in), bits(bits), mask((1U << bits) - 1) {}
	uint32_t read()
	{
		uint32_t val = (read_le<uint32_t>(in) >> bits_used) & mask;

		bits_used += bits;
		in += bits_used / 8;
		bits_used %= 8;

		return val;
	}

private:
	const unsigned char *in;
	const unsigned bits;
	const unsigned mask;
	unsigned bits_used = 0;
};

template<unsigned NumStreams>
struct InterleavedBitReader {
public:
	InterleavedBitReader(const unsigned char *in, unsigned bits)
		: in(in), bits(bits), mask((1U << bits) - 1) {}
	uint32_t read()
	{
		uint32_t val;
		if (bits_used + bits > 32) {
			val = (read_le<uint32_t>(in) >> bits_used) | (read_le<uint32_t>(in + Stride) << (32 - bits_used));
		} else {
			val = (read_le<uint32_t>(in) >> bits_used);
		}

		bits_used += bits;
		in += Stride * (bits_used / 32);
		bits_used %= 32;

		return val & mask;
	}

private:
	static constexpr unsigned Stride = NumStreams * sizeof(uint32_t);
	const unsigned char *in;
	const unsigned bits;
	const unsigned mask;
	unsigned bits_used = 0;
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

// Constant block. Layout:
//
//  - Bit width (6 bits) | type << 6
//  - Base values (<bits> bits, rounded up to nearest byte)
template<class Docid>
const unsigned char *decode_constant(const unsigned char *in, unsigned num, Docid *out)
{
	const unsigned bit_width = *in++ & 0x3f;
	Docid val = read_le<Docid>(in);
	if (bit_width < sizeof(Docid) * 8) {
		val &= ((1U << bit_width) - 1);
	}

	Docid prev_val = out[-1];
	for (unsigned i = 0; i < num; ++i) {
		out[i] = prev_val = val + prev_val + 1;
	}
	return in + div_round_up(bit_width, 8);
}

// FOR block (ie., PFor without exceptions). Layout:
//
//  - Bit width (6 bits) | type << 6
//  - Base values (<num> values of <bits> bits, rounded up to a multiple of 32 values)
template<class Docid>
const unsigned char *decode_for(const unsigned char *in, unsigned num, Docid *out)
{
	const unsigned bit_width = *in++ & 0x3f;

	Docid prev_val = out[-1];
	BitReader bs(in, bit_width);
	for (unsigned i = 0; i < num; ++i) {
		prev_val = out[i] = bs.read() + prev_val + 1;
	}
	return in + bytes_for_packed_bits(num, bit_width);
}

#ifdef COULD_HAVE_SSE2
template<unsigned BlockSize>
__attribute__((target("sse2")))
inline void delta_decode_sse2(uint32_t *out)
{
	// Use 4/3/2/1 as delta instead of fixed 1, so that we can do the prev_val + delta
	// in parallel with something else.
	const __m128i delta = _mm_set_epi32(4, 3, 2, 1);
	__m128i prev_val = _mm_set1_epi32(out[-1]);
	__m128i *outvec = reinterpret_cast<__m128i *>(out);
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		__m128i val = _mm_loadu_si128(outvec + i);
		val = _mm_add_epi32(val, _mm_slli_si128(val, 4));
		val = _mm_add_epi32(val, _mm_slli_si128(val, 8));
		val = _mm_add_epi32(val, _mm_add_epi32(prev_val, delta));
		_mm_storeu_si128(outvec + i, val);

		prev_val = _mm_shuffle_epi32(val, _MM_SHUFFLE(3, 3, 3, 3));
	}
}

template<unsigned BlockSize, bool OrWithExisting>
__attribute__((target("sse2")))
const unsigned char *decode_bitmap_sse2(const unsigned char *in, unsigned bit_width, uint32_t *out)
{
	const __m128i *invec = reinterpret_cast<const __m128i *>(in);
	__m128i *outvec = reinterpret_cast<__m128i *>(out);
	const __m128i mask = _mm_set1_epi32((1U << bit_width) - 1);
	unsigned bits_used = 0;
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		__m128i val = _mm_srli_epi32(_mm_loadu_si128(invec), bits_used);
		if (bits_used + bit_width > 32) {
			__m128i val_upper = _mm_slli_epi32(_mm_loadu_si128(invec + 1), 32 - bits_used);
			val = _mm_or_si128(val, val_upper);
		}
		val = _mm_and_si128(val, mask);
		if constexpr (OrWithExisting) {
			val = _mm_or_si128(val, _mm_loadu_si128(outvec + i));
		}
		_mm_storeu_si128(outvec + i, val);

		bits_used += bit_width;
		invec += bits_used / 32;
		bits_used %= 32;
	}
	in += bytes_for_packed_bits(BlockSize, bit_width);
	return in;
}
#endif

// Like decode_for(), but the values are organized in four independent streams,
// for SIMD (presumably SSE2). Supports a whole block only.
template<unsigned BlockSize, class Docid>
const unsigned char *decode_for_interleaved_generic(const unsigned char *in, Docid *out)
{
	const unsigned bit_width = *in++ & 0x3f;

	InterleavedBitReader<4> bs0(in + 0 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs1(in + 1 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs2(in + 2 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs3(in + 3 * sizeof(uint32_t), bit_width);
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		out[i * 4 + 0] = bs0.read();
		out[i * 4 + 1] = bs1.read();
		out[i * 4 + 2] = bs2.read();
		out[i * 4 + 3] = bs3.read();
	}
	Docid prev_val = out[-1];
	for (unsigned i = 0; i < BlockSize; ++i) {
		out[i] = prev_val = out[i] + prev_val + 1;
	}
	return in + bytes_for_packed_bits(BlockSize, bit_width);
}

template<unsigned BlockSize, class Docid>
const unsigned char *decode_for_interleaved(const unsigned char *in, Docid *out)
{
	if constexpr (BlockSize == 128 && sizeof(Docid) == sizeof(uint32_t)) {
		return decode_for_interleaved_128_32(in, out);
	} else {
		return decode_for_interleaved_generic(in, out);
	}
}

__attribute__((target("default")))
const unsigned char *decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	return decode_for_interleaved_generic<128>(in, out);
}

#ifdef COULD_HAVE_SSE2
// Specialized version for SSE2.
__attribute__((target("sse2")))
const unsigned char *decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	constexpr unsigned BlockSize = 128;

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_bitmap_sse2<BlockSize, false>(in, bit_width, out);
	delta_decode_sse2<BlockSize>(out);

	return in;
}
#endif

template<class Docid>
const unsigned char *decode_pfor_bitmap_exceptions(const unsigned char *in, unsigned num, unsigned bit_width, Docid *out)
{
	const unsigned exception_bit_width = *in++;
	const uint64_t *exception_bitmap_ptr = reinterpret_cast<const uint64_t *>(in);
	in += div_round_up(num, 8);

	int num_exceptions = 0;

	BitReader bs(in, exception_bit_width);
	for (unsigned i = 0; i < num; i += 64, ++exception_bitmap_ptr) {
		uint64_t exceptions = read_le<uint64_t>(exception_bitmap_ptr);
		if (num - i < 64) {
			// We've read some bytes past the end, so clear out the junk bits.
			exceptions &= (1ULL << (num - i)) - 1;
		}
		for (; exceptions != 0; exceptions &= exceptions - 1, ++num_exceptions) {
			unsigned idx = (ffsll(exceptions) - 1) + i;
			out[idx] = bs.read() << bit_width;
		}
	}
	in += bytes_for_packed_bits(num_exceptions, exception_bit_width);
	return in;
}

// PFor block with bitmap exceptions. Layout:
//
//  - Bit width (6 bits) | type << 6
//  - Exception bit width (8 bits)
//  - Bitmap of which values have exceptions (<num> bits, rounded up to a byte)
//  - Exceptions (<num_exc> values of <bits_exc> bits, rounded up to a byte)
//  - Base values (<num> values of <bits> bits, rounded up to a byte)
template<class Docid>
const unsigned char *decode_pfor_bitmap(const unsigned char *in, unsigned num, Docid *out)
{
	memset(out, 0, num * sizeof(Docid));

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_pfor_bitmap_exceptions(in, num, bit_width, out);

	// Decode the base values, and delta-decode.
	Docid prev_val = out[-1];
	BitReader bs(in, bit_width);
	for (unsigned i = 0; i < num; ++i) {
		out[i] = prev_val = (out[i] | bs.read()) + prev_val + 1;
	}
	return in + bytes_for_packed_bits(num, bit_width);
}

// Like decode_pfor_bitmap(), but the base values are organized in four
// independent streams, for SIMD (presumably SSE2). Supports a whole block only.
template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_bitmap_interleaved_generic(const unsigned char *in, Docid *out)
{
	memset(out, 0, BlockSize * sizeof(Docid));

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_pfor_bitmap_exceptions(in, BlockSize, bit_width, out);

	// Decode the base values.
	InterleavedBitReader<4> bs0(in + 0 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs1(in + 1 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs2(in + 2 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs3(in + 3 * sizeof(uint32_t), bit_width);
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		out[i * 4 + 0] |= bs0.read();
		out[i * 4 + 1] |= bs1.read();
		out[i * 4 + 2] |= bs2.read();
		out[i * 4 + 3] |= bs3.read();
	}

	// Delta-decode.
	Docid prev_val = out[-1];
	for (unsigned i = 0; i < BlockSize; ++i) {
		out[i] = prev_val = out[i] + prev_val + 1;
	}
	return in + bytes_for_packed_bits(BlockSize, bit_width);
}

template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_bitmap_interleaved(const unsigned char *in, Docid *out)
{
	if constexpr (BlockSize == 128 && sizeof(Docid) == sizeof(uint32_t)) {
		return decode_pfor_bitmap_interleaved_128_32(in, out);
	} else {
		return decode_pfor_bitmap_interleaved_generic(in, out);
	}
}

__attribute__((target("default")))
const unsigned char *decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	return decode_pfor_bitmap_interleaved_generic<128>(in, out);
}

#ifdef COULD_HAVE_SSE2
// Specialized version for SSE2.
__attribute__((target("sse2")))
const unsigned char *decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	constexpr unsigned BlockSize = 128;
	using Docid = uint32_t;

	memset(out, 0, BlockSize * sizeof(Docid));

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_pfor_bitmap_exceptions(in, BlockSize, bit_width, out);
	in = decode_bitmap_sse2<BlockSize, true>(in, bit_width, out);
	delta_decode_sse2<BlockSize>(out);

	return in;
}
#endif

// PFor block with variable-byte exceptions. Layout:
//
//  - Bit width (6 bits) | type << 6
//  - Number of exceptions (8 bits)
//  - Base values (<num> values of <bits> bits, rounded up to a byte)
//  - Exceptions:
//    - If first byte is 255, <num_exc> 32-bit values (does not include the 255 byte)
//    - Else, <num_exc> varbyte-encoded values (includes the non-255 byte)
//  - Indexes of exceptions (<num_exc> bytes).
template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_vb(const unsigned char *in, unsigned num, Docid *out)
{
	//fprintf(stderr, "in=%p out=%p num=%u\n", in, out, num);

	const unsigned bit_width = *in++ & 0x3f;
	unsigned num_exceptions = *in++;

	// Decode the base values.
	BitReader bs(in, bit_width);
	for (unsigned i = 0; i < num; ++i) {
		out[i] = bs.read();
	}
	in += bytes_for_packed_bits(num, bit_width);

	// Decode exceptions.
	Docid exceptions[BlockSize];
	if (*in == 255) {
		++in;
		for (unsigned i = 0; i < num_exceptions; ++i) {
			exceptions[i] = read_le<Docid>(in);
			in += sizeof(Docid);
		}
	} else {
		for (unsigned i = 0; i < num_exceptions; ++i) {
			in = read_vb(in, &exceptions[i]);
		}
	}
	// Apply exceptions.
	for (unsigned i = 0; i < num_exceptions; ++i) {
		unsigned idx = *in++;
		out[idx] |= exceptions[i] << bit_width;
	}

	// Delta-decode.
	Docid prev_val = out[-1];
	for (unsigned i = 0; i < num; ++i) {
		out[i] = prev_val = out[i] + prev_val + 1;
	}

	return in;
}

// Like decode_pfor_vb(), but the base values are organized in four
// independent streams, for SIMD (presumably SSE2). Supports a whole block only.
template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_vb_interleaved_generic(const unsigned char *in, Docid *out)
{
	const unsigned bit_width = *in++ & 0x3f;
	unsigned num_exceptions = *in++;

	// Decode the base values.
	InterleavedBitReader<4> bs0(in + 0 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs1(in + 1 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs2(in + 2 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs3(in + 3 * sizeof(uint32_t), bit_width);
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		out[i * 4 + 0] = bs0.read();
		out[i * 4 + 1] = bs1.read();
		out[i * 4 + 2] = bs2.read();
		out[i * 4 + 3] = bs3.read();
	}
	in += bytes_for_packed_bits(BlockSize, bit_width);

	// Decode exceptions.
	Docid exceptions[BlockSize];
	if (*in == 255) {
		++in;
		for (unsigned i = 0; i < num_exceptions; ++i) {
			exceptions[i] = read_le<Docid>(in);
			in += sizeof(Docid);
		}
	} else {
		for (unsigned i = 0; i < num_exceptions; ++i) {
			in = read_vb(in, &exceptions[i]);
		}
	}

	// Apply exceptions.
	for (unsigned i = 0; i < num_exceptions; ++i) {
		unsigned idx = *in++;
		out[idx] |= exceptions[i] << bit_width;
	}

	// Delta-decode.
	Docid prev_val = out[-1];
	for (unsigned i = 0; i < BlockSize; ++i) {
		out[i] = prev_val = out[i] + prev_val + 1;
	}

	return in;
}

template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_vb_interleaved(const unsigned char *in, Docid *out)
{
	if constexpr (BlockSize == 128 && sizeof(Docid) == sizeof(uint32_t)) {
		return decode_pfor_vb_interleaved_128_32(in, out);
	} else {
		return decode_pfor_vb_interleaved_generic(in, out);
	}
}

__attribute__((target("default")))
const unsigned char *decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	return decode_pfor_vb_interleaved_generic<128>(in, out);
}

// Specialized version for SSE2.
__attribute__((target("sse2")))
const unsigned char *decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	constexpr unsigned BlockSize = 128;
	using Docid = uint32_t;

	const unsigned bit_width = *in++ & 0x3f;
	unsigned num_exceptions = *in++;

	// Decode the base values.
	in = decode_bitmap_sse2<BlockSize, false>(in, bit_width, out);

	// Decode exceptions.
	Docid exceptions[BlockSize];
	if (*in == 255) {
		++in;
		for (unsigned i = 0; i < num_exceptions; ++i) {
			exceptions[i] = read_le<Docid>(in);
			in += sizeof(Docid);
		}
	} else {
		for (unsigned i = 0; i < num_exceptions; ++i) {
			in = read_vb(in, &exceptions[i]);
		}
	}

	// Apply exceptions.
	for (unsigned i = 0; i < num_exceptions; ++i) {
		unsigned idx = *in++;
		out[idx] |= exceptions[i] << bit_width;
	}

	delta_decode_sse2<BlockSize>(out);

	return in;
}

enum BlockType {
	FOR = 0,
	PFOR_VB = 1,
	PFOR_BITMAP = 2,
	CONSTANT = 3
};

template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_delta1(const unsigned char *in, unsigned num, bool interleaved, Docid *out)
{
	if (num == 0) {
		return in;
	}
	in = read_baseval(in, out++);

	for (unsigned i = 1; i < num; i += BlockSize, out += BlockSize) {
		const unsigned num_this_block = std::min<unsigned>(num - i, BlockSize);
		switch (in[0] >> 6) {
		case BlockType::FOR:
			if (interleaved && num_this_block == BlockSize) {
				dprintf("%d+%d: blocktype=%d (for, interleaved), bitwidth=%d\n", i, num_this_block, in[0] >> 6, in[0] & 0x3f);
				in = decode_for_interleaved<BlockSize>(in, out);
			} else {
				dprintf("%d+%d: blocktype=%d (for), bitwidth=%d\n", i, num_this_block, in[0] >> 6, in[0] & 0x3f);
				in = decode_for(in, num_this_block, out);
			}
			break;
		case BlockType::PFOR_VB:
			if (interleaved && num_this_block == BlockSize) {
				dprintf("%d+%d: blocktype=%d (pfor + vb, interleaved), bitwidth=%d\n", i, num_this_block, in[0] >> 6, in[0] & 0x3f);
				in = decode_pfor_vb_interleaved<BlockSize>(in, out);
			} else {
				dprintf("%d+%d: blocktype=%d (pfor + vb), bitwidth=%d\n", i, num_this_block, in[0] >> 6, in[0] & 0x3f);
				in = decode_pfor_vb<BlockSize>(in, num_this_block, out);
			}
			break;
		case BlockType::PFOR_BITMAP:
			if (interleaved && num_this_block == BlockSize) {
				dprintf("%d+%d: blocktype=%d (pfor + bitmap, interleaved), bitwidth=%d\n", i, num_this_block, in[0] >> 6, in[0] & 0x3f);
				in = decode_pfor_bitmap_interleaved<BlockSize>(in, out);
			} else {
				dprintf("%d+%d: blocktype=%d (pfor + bitmap), bitwidth=%d\n", i, num_this_block, in[0] >> 6, in[0] & 0x3f);
				in = decode_pfor_bitmap(in, num_this_block, out);
			}
			break;
		case BlockType::CONSTANT:
			dprintf("%d+%d: blocktype=%d (constant), bitwidth=%d\n", i, num_this_block, in[0] >> 6, in[0] & 0x3f);
			in = decode_constant(in, num_this_block, out);
			break;
		}
	}

	return in;
}

#endif  // !defined(_TURBOPFOR_H)

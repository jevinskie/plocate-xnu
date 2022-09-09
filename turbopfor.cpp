#include <algorithm>
#include <assert.h>
#ifdef HAS_ENDIAN_H
#include <endian.h>
#else
#include "endian_compat.h"
#endif
#include <stdint.h>
#include <string.h>
#include <strings.h>

// This is a mess. :-/ Maybe it would be good just to drop support for
// multiversioning; the only platform it really helps is 32-bit x86.
// This may change if we decide to use AVX or similar in the future, though.
#if defined(__i386__) || defined(__x86_64__)
#ifdef __SSE2__
#define COULD_HAVE_SSE2
#define SUPPRESS_DEFAULT
#include <immintrin.h>
#define TARGET_SSE2
#elif defined(HAS_FUNCTION_MULTIVERSIONING)
#define COULD_HAVE_SSE2
#include <immintrin.h>
#define TARGET_SSE2 __attribute__((target("sse2")))
#define TARGET_DEFAULT __attribute__((target("default")))
#else
#define TARGET_DEFAULT
#endif
#else
// Function multiversioning is x86-only.
#define TARGET_DEFAULT
#endif

#include "turbopfor-common.h"

#define dprintf(...)
//#define dprintf(...) fprintf(stderr, __VA_ARGS__);

#ifndef SUPPRESS_DEFAULT
// Forward declarations to declare to the template code below that they exist.
// (These must seemingly be non-templates for function multiversioning to work.)
TARGET_DEFAULT
const unsigned char *
decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out);
TARGET_DEFAULT
const unsigned char *
decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out);
TARGET_DEFAULT
const unsigned char *
decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out);
#endif

#ifdef COULD_HAVE_SSE2
TARGET_SSE2
const unsigned char *
decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out);
TARGET_SSE2
const unsigned char *
decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out);
TARGET_SSE2
const unsigned char *
decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out);
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
	} else if (*in < 240) {
		*out = ((uint32_t(in[0]) << 24) |
		        (uint32_t(in[1]) << 16) |
		        (uint32_t(in[2]) << 8) |
		        (uint32_t(in[3]))) & 0xfffffff;
		return in + 4;
	} else {
		assert(false);  // Not implemented.
	}
}

// Does not read past the end of the input.
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
		: in(in), bits(bits), mask(mask_for_bits(bits)) {}

	// Can read 4 bytes past the end of the input (if bits_used == 0).
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
		: in(in), bits(bits), mask(mask_for_bits(bits)) {}

	// Can read 4 bytes past the end of the input (if bit_width == 0).
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

#ifdef COULD_HAVE_SSE2
struct InterleavedBitReaderSSE2 {
public:
	TARGET_SSE2
	InterleavedBitReaderSSE2(const unsigned char *in, unsigned bits)
		: in(reinterpret_cast<const __m128i *>(in)), bits(bits), mask(_mm_set1_epi32(mask_for_bits(bits))) {}

	// Can read 16 bytes past the end of the input (if bit_width == 0).
	TARGET_SSE2
	__m128i
	read()
	{
		__m128i val = _mm_srli_epi32(_mm_loadu_si128(in), bits_used);
		if (bits_used + bits > 32) {
			__m128i val_upper = _mm_slli_epi32(_mm_loadu_si128(in + 1), 32 - bits_used);
			val = _mm_or_si128(val, val_upper);
		}
		val = _mm_and_si128(val, mask);

		bits_used += bits;
		in += bits_used / 32;
		bits_used %= 32;
		return val;
	}

private:
	const __m128i *in;
	const unsigned bits;
	const __m128i mask;
	unsigned bits_used = 0;
};
#endif

// Constant block. Layout:
//
//  - Bit width (6 bits) | type << 6
//  - Base values (<bits> bits, rounded up to nearest byte)
//
// Can read 4 bytes past the end of the input (if bit_width == 0).
template<class Docid>
const unsigned char *decode_constant(const unsigned char *in, unsigned num, Docid *out)
{
	const unsigned bit_width = *in++ & 0x3f;
	Docid val = read_le<Docid>(in);
	if (bit_width < sizeof(Docid) * 8) {
		val &= mask_for_bits(bit_width);
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
//
// Can read 4 bytes past the end of the input (inherit from BitReader).
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
class DeltaDecoderSSE2 {
public:
	TARGET_SSE2
	DeltaDecoderSSE2(uint32_t prev_val)
		: prev_val(_mm_set1_epi32(prev_val)) {}

	TARGET_SSE2
	__m128i
	decode(__m128i val)
	{
		val = _mm_add_epi32(val, _mm_slli_si128(val, 4));
		val = _mm_add_epi32(val, _mm_slli_si128(val, 8));
		val = _mm_add_epi32(val, _mm_add_epi32(prev_val, delta));
		prev_val = _mm_shuffle_epi32(val, _MM_SHUFFLE(3, 3, 3, 3));
		return val;
	}

private:
	// Use 4/3/2/1 as delta instead of fixed 1, so that we can do the prev_val + delta
	// in parallel with something else.
	const __m128i delta = _mm_set_epi32(4, 3, 2, 1);

	__m128i prev_val;
};

template<unsigned BlockSize>
TARGET_SSE2 inline void delta_decode_sse2(uint32_t *out)
{
	DeltaDecoderSSE2 delta(out[-1]);
	__m128i *outvec = reinterpret_cast<__m128i *>(out);
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		__m128i val = _mm_loadu_si128(outvec + i);
		_mm_storeu_si128(outvec + i, delta.decode(val));
	}
}

// Can read 16 bytes past the end of its input (inherit from InterleavedBitReaderSSE2).
template<unsigned BlockSize, bool OrWithExisting, bool DeltaDecode, unsigned bit_width>
TARGET_SSE2 const unsigned char *
decode_bitmap_sse2_unrolled(const unsigned char *in, uint32_t *out)
{
	__m128i *outvec = reinterpret_cast<__m128i *>(out);
	DeltaDecoderSSE2 delta(out[-1]);
	InterleavedBitReaderSSE2 bs(in, bit_width);
#pragma GCC unroll 32
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		__m128i val = bs.read();
		if constexpr (OrWithExisting) {
			val = _mm_or_si128(val, _mm_slli_epi32(_mm_loadu_si128(outvec + i), bit_width));
		}
		if constexpr (DeltaDecode) {
			val = delta.decode(val);
		}
		_mm_storeu_si128(outvec + i, val);
	}
	in += bytes_for_packed_bits(BlockSize, bit_width);
	return in;
}

// Can read 16 bytes past the end of its input (inherit from InterleavedBitReaderSSE2).
template<unsigned BlockSize, bool OrWithExisting, bool DeltaDecode>
TARGET_SSE2 const unsigned char *
decode_bitmap_sse2(const unsigned char *in, unsigned bit_width, uint32_t *out)
{
	switch (bit_width) {
	case 0:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 0>(in, out);
	case 1:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 1>(in, out);
	case 2:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 2>(in, out);
	case 3:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 3>(in, out);
	case 4:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 4>(in, out);
	case 5:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 5>(in, out);
	case 6:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 6>(in, out);
	case 7:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 7>(in, out);
	case 8:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 8>(in, out);
	case 9:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 9>(in, out);
	case 10:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 10>(in, out);
	case 11:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 11>(in, out);
	case 12:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 12>(in, out);
	case 13:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 13>(in, out);
	case 14:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 14>(in, out);
	case 15:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 15>(in, out);
	case 16:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 16>(in, out);
	case 17:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 17>(in, out);
	case 18:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 18>(in, out);
	case 19:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 19>(in, out);
	case 20:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 20>(in, out);
	case 21:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 21>(in, out);
	case 22:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 22>(in, out);
	case 23:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 23>(in, out);
	case 24:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 24>(in, out);
	case 25:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 25>(in, out);
	case 26:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 26>(in, out);
	case 27:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 27>(in, out);
	case 28:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 28>(in, out);
	case 29:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 29>(in, out);
	case 30:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 30>(in, out);
	case 31:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 31>(in, out);
	case 32:
		return decode_bitmap_sse2_unrolled<BlockSize, OrWithExisting, DeltaDecode, 32>(in, out);
	}
	assert(false);
}
#endif

// Like decode_for(), but the values are organized in four independent streams,
// for SIMD (presumably SSE2). Supports a whole block only.
//
// Can read 16 bytes past the end of its input (inherit from InterleavedBitReader).
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

// Does not read past the end of the input.
template<unsigned BlockSize, class Docid>
const unsigned char *decode_for_interleaved(const unsigned char *in, Docid *out)
{
	if constexpr (BlockSize == 128 && sizeof(Docid) == sizeof(uint32_t)) {
		return decode_for_interleaved_128_32(in, out);
	} else {
		return decode_for_interleaved_generic(in, out);
	}
}

#ifndef SUPPRESS_DEFAULT
// Does not read past the end of the input.
TARGET_DEFAULT
const unsigned char *
decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	return decode_for_interleaved_generic<128>(in, out);
}
#endif

#ifdef COULD_HAVE_SSE2
// Specialized version for SSE2.
// Can read 16 bytes past the end of the input (inherit from decode_bitmap_sse2()).
TARGET_SSE2
const unsigned char *
decode_for_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	constexpr unsigned BlockSize = 128;

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_bitmap_sse2<BlockSize, /*OrWithExisting=*/false, /*DeltaDecode=*/true>(in, bit_width, out);

	return in;
}
#endif

// Can read 4 bytes past the end of the input (inherit from BitReader).
template<class Docid>
const unsigned char *decode_pfor_bitmap_exceptions(const unsigned char *in, unsigned num, Docid *out)
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
			out[idx] = bs.read();
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
//
// Can read 4 bytes past the end of the input (inherit from BitReader).
template<class Docid>
const unsigned char *decode_pfor_bitmap(const unsigned char *in, unsigned num, Docid *out)
{
	memset(out, 0, num * sizeof(Docid));

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_pfor_bitmap_exceptions(in, num, out);

	// Decode the base values, and delta-decode.
	Docid prev_val = out[-1];
	BitReader bs(in, bit_width);
	for (unsigned i = 0; i < num; ++i) {
		out[i] = prev_val = ((out[i] << bit_width) | bs.read()) + prev_val + 1;
	}
	return in + bytes_for_packed_bits(num, bit_width);
}

// Like decode_pfor_bitmap(), but the base values are organized in four
// independent streams, for SIMD (presumably SSE2). Supports a whole block only.
//
// Can read 16 bytes past the end of the input (inherit from InterleavedBitReader
// and decode_pfor_bitmap_exceptions()).
template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_bitmap_interleaved_generic(const unsigned char *in, Docid *out)
{
	memset(out, 0, BlockSize * sizeof(Docid));

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_pfor_bitmap_exceptions(in, BlockSize, out);

	// Decode the base values.
	InterleavedBitReader<4> bs0(in + 0 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs1(in + 1 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs2(in + 2 * sizeof(uint32_t), bit_width);
	InterleavedBitReader<4> bs3(in + 3 * sizeof(uint32_t), bit_width);
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		out[i * 4 + 0] = bs0.read() | (out[i * 4 + 0] << bit_width);
		out[i * 4 + 1] = bs1.read() | (out[i * 4 + 1] << bit_width);
		out[i * 4 + 2] = bs2.read() | (out[i * 4 + 2] << bit_width);
		out[i * 4 + 3] = bs3.read() | (out[i * 4 + 3] << bit_width);
	}

	// Delta-decode.
	Docid prev_val = out[-1];
	for (unsigned i = 0; i < BlockSize; ++i) {
		out[i] = prev_val = out[i] + prev_val + 1;
	}
	return in + bytes_for_packed_bits(BlockSize, bit_width);
}

// Can read 16 bytes past the end of the input (inherit from decode_pfor_bitmap_interleaved_generic()).
template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_bitmap_interleaved(const unsigned char *in, Docid *out)
{
	if constexpr (BlockSize == 128 && sizeof(Docid) == sizeof(uint32_t)) {
		return decode_pfor_bitmap_interleaved_128_32(in, out);
	} else {
		return decode_pfor_bitmap_interleaved_generic(in, out);
	}
}

#ifndef SUPPRESS_DEFAULT
TARGET_DEFAULT
const unsigned char *
decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	return decode_pfor_bitmap_interleaved_generic<128>(in, out);
}
#endif

#ifdef COULD_HAVE_SSE2
// Specialized version for SSE2.
//
// Can read 16 bytes past the end of the input (inherit from InterleavedBitReaderSSE2
// and decode_pfor_bitmap_exceptions()).
TARGET_SSE2
const unsigned char *
decode_pfor_bitmap_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	constexpr unsigned BlockSize = 128;

// Set all output values to zero, before the exceptions are filled in.
#pragma GCC unroll 4
	for (unsigned i = 0; i < BlockSize / 4; ++i) {
		_mm_storeu_si128(reinterpret_cast<__m128i *>(out) + i, _mm_setzero_si128());
	}

	const unsigned bit_width = *in++ & 0x3f;

	in = decode_pfor_bitmap_exceptions(in, BlockSize, out);
	in = decode_bitmap_sse2<BlockSize, /*OrWithExisting=*/true, /*DeltaDecode=*/true>(in, bit_width, out);

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
//
// Can read 4 bytes past the end of the input (inherit from BitReader,
// assuming zero exceptions).
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
// Can read 16 bytes past the end of its input (inherit from InterleavedBitReader).
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

// Can read 16 bytes past the end of its input (inherit from decode_pfor_vb_interleaved_generic()).
template<unsigned BlockSize, class Docid>
const unsigned char *decode_pfor_vb_interleaved(const unsigned char *in, Docid *out)
{
	if constexpr (BlockSize == 128 && sizeof(Docid) == sizeof(uint32_t)) {
		return decode_pfor_vb_interleaved_128_32(in, out);
	} else {
		return decode_pfor_vb_interleaved_generic(in, out);
	}
}

#ifndef SUPPRESS_DEFAULT
TARGET_DEFAULT
const unsigned char *
decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	return decode_pfor_vb_interleaved_generic<128>(in, out);
}
#endif

#ifdef COULD_HAVE_SSE2
// Specialized version for SSE2.
// Can read 16 bytes past the end of the input (inherit from decode_bitmap_sse2()).
TARGET_SSE2
const unsigned char *
decode_pfor_vb_interleaved_128_32(const unsigned char *in, uint32_t *out)
{
	constexpr unsigned BlockSize = 128;
	using Docid = uint32_t;

	const unsigned bit_width = *in++ & 0x3f;
	unsigned num_exceptions = *in++;

	// Decode the base values.
	in = decode_bitmap_sse2<BlockSize, /*OrWithExisting=*/false, /*DeltaDecode=*/false>(in, bit_width, out);

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
#endif

// Can read 16 bytes past the end of the input (inherit from several functions).
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

const unsigned char *decode_pfor_delta1_128(const unsigned char *in, unsigned num, bool interleaved, uint32_t *out)
{
	return decode_pfor_delta1<128>(in, num, interleaved, out);
}

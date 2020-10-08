#ifndef _TURBOPFOR_H
#define _TURBOPFOR_H 1

// A reimplementation of parts of the TurboPFor codecs, using the same
// storage format. These are not as fast as the reference implementation
// (about 80% of the performance, averaged over a real plocate corpus),
// and do not support the same breadth of codecs (in particular, only
// delta-plus-1 is implemented, and only 32-bit docids are tested),
// but aim to be more portable and (ideally) easier-to-understand.
// In particular, they will compile on x86 without SSE4.1 or AVX support.
// Unlike the reference code, only GCC and GCC-compatible compilers
// (e.g. Clang) are supported.
//
// The main reference is https://michael.stapelberg.ch/posts/2019-02-05-turbopfor-analysis/,
// although some implementation details have been worked out by studying the
// TurboPFor code.
//
// The decoder, like the reference implementation, is not robust against
// malicious of corrupted. Several functions (again like the reference
// implementation) can read N bytes past the end, so you need to have some slop
// in the input buffers; this is documented for each function (unlike
// the reference implementation), but the documented slop assumes a
// non-malicious encoder.
//
// Although all of these algorithms are templatized, we expose only
// the single specialization that we need, in order to increase the
// speed of incremental compilation.

const unsigned char *decode_pfor_delta1_128(const unsigned char *in, unsigned num, bool interleaved, uint32_t *out);

#endif  // !defined(_TURBOPFOR_H)

#ifndef _DPRINTF_H
#define _DPRINTF_H 1

#include <stdio.h>

extern bool use_debug;

// Debug printf.
#define dprintf(...) \
	do { \
		if (use_debug) { \
			fprintf(stderr, __VA_ARGS__); \
		} \
	} while (false)

#endif  // !defined(_DPRINTF_H)

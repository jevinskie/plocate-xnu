#ifndef DB_H
#define DB_H 1

#include <stdint.h>

struct Header {
	char magic[8];  // "\0plocate";
	uint32_t version;  // 0.
	uint32_t hashtable_size;
	uint32_t extra_ht_slots;
	uint32_t pad;   // Unused.
	uint64_t hash_table_offset_bytes;
	uint64_t filename_index_offset_bytes;
};

struct Trigram {
	uint32_t trgm;
	uint32_t num_docids;
	uint64_t offset;

	bool operator==(const Trigram &other) const
	{
		return trgm == other.trgm;
	}
	bool operator<(const Trigram &other) const
	{
		return trgm < other.trgm;
	}
};

inline uint32_t hash_trigram(uint32_t trgm, uint32_t ht_size)
{
	// CRC-like computation.
	uint32_t crc = trgm;
	for (int i = 0; i < 32; i++) {
		bool bit = crc & 0x80000000;
		crc <<= 1;
		if (bit) {
			crc ^= 0x1edc6f41;
		}
	}
	return crc % ht_size;
}

#endif  // !defined(DB_H)

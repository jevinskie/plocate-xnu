#ifndef DB_H
#define DB_H 1

#include <stdint.h>

struct Header {
	char magic[8];  // "\0plocate";
	uint32_t version;  // 1.
	uint32_t hashtable_size;
	uint32_t extra_ht_slots;
	uint32_t num_docids;
	uint64_t hash_table_offset_bytes;
	uint64_t filename_index_offset_bytes;

	// Version 1 and up only.
	uint32_t max_version;  // Nominally 1 or 2, but can be increased if more features are added in a backward-compatible way.
	uint32_t zstd_dictionary_length_bytes;
	uint64_t zstd_dictionary_offset_bytes;

	// Only if max_version >= 2, and only relevant for updatedb.
	uint64_t directory_data_length_bytes;
	uint64_t directory_data_offset_bytes;
	uint64_t next_zstd_dictionary_length_bytes;
	uint64_t next_zstd_dictionary_offset_bytes;
	uint64_t conf_block_length_bytes;
	uint64_t conf_block_offset_bytes;

	// Only if max_version >= 2.
	bool require_visibility;
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

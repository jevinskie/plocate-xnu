#ifndef _SERIALIZER_H
#define _SERIALIZER_H 1

#include "options.h"

#include <assert.h>
#include <queue>
#include <stdint.h>
#include <string>

class ResultReceiver {
public:
	virtual ~ResultReceiver() = default;
	virtual void print(uint64_t seq, uint64_t skip, const std::string msg) = 0;
};

class Serializer : public ResultReceiver {
public:
	~Serializer() { assert(limit_left <= 0 || pending.empty()); }
	void print(uint64_t seq, uint64_t skip, const std::string msg) override;

private:
	uint64_t next_seq = 0;
	struct Element {
		uint64_t seq, skip;
		std::string msg;

		bool operator<(const Element &other) const
		{
			return seq > other.seq;
		}
	};
	std::priority_queue<Element> pending;
};

#endif  // !defined(_SERIALIZER_H)

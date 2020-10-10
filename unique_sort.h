#ifndef _UNIQUE_SORT_H
#define _UNIQUE_SORT_H 1

#include <algorithm>

template<class Container, class LessThan = std::less<typename Container::value_type>, class EqualTo = std::equal_to<typename Container::value_type>>
void unique_sort(Container *c, const LessThan &lt = LessThan(), const EqualTo &eq = EqualTo())
{
	sort(c->begin(), c->end(), lt);
	auto new_end = unique(c->begin(), c->end(), eq);
	c->erase(new_end, c->end());
}

#endif  // !defined(_UNIQUE_SORT_H)

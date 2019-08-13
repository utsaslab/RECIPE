#ifndef __IDX__UTILS__DATA_SET_MODIFIERS__
#define __IDX__UTILS__DATA_SET_MODIFIERS__

#include <algorithm>
#include <vector>

#include "idx/utils/RandomRangeGenerator.hpp"

namespace idx { namespace utils {

template<class ElementType> void randomizeData(std::vector<ElementType> & data) {
	std::random_shuffle(data.begin(), data.end());
}

template<class ElementType> void sortData(std::vector<ElementType> & data) {
	std::sort(data.begin(), data.end());
}

template<class ElementType> void reverseData(std::vector<ElementType> & data) {
	std::sort(data.begin(), data.end(), std::greater<ElementType>());
}

} }

#endif
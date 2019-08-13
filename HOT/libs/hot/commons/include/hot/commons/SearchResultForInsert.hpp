#ifndef __HOT__COMMONS__SEARCH_RESULT_FOR_INSERT___
#define __HOT__COMMONS__SEARCH_RESULT_FOR_INSERT___

#include <cstdint>

namespace hot { namespace commons {

/**
 * A Helper Function for storing additional result information:
 * 	- the index of the return entry
 * 	- the most significant bit index of the containing node
 */
struct SearchResultForInsert {
	uint32_t mEntryIndex;
	uint16_t mMostSignificantBitIndex;

	inline SearchResultForInsert(uint32_t entryIndex, uint16_t mostSignificantBitIndex)
		: mEntryIndex(entryIndex), mMostSignificantBitIndex(mostSignificantBitIndex) {
	}

	inline SearchResultForInsert() {
	}

	inline void init(uint32_t entryIndex, uint16_t mostSignificantBitIndex) {
		mEntryIndex = entryIndex;
		mMostSignificantBitIndex = mostSignificantBitIndex;
	}
};

}}

#endif
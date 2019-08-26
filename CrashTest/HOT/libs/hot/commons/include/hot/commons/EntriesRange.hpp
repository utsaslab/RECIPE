#ifndef __HOT__COMMONS__ENTRIES_RANGE__
#define __HOT__COMMONS__ENTRIES_RANGE__

#include <cstdint>

namespace hot { namespace commons {

/**
 * Describes a range of entries by its start index and the number of entries contained.
 */
struct EntriesRange {
	uint32_t const mFirstIndexInRange;
	uint32_t const mNumberEntriesInRange;

	EntriesRange(uint32_t firstIndexInRange, uint32_t numberEntriesInRange)
		: mFirstIndexInRange(firstIndexInRange), mNumberEntriesInRange(numberEntriesInRange)
	{
	}

	uint32_t inline getLastIndexInRange() const {
		return mFirstIndexInRange + mNumberEntriesInRange - 1;
	}
};

}}

#endif
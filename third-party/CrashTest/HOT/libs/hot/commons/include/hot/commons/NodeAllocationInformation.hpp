#ifndef __HOT__ROWEX__NODE_ALLOCATION_INFORMATION__
#define __HOT__ROWEX__NODE_ALLOCATION_INFORMATION__

namespace hot { namespace commons {

struct NodeAllocationInformation {
	uint32_t const mEntriesMask;
	uint16_t const mTotalSizeInBytes;
	uint16_t const mPointerOffset;

	NodeAllocationInformation(uint32_t entriesMask, uint16_t totalSizeInBytes, uint16_t pointerOffset)
		: mEntriesMask(entriesMask), mTotalSizeInBytes(totalSizeInBytes), mPointerOffset(pointerOffset)
	{
	}
};

}}

#endif
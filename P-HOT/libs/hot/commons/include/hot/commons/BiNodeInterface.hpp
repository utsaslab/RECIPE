#ifndef __HOT__COMMONS__BI_NODE_INTERFACE___
#define __HOT__COMMONS__BI_NODE_INTERFACE___

namespace hot { namespace commons {

template<typename ChildPointerType> struct BiNode {
	uint16_t mDiscriminativeBitIndex;
	uint16_t mHeight;
	ChildPointerType mLeft;
	ChildPointerType mRight;

	inline BiNode() {
		//is intentionally left undefined for performance reasons!!
	}

	inline BiNode(BiNode const & other) = default;
	inline BiNode & operator=(BiNode const & other) = default;

	inline BiNode(uint16_t const discriminativeBitIndex, uint16_t const height, ChildPointerType const & left, ChildPointerType const & right);
	inline static BiNode createFromExistingAndNewEntry(DiscriminativeBit const & discriminativeBit, ChildPointerType const & existingNode, ChildPointerType const & newEntry);
};

}}

#endif
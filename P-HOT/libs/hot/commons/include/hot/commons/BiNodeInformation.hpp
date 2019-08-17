#ifndef __HOT__COMMONS__BI_NODE_INFORMATION__
#define __HOT__COMMONS__BI_NODE_INFORMATION__

#include <cstdint>

#include "hot/commons/EntriesRange.hpp"

namespace hot { namespace commons {

/**
 * The BiNodeInformation, contains all information which is necessary a BiNode and the entries in its subtree for a linearized binary patricia trie.
 *
 * It therefore consists of:
 * 	+ The positions of the elements contained in its left/right subtree
 * 	+ The index of the corresponding discriminative bit
 * 	+ A partial key representing the corresponding discriminative bit
 */
struct BiNodeInformation {
	uint32_t const mDiscriminativeBitIndex;
	uint32_t const mDiscriminativeBitMask;

	EntriesRange const mLeft;
	EntriesRange const mRight;

	BiNodeInformation(uint32_t discriminativeBitIndex, uint32_t discriminativeBitMask, EntriesRange const & left, EntriesRange const & right)
		: mDiscriminativeBitIndex(discriminativeBitIndex), mDiscriminativeBitMask(discriminativeBitMask), mLeft(left), mRight(right)
	{
	}

	/**
	 * @return the total number of entries in the subtree rooted at the described BiNode
	 */
	uint32_t getTotalNumberEntries() const {
		return mLeft.mNumberEntriesInRange + mRight.mNumberEntriesInRange;
	}

};

}}

#endif
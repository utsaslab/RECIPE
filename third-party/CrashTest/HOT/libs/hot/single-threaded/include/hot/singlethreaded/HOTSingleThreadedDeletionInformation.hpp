#ifndef __HOT__SINGLETHREADED__HOT_SINGLE_THREADED_DELETION_INFORMATION__
#define __HOT__SINGLETHREADED__HOT_SINGLE_THREADED_DELETION_INFORMATION__

#include <hot/commons/BiNodeInformation.hpp>

#include "hot/singlethreaded/HOTSingleThreadedChildPointerInterface.hpp"
#include "hot/singlethreaded/HOTSingleThreadedNodeBaseInterface.hpp"

namespace hot { namespace singlethreaded {

/**
 * The deletion information required to create a new node by removing an entry from a given node.
 * It therefore contains:
 * 	- information required to compress and recode existing partial keys
 * 	- the index of the entry to remove
 * 	- information regarding the BiNode affected by the deletion operation consisting of:
 * 		+ the position of the entries in this BiNode
 * 		+ the position of the discriminative bit corresponding to this BiNode
 * 		+ the side of the BiNode the entry to delete was contained in (mDiscriminativeBitValueForEntry=0 => left,  mDiscriminativeBitValueForEntry=1 => right)
 * 	- a potential direct neighbour of the entry to delete (this is important for merge operations, or BiNode pull down => in both case this information must be taken from the parent's node deletion information)
 */
class HOTSingleThreadedDeletionInformation {
	HOTSingleThreadedChildPointer mContainingNode;
	uint32_t mCompressionMask;
	uint32_t mIndexOfEntryToRemove;
	uint32_t mDiscriminativeBitValueForEntry;

	hot::commons::BiNodeInformation mBiNodeInformation;
	HOTSingleThreadedChildPointer* mPotentialDirectNeighbour;


public:
	HOTSingleThreadedDeletionInformation(HOTSingleThreadedChildPointer const & containingNode, uint32_t compressionMask, uint32_t indexOfEntryToRemove, hot::commons::BiNodeInformation const & biNodeInformation)
		: mContainingNode(containingNode),
		  mCompressionMask(compressionMask),
		  mIndexOfEntryToRemove(indexOfEntryToRemove),
		  mDiscriminativeBitValueForEntry(1 - (biNodeInformation.mRight.mFirstIndexInRange - indexOfEntryToRemove)),
		  mBiNodeInformation(biNodeInformation),
		  mPotentialDirectNeighbour(determineDirectNeighbourIfAvailable())
	{
	}

	/**
	 *
	 * @return whether the entry to delete has a direct neighbour. An entry has a direct neighbour if its sibling BiNode is leaf BiNode and therefore either points to an actual leaf value of a child HOT node.
	 */
	bool hasDirectNeighbour() const {
		return mBiNodeInformation.getTotalNumberEntries() == 2;
	}

	/**
	 *
	 * @return if the entry to delete has a direct neighbour this function returns the direct neighbour. Otherwise it will return a null child pointer.
	 */
	HOTSingleThreadedChildPointer* getDirectNeighbourIfAvailable() const {
		return mPotentialDirectNeighbour;
	}

	/**
	 *
	 * @return the mask which can be used to recode/compress the partial keys for the required partial key representation in a new node without the entry to remove
	 */
	uint32_t getCompressionMask() const {
		return mCompressionMask;
	}

	/**
	 * @return the index of the entry to delete in the original node
	 */
	uint32_t getIndexOfEntryToRemove() const {
		return mIndexOfEntryToRemove;
	}

	/**
	 * @return whether the entry to delete is in the left or right subtree of the subtree rooted at the affected BiNode.
	 */
	uint32_t getDiscriminativeBitValueForEntry() const {
		return mDiscriminativeBitValueForEntry;
	}

	/**
	 * When recursively deleting entries, triggered by a merge or BiNode pushdown:
	 * 	+ first the sibling node is replaced from the parent
	 * 	+ second the remaining entry of two affected entries will be replaced with the merged node
	 * 	+ finally the newly created parent node is integrated into the structure
	 *
	 * @return the index of the remaining entry which must be replaced in the second step
	 */
	uint32_t getIndexOfEntryToReplace() const {
		return mIndexOfEntryToRemove - mDiscriminativeBitValueForEntry;
	}

	hot::commons::BiNodeInformation const & getAffectedBiNode() const {
		return mBiNodeInformation;
	}

	/**
	 * @return a pointer to the node which contains the entry to delete
	 */
	HOTSingleThreadedChildPointer const & getContainingNode() const {
		return mContainingNode;
	}

private:
	HOTSingleThreadedChildPointer* determineDirectNeighbourIfAvailable() const {
		uint32_t directNeighbourIndex = mBiNodeInformation.mRight.mFirstIndexInRange - mDiscriminativeBitValueForEntry;
		return hasDirectNeighbour() ? mContainingNode.getNode()->getPointers() + directNeighbourIndex : nullptr;
	}
};

}}

#endif
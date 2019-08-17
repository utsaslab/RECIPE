#ifndef __HOT__ROWEX__LOCKABLE_INSERT_STACK_ENTRY___
#define __HOT__ROWEX__LOCKABLE_INSERT_STACK_ENTRY___

#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/SearchResultForInsert.hpp>

#include "HOTRowexChildPointer.hpp"

namespace hot { namespace rowex {

class HOTRowexInsertStackEntry {
	HOTRowexChildPointer mChildPointer;
	HOTRowexChildPointer *mChildPointerLocation;

public:
	hot::commons::SearchResultForInsert mSearchResultForInsert;

	inline void initNode(HOTRowexChildPointer* childPointerLocation, HOTRowexChildPointer const & childPointer) {
		initChildPointer(childPointerLocation, childPointer);
	}

	inline void initLeaf(HOTRowexChildPointer* childPointerLocation, HOTRowexChildPointer const & childPointer) {
		initChildPointer(childPointerLocation, childPointer);

		//important for finding the correct depth!!
		mSearchResultForInsert.mMostSignificantBitIndex = UINT16_MAX;
	}

	HOTRowexChildPointer getChildPointer() const {
		return mChildPointer;
	}

    HOTRowexChildPointer*& getChildPointerLocation() {
        return mChildPointerLocation;
    }

	bool isConsistent() const {
		return (*mChildPointerLocation) == mChildPointer;
	}

	void updateChildPointer(HOTRowexChildPointer const & childPointer) {
		//for synchronized case do not update the pointer location as this location is needed for unlock later on!
		*mChildPointerLocation = childPointer;
		//mChildPointer = childPointer;
	}

	inline hot::commons::InsertInformation getInsertInformation(hot::commons::DiscriminativeBit const & mismatchingBit)  const {
		uint32_t entryIndex = mSearchResultForInsert.mEntryIndex;
		return getChildPointer().executeForSpecificNodeType(
			false, [&](auto const &existingNode) -> hot::commons::InsertInformation {
				return existingNode.getInsertInformation(entryIndex, mismatchingBit);
			});
	}

	inline bool isSingleEntryAffected(hot::commons::InsertInformation const & insertInformation) const {
		return (insertInformation.getNumberEntriesInAffectedSubtree() == 1u);
	}

	bool tryLock() {
		return getNode()->tryLock();
	}

	template<typename MemoryReclamationStrategy> void markAsObsolete(MemoryReclamationStrategy & memoryReclamation) {
		getNode()->markAsObsolete();
		memoryReclamation.scheduleForDeletion(mChildPointer);
	}

	void unlock() {
		return getNode()->unlock();
	}

	bool isObsolete() {
		return getNode()->isObsolete();
	}

private:
	//PERFORMANCE this must be uninitialized
	HOTRowexInsertStackEntry() {
		assert(false);
	}

	inline void initChildPointer(HOTRowexChildPointer* childPointerLocation, HOTRowexChildPointer const & childPointer) {
		mChildPointer = childPointer;
		mChildPointerLocation = childPointerLocation;
	}

	HOTRowexNodeBase* getNode() {
		return getChildPointer().getNode();
	}
};

}}

#endif
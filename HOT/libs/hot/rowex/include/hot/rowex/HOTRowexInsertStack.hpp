#ifndef __HOT__ROWEX__INSERT_STACK__
#define __HOT__ROWEX__INSERT_STACK__

#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/DiscriminativeBit.hpp>

#include <idx/contenthelpers/OptionalValue.hpp>
#include <idx/contenthelpers/TidConverters.hpp>
#include <idx/contenthelpers/KeyUtilities.hpp>

#include "hot/rowex/HOTRowexChildPointer.hpp"
#include "hot/rowex/HOTRowexFirstInsertLevel.hpp"

namespace hot { namespace rowex {

template<typename ValueType, template<typename> typename KeyExtractor, typename InsertStackEntry>
struct HOTRowexInsertStack {
	static KeyExtractor<ValueType> extractKey;
	using KeyType = decltype(extractKey(std::declval<ValueType>()));
	using EntryType = InsertStackEntry;

	char mRawStack[64 * sizeof(EntryType)];

	EntryType* mLeafEntry;

	EntryType const * getRawStack() const {
		return reinterpret_cast<EntryType const*>(mRawStack);
	}

	EntryType* getRawStack() {
		return reinterpret_cast<EntryType*>(mRawStack);
	}

	//do not initialize, for performance Reasons
	HOTRowexInsertStack(HOTRowexChildPointer currentRoot, HOTRowexChildPointer* rootPointer, uint8_t const *newKeyBytes) : mLeafEntry(reinterpret_cast<EntryType*>(mRawStack)) {
		HOTRowexChildPointer* currentPointerLocation = rootPointer;
		HOTRowexChildPointer currentPointer = currentRoot;
		while (!currentPointer.isLeaf()) {
			mLeafEntry->initNode(currentPointerLocation, currentPointer);
			currentPointerLocation = currentPointer.executeForSpecificNodeType(true, [&,this](auto &node) {
				return node.searchForInsert(mLeafEntry->mSearchResultForInsert, newKeyBytes);
			});
			currentPointer = *currentPointerLocation;
			++mLeafEntry;
		}
		mLeafEntry->initLeaf(currentPointerLocation, currentPointer);
	}

	bool isConsistent(EntryType* firstLockedEntry, unsigned int numberLockedEntries) {
		bool isConsistent = true;
		//start at minus one to check that the considered entry in the firstLockedEntry is still the same (not already replaced or leaf node pushdown or similar)
		for(int i=-1; i < static_cast<int>(numberLockedEntries); ++i) {
			isConsistent &= (firstLockedEntry - i)->isConsistent();
		}
		return isConsistent;
	}

	/***
	 *
	 * @param newDiscriminativeBit
	 * @param insertDepth
	 *
	 * @return the number entries locked or 0 it the lock could not be aquired
	 */
	inline unsigned int tryLock(HOTRowexChildPointer* mRoot, HOTRowexFirstInsertLevel<EntryType> const & insertLevel) {
		EntryType* firstInsertStackEntry = insertLevel.mFirstEntry;
		EntryType* insertStackEntry = firstInsertStackEntry;
		HOTRowexChildPointer currentNode = insertStackEntry->getChildPointer();

		int numberLockedEntries = 0;

		bool lockedSuccessfully = insertStackEntry->tryLock();
		numberLockedEntries += static_cast<unsigned int>(lockedSuccessfully);

		if(lockedSuccessfully & (!insertLevel.mIsLeafNodePushdown) & (insertStackEntry > getRawStack())) {
			EntryType* parentStackEntry = insertStackEntry - 1;
			lockedSuccessfully = parentStackEntry->tryLock();
			numberLockedEntries += static_cast<unsigned int>(lockedSuccessfully);

			while ((lockedSuccessfully & (parentStackEntry > getRawStack())) &&
				((currentNode.getNode()->isFull()) & ((currentNode.getHeight() + 1) == parentStackEntry->getChildPointer().getHeight()))) {
				currentNode = parentStackEntry->getChildPointer();
				lockedSuccessfully = (--parentStackEntry)->tryLock();
				numberLockedEntries += static_cast<unsigned int>(lockedSuccessfully);
			}
		}

		if(!lockedSuccessfully  || !isConsistent(insertStackEntry, numberLockedEntries)) {
			for(int i=numberLockedEntries - 1; i >= 0; --i) {
				(firstInsertStackEntry - i)->unlock();
			}
			numberLockedEntries = 0;
		}

		assert([&]() -> bool {
			EntryType *lastLockedEntry = firstInsertStackEntry + numberLockedEntries - 1;
			return (numberLockedEntries == 0 || lastLockedEntry != getRawStack() || *mRoot == lastLockedEntry->getChildPointer());
		}());


		return numberLockedEntries;
	}

	HOTRowexFirstInsertLevel<EntryType> determineInsertLevel(hot::commons::DiscriminativeBit const & mismatchingBit) {
		EntryType* nextInsertStackEntry = getRawStack() + 1u;

		//searches for the node to insert the new value into., typename InsertStackEntry
		//Be aware that this can result in a false positive. Therefor in case only a single entry is affected and it has a child node it must be inserted into the child node
		//this is an alternative approach to using getLeastSignificantDiscriminativeBitForEntry
		while ((mismatchingBit.mAbsoluteBitIndex >
			nextInsertStackEntry->mSearchResultForInsert.mMostSignificantBitIndex) & (nextInsertStackEntry < mLeafEntry)) {
			++nextInsertStackEntry;
		}

		EntryType* possibleInsertStackEntry = nextInsertStackEntry - 1u;
		//this is ensured because mMostSignificantDiscriminativeBitIndex is set to MAX_INT16 for the leaf entry
		assert(possibleInsertStackEntry < mLeafEntry);

		hot::commons::InsertInformation const &insertInformation = possibleInsertStackEntry->getInsertInformation(mismatchingBit);
		bool isSingleEntry = possibleInsertStackEntry->isSingleEntryAffected(insertInformation);

		bool isLeafEntry = (nextInsertStackEntry == mLeafEntry);
		bool isBoundaryNode = isSingleEntry & (!isLeafEntry);

		return (isBoundaryNode)
			   //in this case the single entry is a boundary entry -> insert the value into the child partition
			   //As the insert results in a new partition root, no prefix bits are set and all entries in the partition are affected
			   ? HOTRowexFirstInsertLevel<EntryType> { nextInsertStackEntry ,
				  {0, 0, static_cast<uint32_t>(nextInsertStackEntry->getChildPointer().getNode()->getNumberEntries()), mismatchingBit},
				   false}
			   : HOTRowexFirstInsertLevel<EntryType> { possibleInsertStackEntry, insertInformation, isSingleEntry & isLeafEntry & (possibleInsertStackEntry->getChildPointer().getHeight() > 1) };
	}

	idx::contenthelpers::OptionalValue<hot::commons::DiscriminativeBit> getMismatchingBit(uint8_t const *newKeyBytes) const {
		intptr_t tid = mLeafEntry->getChildPointer().getTid();
		ValueType const &existingValue = idx::contenthelpers::tidToValue<ValueType>(tid);
		KeyType const &existingKey = extractKey(existingValue);
		auto const &existingFixedSizeKey = idx::contenthelpers::toFixSizedKey(
			idx::contenthelpers::toBigEndianByteOrder(existingKey));
		uint8_t const *existingKeyBytes = idx::contenthelpers::interpretAsByteArray(existingFixedSizeKey);
		return hot::commons::getMismatchingBit(existingKeyBytes, newKeyBytes, static_cast<uint16_t>(idx::contenthelpers::getMaxKeyLength<KeyType>()));
	}
};

template<typename ValueType, template <typename> typename KeyExtractor, typename InsertStackEntry> KeyExtractor<ValueType> HOTRowexInsertStack<ValueType, KeyExtractor, InsertStackEntry>::extractKey;

}}

#endif
#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED__

#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <utility>
#include <set>
#include <map>
#include <numeric>
#include <cstring>


#include <hot/commons/Algorithms.hpp>
#include <hot/commons/BiNode.hpp>
#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/TwoEntriesNode.hpp>

#include "hot/singlethreaded/HOTSingleThreadedInterface.hpp"
#include "hot/singlethreaded/HOTSingleThreadedNode.hpp"

//Helper Data Structures
#include "HOTSingleThreadedInsertStackEntry.hpp"

#include "hot/singlethreaded/HOTSingleThreadedIterator.hpp"
#include "hot/singlethreaded/HOTSingleThreadedDeletionInformation.hpp"

#include "idx/contenthelpers/KeyUtilities.hpp"
#include "idx/contenthelpers/TidConverters.hpp"
#include "idx/contenthelpers/ContentEquals.hpp"
#include "idx/contenthelpers/KeyComparator.hpp"
#include "idx/contenthelpers/OptionalValue.hpp"

namespace hot { namespace singlethreaded {

template<typename ValueType, template <typename> typename KeyExtractor> KeyExtractor<ValueType> HOTSingleThreaded<ValueType, KeyExtractor>::extractKey;
template<typename ValueType, template <typename> typename KeyExtractor>
	typename idx::contenthelpers::KeyComparator<typename  HOTSingleThreaded<ValueType, KeyExtractor>::KeyType>::type
	HOTSingleThreaded<ValueType, KeyExtractor>::compareKeys;

template<typename ValueType, template <typename> typename KeyExtractor> typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::END_ITERATOR {};

template<typename ValueType, template <typename> typename KeyExtractor> HOTSingleThreaded<ValueType, KeyExtractor>::HOTSingleThreaded() : mRoot {} {
}

template<typename ValueType, template <typename> typename KeyExtractor> HOTSingleThreaded<ValueType, KeyExtractor>::HOTSingleThreaded(HOTSingleThreaded && other) {
	mRoot = other.mRoot;
	other.mRoot = {};
}

template<typename ValueType, template <typename> typename KeyExtractor> HOTSingleThreaded<ValueType, KeyExtractor> & HOTSingleThreaded<ValueType, KeyExtractor>::operator=(HOTSingleThreaded && other) {
	mRoot = other.mRoot;
	other.mRoot = {};
	return *this;
}

template<typename ValueType, template <typename> typename KeyExtractor> HOTSingleThreaded<ValueType, KeyExtractor>::~HOTSingleThreaded() {
	mRoot.deleteSubtree();
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline bool HOTSingleThreaded<ValueType, KeyExtractor>::isEmpty() const {
	return !mRoot.isLeaf() & (mRoot.getNode() == nullptr);
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline bool HOTSingleThreaded<ValueType, KeyExtractor>::isRootANode() const {
	return mRoot.isNode() & (mRoot.getNode() != nullptr);
}

template<typename ValueType, template <typename> typename KeyExtractor>inline __attribute__((always_inline)) idx::contenthelpers::OptionalValue<ValueType> HOTSingleThreaded<ValueType, KeyExtractor>::lookup(HOTSingleThreaded<ValueType, KeyExtractor>::KeyType const &key) {
	auto const & fixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(key));
	uint8_t const* byteKey = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);

	HOTSingleThreadedChildPointer current =  mRoot;
	while((!current.isLeaf()) & (current.getNode() != nullptr)) {
		HOTSingleThreadedChildPointer const * const & currentChildPointer = current.search(byteKey);
		current = *currentChildPointer;
	}
	return current.isLeaf() ? extractAndMatchLeafValue(current, key) : idx::contenthelpers::OptionalValue<ValueType>();
}

template<typename ValueType, template <typename> typename KeyExtractor>idx::contenthelpers::OptionalValue <ValueType> HOTSingleThreaded<ValueType, KeyExtractor>::extractAndMatchLeafValue( HOTSingleThreadedChildPointer const & current, HOTSingleThreaded<ValueType, KeyExtractor>::KeyType const &key) {
	ValueType const & value = idx::contenthelpers::tidToValue<ValueType>(current.getTid());
	return { idx::contenthelpers::contentEquals(extractKey(value), key), value };
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline idx::contenthelpers::OptionalValue<ValueType> HOTSingleThreaded<ValueType, KeyExtractor>::scan(KeyType const &key, size_t numberValues) const {
	const_iterator iterator = lower_bound(key);
	for(size_t i = 0u; i < numberValues && iterator != end(); ++i) {
		++iterator;
	}
	return iterator == end() ? idx::contenthelpers::OptionalValue<ValueType>({}) : idx::contenthelpers::OptionalValue<ValueType>({ true, *iterator });
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline unsigned int HOTSingleThreaded<ValueType, KeyExtractor>::searchForInsert(uint8_t const * keyBytes, std::array<HOTSingleThreadedInsertStackEntry, 64> & insertStack) {
	HOTSingleThreadedChildPointer* current = &mRoot;
	unsigned int currentDepth = 0;
	while(!current->isLeaf()) {
		insertStack[currentDepth].mChildPointer = current;
		current = current->executeForSpecificNodeType(true, [&](auto & node) {
			HOTSingleThreadedInsertStackEntry & currentStackEntry = insertStack[currentDepth];
			return node.searchForInsert(currentStackEntry.mSearchResultForInsert, keyBytes);
		});
		++currentDepth;
	}
	insertStack[currentDepth].initLeaf(current);
	return currentDepth;
}

template<typename ValueType, template <typename> typename KeyExtractor> inline bool HOTSingleThreaded<ValueType, KeyExtractor>::remove(KeyType const & key) {
	auto const & fixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(key));
	uint8_t const* keyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);
	bool wasContained = false;

	if(isRootANode()) {
		std::array<HOTSingleThreadedInsertStackEntry, 64> insertStack;
		unsigned int leafDepth = searchForInsert(keyBytes, insertStack);
		intptr_t tid = insertStack[leafDepth].mChildPointer->getTid();
		KeyType const & existingKey = extractKey(idx::contenthelpers::tidToValue<ValueType>(tid));
		wasContained = idx::contenthelpers::contentEquals(existingKey, key);
		if(wasContained) {
			removeWithStack(insertStack, leafDepth - 1);
		}
	} else if(mRoot.isLeaf() && hasTheSameKey(mRoot.getTid(), key)) {
		mRoot = HOTSingleThreadedChildPointer();
		wasContained = true;
	}

	return wasContained;
};

template<typename ValueType,  template <typename> typename KeyExtractor>
void
HOTSingleThreaded<ValueType, KeyExtractor>::removeWithStack(std::array<HOTSingleThreadedInsertStackEntry, 64> const &searchStack, unsigned int currentDepth) {
	removeAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(searchStack, currentDepth, determineDeletionInformation(searchStack, currentDepth), [](HOTSingleThreadedChildPointer const & newNode, size_t /* offset */){
		return newNode;
	});
}

template<typename ValueType,  template <typename> typename KeyExtractor>
void HOTSingleThreaded<ValueType, KeyExtractor>::removeRecurseUp(std::array<HOTSingleThreadedInsertStackEntry, 64> const &searchStack, unsigned int currentDepth,  HOTSingleThreadedDeletionInformation const & deletionInformation, HOTSingleThreadedChildPointer const & replacement) {
	if(deletionInformation.getContainingNode().getNumberEntries() == 2) {
		HOTSingleThreadedChildPointer previous = *searchStack[currentDepth].mChildPointer;
		*searchStack[currentDepth].mChildPointer = replacement;
		previous.free();
	} else {
		removeAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(searchStack, currentDepth, deletionInformation, [&](HOTSingleThreadedChildPointer const & newNode, size_t offset){
			newNode.getNode()->getPointers()[offset + deletionInformation.getIndexOfEntryToReplace()] = replacement;
			return newNode;
		});
	}
}

template<typename ValueType, template <typename> typename KeyExtractor> template<typename Operation>
	void HOTSingleThreaded<ValueType, KeyExtractor>::removeAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(
		std::array<HOTSingleThreadedInsertStackEntry, 64> const &searchStack, unsigned int currentDepth, HOTSingleThreadedDeletionInformation const & deletionInformation, Operation const & operation
	)
{
	HOTSingleThreadedChildPointer* current = searchStack[currentDepth].mChildPointer;
	bool isRoot = currentDepth == 0;
	if(isRoot) {
		removeEntryAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(current, deletionInformation, operation);
	} else {
		unsigned int parentDepth = currentDepth - 1;
		HOTSingleThreadedDeletionInformation const & parentDeletionInformation = determineDeletionInformation(searchStack, parentDepth);
		bool hasDirectNeighbour = parentDeletionInformation.hasDirectNeighbour();
		if(hasDirectNeighbour) {
			HOTSingleThreadedChildPointer* potentialDirectNeighbour = parentDeletionInformation.getDirectNeighbourIfAvailable();
			if((potentialDirectNeighbour->getHeight() == current->getHeight())) {
				size_t totalNumberEntries = potentialDirectNeighbour->getNumberEntries() + current->getNumberEntries() - 1;
				if(totalNumberEntries <= MAXIMUM_NUMBER_NODE_ENTRIES) {
					HOTSingleThreadedNodeBase *parentNode = searchStack[parentDepth].mChildPointer->getNode();
					HOTSingleThreadedChildPointer left = parentNode->getPointers()[parentDeletionInformation.getAffectedBiNode().mLeft.mFirstIndexInRange];
					HOTSingleThreadedChildPointer right = parentNode->getPointers()[parentDeletionInformation.getAffectedBiNode().mRight.mFirstIndexInRange];
					HOTSingleThreadedChildPointer mergedNode = operation(
						mergeNodesAndRemoveEntryIfPossible(
							parentDeletionInformation.getAffectedBiNode().mDiscriminativeBitIndex, left, right,
							deletionInformation, parentDeletionInformation.getDiscriminativeBitValueForEntry()
						),
						//offset in case the deleted entry is in the right side
						left.getNumberEntries() * parentDeletionInformation.getDiscriminativeBitValueForEntry()
					);
					assert(!mergedNode.isUnused() && mergedNode.isNode());
					removeRecurseUp(searchStack, parentDepth, parentDeletionInformation, mergedNode);
					left.free();
					right.free();
				} else {
					removeEntryAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(current, deletionInformation, operation);
				}
			} else if((potentialDirectNeighbour->getHeight() < current->getHeight())) {
				//this is required in case for the creation of this tree a node split happened, resulting in a link to a leaf or a node of smaller height
				//move directNeighbour into current and remove
				//removeAndAdd
				hot::commons::DiscriminativeBit keyInformation(parentDeletionInformation.getAffectedBiNode().mDiscriminativeBitIndex, !parentDeletionInformation.getDiscriminativeBitValueForEntry());
				HOTSingleThreadedChildPointer previousNode = *current;
				HOTSingleThreadedChildPointer newNode = operation(current->executeForSpecificNodeType(false, [&](auto const & currentNode) {
					return currentNode.removeAndAddEntry(deletionInformation, keyInformation, *potentialDirectNeighbour);
				}), parentDeletionInformation.getDiscriminativeBitValueForEntry());
				removeRecurseUp(searchStack, parentDepth, parentDeletionInformation, newNode);
				previousNode.free();
			} else {
				removeEntryAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(current, deletionInformation, operation);
			}
		} else {
			removeEntryAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(current, deletionInformation, operation);
		}
	}
};

template<typename ValueType, template <typename> typename KeyExtractor> template<typename Operation>
	void HOTSingleThreaded<ValueType, KeyExtractor>::removeEntryAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(
		HOTSingleThreadedChildPointer* const currentNodePointer, HOTSingleThreadedDeletionInformation const & deletionInformation, Operation const & operation
	)
{
	HOTSingleThreadedChildPointer previous = *currentNodePointer;
	*currentNodePointer = operation(
		currentNodePointer->executeForSpecificNodeType(false, [&](auto const & currentNode){
			return currentNode.removeEntry(deletionInformation);
		}),
		0
	);
	previous.free();
};


template<typename ValueType, template <typename> typename KeyExtractor>
inline bool HOTSingleThreaded<ValueType, KeyExtractor>::insert(ValueType const & value) {
	bool inserted = true;
	auto const & fixedSizeKey =
        idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(extractKey(value)));
	uint8_t const* keyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);

	if(isRootANode()) {
		std::array<HOTSingleThreadedInsertStackEntry, 64> insertStack;
		unsigned int leafDepth = searchForInsert(keyBytes, insertStack);
		intptr_t tid = insertStack[leafDepth].mChildPointer->getTid();  // results from search
		KeyType const & existingKey = extractKey(idx::contenthelpers::tidToValue<ValueType>(tid));
		inserted = insertWithInsertStack(insertStack, leafDepth, existingKey, keyBytes, value);
	} else if(mRoot.isLeaf()) {
		HOTSingleThreadedChildPointer valueToInsert(idx::contenthelpers::valueToTid(value));
		ValueType const & currentLeafValue = idx::contenthelpers::tidToValue<ValueType>(mRoot.getTid());
		auto const & existingFixedSizeKey =
            idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(extractKey(currentLeafValue)));
		uint8_t const* existingKeyBytes =
            idx::contenthelpers::interpretAsByteArray(existingFixedSizeKey);

		inserted = hot::commons::executeForDiffingKeys(existingKeyBytes, keyBytes,
                idx::contenthelpers::getMaxKeyLength<KeyType>(),
                [&](hot::commons::DiscriminativeBit const & significantKeyInformation) {
			hot::commons::BiNode<HOTSingleThreadedChildPointer> const &binaryNode =
            hot::commons::BiNode<HOTSingleThreadedChildPointer>::createFromExistingAndNewEntry(
                    significantKeyInformation, mRoot, valueToInsert);
			mRoot = hot::commons::createTwoEntriesNode<HOTSingleThreadedChildPointer,
            HOTSingleThreadedNode>(binaryNode)->toChildPointer();
		});
	} else {
		mRoot = HOTSingleThreadedChildPointer(idx::contenthelpers::valueToTid(value));
	}
	return inserted;
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline bool HOTSingleThreaded<ValueType, KeyExtractor>::insertWithInsertStack(
	std::array<HOTSingleThreadedInsertStackEntry, 64> &insertStack,
    unsigned int leafDepth, KeyType const &existingKey,
	uint8_t const *newKeyBytes, ValueType const &newValue)
{
	auto const & existingFixedSizeKey =
        idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(existingKey));
	uint8_t const* existingKeyBytes = idx::contenthelpers::interpretAsByteArray(existingFixedSizeKey);
	return hot::commons::executeForDiffingKeys(existingKeyBytes, newKeyBytes,
            idx::contenthelpers::getMaxKeyLength<KeyType>(),
            [&](hot::commons::DiscriminativeBit const & significantKeyInformation) {
		unsigned int insertDepth = 0;
		//Searches for the node to insert the new value into.
		//Be aware that this can result in a false positive.
        //Therefore, in case only a single entry is affected
        //and it has a child node it must be inserted into the child node
		//this is an alternative approach to using getLeastSignificantDiscriminativeBitForEntry
		while(significantKeyInformation.mAbsoluteBitIndex >
                insertStack[insertDepth+1].mSearchResultForInsert.mMostSignificantBitIndex) {
			++insertDepth;
		}
		//this is ensured because mMostSignificantDiscriminativeBitIndex is set to MAX_INT16 for the leaf entry
		assert(insertDepth < leafDepth);

		HOTSingleThreadedChildPointer valueToInsert(idx::contenthelpers::valueToTid(newValue));
		insertNewValueIntoNode(insertStack, significantKeyInformation, insertDepth, leafDepth, valueToInsert);
	});
}


template<typename ValueType, template <typename> typename KeyExtractor> inline idx::contenthelpers::OptionalValue<ValueType> HOTSingleThreaded<ValueType, KeyExtractor>::upsert(ValueType newValue) {
	KeyType newKey = extractKey(newValue);
	auto const & fixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(extractKey(newValue)));
	uint8_t const* keyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);

	if(isRootANode()) {
		std::array<HOTSingleThreadedInsertStackEntry, 64> insertStack;
		unsigned int leafDepth = searchForInsert(keyBytes, insertStack);
		intptr_t tid = insertStack[leafDepth].mChildPointer->getTid();
		ValueType const & existingValue = idx::contenthelpers::tidToValue<ValueType>(tid);

		if(insertWithInsertStack(insertStack, leafDepth, extractKey(existingValue), keyBytes, newValue)) {
			return idx::contenthelpers::OptionalValue<ValueType>();
		} else {
			*insertStack[leafDepth].mChildPointer = HOTSingleThreadedChildPointer(idx::contenthelpers::valueToTid(newValue));
			return idx::contenthelpers::OptionalValue<ValueType>(true, existingValue);;
		}
	} else if(mRoot.isLeaf()) {
		ValueType existingValue = idx::contenthelpers::tidToValue<ValueType>(mRoot.getTid());
		if(idx::contenthelpers::contentEquals(extractKey(existingValue), newKey)) {
			mRoot = HOTSingleThreadedChildPointer(idx::contenthelpers::valueToTid(newValue));
			return { true, existingValue };
		} else {
			insert(newValue);
			return {};
		}
	} else {
		mRoot = HOTSingleThreadedChildPointer(idx::contenthelpers::valueToTid(newValue));
		return {};
	}
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::begin() const {
	return isEmpty() ? END_ITERATOR : const_iterator(&mRoot);
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::end() const {
	return END_ITERATOR;
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::find(typename HOTSingleThreaded<ValueType, KeyExtractor>::KeyType const & searchKey) const {
	return isRootANode() ? findForNonEmptyTrie(searchKey) : END_ITERATOR;
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::findForNonEmptyTrie(typename HOTSingleThreaded<ValueType, KeyExtractor>::KeyType const & searchKey) const {
	HOTSingleThreadedChildPointer const * current = &mRoot;

	auto const & fixedSizedSearchKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(extractKey(searchKey)));
	uint8_t const* searchKeyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizedSearchKey);

	HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator it(current, current + 1);
	while(!current->isLeaf()) {
		current = it.descend(current->executeForSpecificNodeType(true, [&](auto & node) {
			return node.search(searchKeyBytes);
		}), current->getNode()->end());
	}

	ValueType const & leafValue = idx::contenthelpers::tidToValue<ValueType>(current->getTid());

	return idx::contenthelpers::contentEquals(extractKey(leafValue), searchKey) ? it : END_ITERATOR;
}

template<typename ValueType, template <typename> typename KeyExtractor> inline  __attribute__((always_inline)) typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::lower_bound(typename HOTSingleThreaded<ValueType, KeyExtractor>::KeyType const & searchKey) const {
	return lower_or_upper_bound(searchKey, true);
}

template<typename ValueType, template <typename> typename KeyExtractor> inline  __attribute__((always_inline)) typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::upper_bound(typename HOTSingleThreaded<ValueType, KeyExtractor>::KeyType const & searchKey) const {
	return lower_or_upper_bound(searchKey, false);
}

template<typename ValueType, template <typename> typename KeyExtractor> inline  __attribute__((always_inline)) typename HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator HOTSingleThreaded<ValueType, KeyExtractor>::lower_or_upper_bound(typename HOTSingleThreaded<ValueType, KeyExtractor>::KeyType const & searchKey, bool is_lower_bound) const {
	if(isEmpty()) {
		return END_ITERATOR;
	}

	HOTSingleThreaded<ValueType, KeyExtractor>::const_iterator it(&mRoot, &mRoot + 1);

	if(mRoot.isLeaf()) {
		ValueType const & existingValue = idx::contenthelpers::tidToValue<ValueType>(mRoot.getTid());
		KeyType const & existingKey = extractKey(existingValue);

		return (idx::contenthelpers::contentEquals(searchKey, existingKey) || compareKeys(existingKey, searchKey)) ? it : END_ITERATOR;
	} else {
		auto const & fixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(searchKey));
		uint8_t const* keyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);

		HOTSingleThreadedChildPointer const * current = &mRoot;
		std::array<uint16_t, 64> mostSignificantBitIndexes;

		while(!current->isLeaf()) {
			current = it.descend(current->executeForSpecificNodeType(true, [&](auto & node) {
				mostSignificantBitIndexes[it.mCurrentDepth] = node.mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex;
				return node.search(keyBytes);
			}), current->getNode()->end());
		}

		ValueType const & existingValue = *it;
		auto const & existingFixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(extractKey(existingValue)));
		uint8_t const* existingKeyBytes = idx::contenthelpers::interpretAsByteArray(existingFixedSizeKey);

		bool keysDiff = hot::commons::executeForDiffingKeys(existingKeyBytes, keyBytes, idx::contenthelpers::getMaxKeyLength<KeyType>(), [&](hot::commons::DiscriminativeBit const & significantKeyInformation) {
			//searches for the node to insert the new value into.
			//Be aware that this can result in a false positive. Therefor in case only a single entry is affected and it has a child node it must be inserted into the child node
			//this is an alternative approach to using getLeastSignificantDiscriminativeBitForEntry
			HOTSingleThreadedChildPointer const * child = it.mNodeStack[it.mCurrentDepth].getCurrent();
			unsigned int entryIndex = child - it.mNodeStack[--it.mCurrentDepth].getCurrent()->getNode()->getPointers();
			while (it.mCurrentDepth > 0 && significantKeyInformation.mAbsoluteBitIndex < mostSignificantBitIndexes[it.mCurrentDepth]) {
				child = it.mNodeStack[it.mCurrentDepth].getCurrent();
				entryIndex = child - it.mNodeStack[--it.mCurrentDepth].getCurrent()->getNode()->getPointers();
			}

			HOTSingleThreadedChildPointer const* currentNode = it.mNodeStack[it.mCurrentDepth].getCurrent();
			currentNode->executeForSpecificNodeType(false, [&](auto const &existingNode) -> void {
				hot::commons::InsertInformation const & insertInformation = existingNode.getInsertInformation(entryIndex, significantKeyInformation);

				unsigned int nextEntryIndex = insertInformation.mKeyInformation.mValue
											  ? (insertInformation.getFirstIndexInAffectedSubtree() + insertInformation.getNumberEntriesInAffectedSubtree())
											  : insertInformation.getFirstIndexInAffectedSubtree();

				HOTSingleThreadedChildPointer const * nextEntry = existingNode.getPointers() + nextEntryIndex;
				HOTSingleThreadedChildPointer const * endPointer = existingNode.end();

				it.descend(nextEntry, endPointer);

				if(nextEntry == endPointer) {
					++it;
				} else {
					it.descend();
				}
			});
		});

		if(!keysDiff && !is_lower_bound) {
			++it;
		}

		return it;
	}
}

inline void insertNewValueIntoNode(std::array<HOTSingleThreadedInsertStackEntry, 64> & insertStack,
        hot::commons::DiscriminativeBit const & significantKeyInformation, unsigned int insertDepth,
        unsigned int leafDepth, HOTSingleThreadedChildPointer const & valueToInsert) {
	HOTSingleThreadedInsertStackEntry const & insertStackEntry = insertStack[insertDepth];

	insertStackEntry.mChildPointer->executeForSpecificNodeType(false, [&](auto const &existingNode) -> void {
		uint32_t entryIndex = insertStackEntry.mSearchResultForInsert.mEntryIndex;
		hot::commons::InsertInformation const &insertInformation = existingNode.getInsertInformation(
			entryIndex, significantKeyInformation
		);

		//As entryMask has Only a single bit set insertInformation.mAffectedSubtreeMask == entryMask checks whether the entry bit is the only bit set in the affectedSubtreeMask
		bool isSingleEntry = (insertInformation.getNumberEntriesInAffectedSubtree() == 1);
		unsigned int nextInsertDepth = insertDepth + 1u;

		bool isLeafEntry = (nextInsertDepth == leafDepth);

		if(isSingleEntry & isLeafEntry) {
			HOTSingleThreadedChildPointer const & leafEntry = *insertStack[nextInsertDepth].mChildPointer;
			//in case the current partition is a leaf partition add it to the partition
			//otherwise create new leaf partition containing the existing leaf node and the new value
			integrateBiNodeIntoTree(insertStack, nextInsertDepth, hot::commons::BiNode<HOTSingleThreadedChildPointer>::createFromExistingAndNewEntry(
				insertInformation.mKeyInformation, leafEntry, valueToInsert
			), true);
		} else if(isSingleEntry) {
            //in this case the single entry is a boundary node -> insert the value into the child partition
			insertStack[nextInsertDepth].mChildPointer->executeForSpecificNodeType(false, [&](auto &childPartition) -> void {
				insertNewValueResultingInNewPartitionRoot(childPartition, insertStack, significantKeyInformation, nextInsertDepth,
														  valueToInsert);
			});
		} else {
			insertNewValue(existingNode, insertStack, insertInformation, insertDepth, valueToInsert);
		}
	});
}

template<typename NodeType> inline void insertNewValueResultingInNewPartitionRoot(NodeType const &existingNode,
																				  std::array<HOTSingleThreadedInsertStackEntry, 64> &insertStack,
																				  const hot::commons::DiscriminativeBit &keyInformation,
																				  unsigned int insertDepth,
																				  HOTSingleThreadedChildPointer const &valueToInsert) {
	HOTSingleThreadedInsertStackEntry const & insertStackEntry = insertStack[insertDepth];
	if (!existingNode.isFull()) {
		//As the insert results in a new partition root, no prefix bits are set and all entries in the partition are affected
		hot::commons::InsertInformation insertInformation { 0, 0, static_cast<uint32_t>(existingNode.getNumberEntries()), keyInformation};
		*(insertStackEntry.mChildPointer) = existingNode.addEntry(insertInformation, valueToInsert);
		delete &existingNode;
	} else {
		assert(keyInformation.mAbsoluteBitIndex != insertStackEntry.mSearchResultForInsert.mMostSignificantBitIndex);
		hot::commons::BiNode<HOTSingleThreadedChildPointer> const &binaryNode = hot::commons::BiNode<HOTSingleThreadedChildPointer>::createFromExistingAndNewEntry(keyInformation, *insertStackEntry.mChildPointer, valueToInsert);
		integrateBiNodeIntoTree(insertStack, insertDepth, binaryNode, true);
	}
}

template<typename NodeType> inline void insertNewValue(NodeType const &existingNode,
													   std::array<HOTSingleThreadedInsertStackEntry, 64> &insertStack,
													   hot::commons::InsertInformation const &insertInformation,
													   unsigned int insertDepth, HOTSingleThreadedChildPointer const &valueToInsert)
{
	HOTSingleThreadedInsertStackEntry const & insertStackEntry = insertStack[insertDepth];

	if (!existingNode.isFull()) {
        // normal insertion
		HOTSingleThreadedChildPointer newNodePointer = existingNode.addEntry(insertInformation, valueToInsert);
		*(insertStackEntry.mChildPointer) = newNodePointer;
		delete &existingNode;
	} else {
		assert(insertInformation.mKeyInformation.mAbsoluteBitIndex != insertStackEntry.mSearchResultForInsert.mMostSignificantBitIndex);
		if (insertInformation.mKeyInformation.mAbsoluteBitIndex > insertStackEntry.mSearchResultForInsert.mMostSignificantBitIndex) {
            // Parent pull up or intermediate node creation
			hot::commons::BiNode<HOTSingleThreadedChildPointer> const &binaryNode = existingNode.split(insertInformation, valueToInsert);
			integrateBiNodeIntoTree(insertStack, insertDepth, binaryNode, true);
			delete &existingNode;
		} else {
            // leaf node pushdown
			hot::commons::BiNode<HOTSingleThreadedChildPointer> const &binaryNode =
                hot::commons::BiNode<HOTSingleThreadedChildPointer>::createFromExistingAndNewEntry(insertInformation.mKeyInformation,
                        *insertStackEntry.mChildPointer, valueToInsert);
			integrateBiNodeIntoTree(insertStack, insertDepth, binaryNode, true);
		}
	}
}

inline void integrateBiNodeIntoTree(std::array<HOTSingleThreadedInsertStackEntry, 64> & insertStack,
        unsigned int currentDepth, hot::commons::BiNode<HOTSingleThreadedChildPointer> const & splitEntries, bool const newIsRight) {
	if(currentDepth == 0) {
        // leaf node pushdown
		*insertStack[0].mChildPointer = hot::commons::createTwoEntriesNode<HOTSingleThreadedChildPointer, HOTSingleThreadedNode>(splitEntries)->toChildPointer();
	} else {
		unsigned int parentDepth = currentDepth - 1;
		HOTSingleThreadedInsertStackEntry const & parentInsertStackEntry = insertStack[parentDepth];
		HOTSingleThreadedChildPointer parentNodePointer = *parentInsertStackEntry.mChildPointer;

		HOTSingleThreadedNodeBase* existingParentNode = parentNodePointer.getNode();
		if(existingParentNode->mHeight > splitEntries.mHeight) {
            // Intermediate node creation
            // Create intermediate partition if height(partition) + 1 < height(parentPartition)
			*insertStack[currentDepth].mChildPointer = hot::commons::createTwoEntriesNode<HOTSingleThreadedChildPointer, HOTSingleThreadedNode>(splitEntries)->toChildPointer();
		} else {
            // Parent pull up
            // Integrate nodes into parent partition if height(partition) + 1 == height(parentPartition)
			hot::commons::DiscriminativeBit const significantKeyInformation { splitEntries.mDiscriminativeBitIndex, newIsRight };

			parentNodePointer.executeForSpecificNodeType(false, [&](auto & parentNode) -> void {
				hot::commons::InsertInformation const & insertInformation = parentNode.getInsertInformation(
					parentInsertStackEntry.mSearchResultForInsert.mEntryIndex, significantKeyInformation
				);

				unsigned int entryOffset = (newIsRight) ? 0 : 1;
				HOTSingleThreadedChildPointer valueToInsert { (newIsRight) ? splitEntries.mRight : splitEntries.mLeft };
				HOTSingleThreadedChildPointer valueToReplace { (newIsRight) ? splitEntries.mLeft : splitEntries.mRight };

				if(!parentNode.isFull()) {
					HOTSingleThreadedChildPointer newNodePointer = parentNode.addEntry(insertInformation, valueToInsert);
					newNodePointer.getNode()->getPointers()[parentInsertStackEntry.mSearchResultForInsert.mEntryIndex + entryOffset] = valueToReplace;
					*parentInsertStackEntry.mChildPointer = newNodePointer;
				} else {
					//The diffing Bit index cannot be larger as the parents mostSignificantBitIndex. the reason is that otherwise
					//the trie condition would be violated
					assert(parentInsertStackEntry.mSearchResultForInsert.mMostSignificantBitIndex < splitEntries.mDiscriminativeBitIndex);

					//Furthermore due to the trie condition it is safe to assume that both the existing entry and the new entry will be part of the same subtree
					hot::commons::BiNode<HOTSingleThreadedChildPointer> const & newSplitEntries = parentNode.split(insertInformation, valueToInsert);

					//Detect subtree side
					//This newSplitEntries.mLeft.getHeight() == parentNodePointer.getHeight() check is important because in case of a split with 1:31 it can happend that if
					//the 1 entry is not a leaf node the node it is pointing to will be pulled up, which implies that the numberEntriesInLowerPart are not correct anymore.
					unsigned int numberEntriesInLowerPart = newSplitEntries.mLeft.getHeight() == parentNode.mHeight ? newSplitEntries.mLeft.getNumberEntries() : 1;
					bool isInUpperPart = numberEntriesInLowerPart <= parentInsertStackEntry.mSearchResultForInsert.mEntryIndex;
					//Here is problem because of parentInsertstackEntry
					unsigned int correspondingEntryIndexInPart = parentInsertStackEntry.mSearchResultForInsert.mEntryIndex - (isInUpperPart * numberEntriesInLowerPart) + entryOffset;
					HOTSingleThreadedChildPointer nodePointerContainingSplitEntries = (isInUpperPart) ? newSplitEntries.mRight : newSplitEntries.mLeft;
					nodePointerContainingSplitEntries.getNode()->getPointers()[correspondingEntryIndexInPart] = valueToReplace;
					integrateBiNodeIntoTree(insertStack, parentDepth, newSplitEntries, true);
				}
				delete &parentNode;
			});
			//Order because branch prediction might choose this case in first place
		}
	}
}

template<typename ValueType, template <typename> typename KeyExtractor> inline HOTSingleThreadedChildPointer HOTSingleThreaded<ValueType, KeyExtractor>::getNodeAtPath(std::initializer_list<unsigned int> path) {
	HOTSingleThreadedChildPointer current = mRoot;
	for(unsigned int entryIndex : path) {
		assert(!current.isLeaf());
		current = current.getNode()->getPointers()[entryIndex];
	}
	return current;
}

template<typename ValueType, template <typename> typename KeyExtractor> inline void HOTSingleThreaded<ValueType, KeyExtractor>::collectStatsForSubtree(HOTSingleThreadedChildPointer const & subTreeRoot, std::map<std::string, double> & stats) const {
	if(!subTreeRoot.isLeaf()) {
		subTreeRoot.executeForSpecificNodeType(true, [&, this](auto & node) -> void {
			std::string nodeType = nodeAlgorithmToString(node.mNodeType);
			stats["total"] += node.getNodeSizeInBytes();
			stats[nodeType] += 1.0;
			for(HOTSingleThreadedChildPointer const & childPointer : node) {
				this->collectStatsForSubtree(childPointer, stats);
			}
		});
	}
}

template<typename ValueType, template <typename> typename KeyExtractor> std::pair<size_t, std::map<std::string, double>> HOTSingleThreaded<ValueType, KeyExtractor>::getStatistics() const {
	std::map<size_t, size_t> leafNodesPerDepth;
	getValueDistribution(mRoot, 0, leafNodesPerDepth);

	std::map<size_t, size_t> leafNodesPerBinaryDepth;
	getBinaryTrieValueDistribution(mRoot, 0, leafNodesPerBinaryDepth);

	std::map<std::string, double> statistics;
	statistics["height"] = mRoot.getHeight();
	statistics["numberAllocations"] = HOTSingleThreadedNodeBase::getNumberAllocations();

	size_t overallLeafNodeCount = 0;
	for(auto leafNodesOnDepth : leafNodesPerDepth) {
		std::string statisticsKey { "leafNodesOnDepth_"};
		std::string levelString = std::to_string(leafNodesOnDepth.first);
		statisticsKey += std::string(2 - levelString.length(), '0') + levelString;
		statistics[statisticsKey] = leafNodesOnDepth.second;
		overallLeafNodeCount += leafNodesOnDepth.second;
	}

	for(auto leafNodesOnBinaryDepth : leafNodesPerBinaryDepth) {
		std::string statisticsKey { "leafNodesOnBinaryDepth_"};
		std::string levelString = std::to_string(leafNodesOnBinaryDepth.first);
		statisticsKey += std::string(3 - levelString.length(), '0') + levelString;
		statistics[statisticsKey] = leafNodesOnBinaryDepth.second;
	}

	statistics["numberValues"] = overallLeafNodeCount;
	collectStatsForSubtree(mRoot, statistics);

	size_t totalSize = statistics["total"];
	statistics.erase("total");

	return {totalSize, statistics };
}

template<typename ValueType, template <typename> typename KeyExtractor> inline void HOTSingleThreaded<ValueType, KeyExtractor>::getValueDistribution(HOTSingleThreadedChildPointer const & childPointer, size_t depth, std::map<size_t, size_t> & leafNodesPerDepth) const {
	if(childPointer.isLeaf()) {
		++leafNodesPerDepth[depth];
	} else {
		for(HOTSingleThreadedChildPointer const & pointer : (*childPointer.getNode())) {
			getValueDistribution(pointer, depth + 1, leafNodesPerDepth);
		}
	}
}

template<typename ValueType, template <typename> typename KeyExtractor> inline void HOTSingleThreaded<ValueType, KeyExtractor>::getBinaryTrieValueDistribution(HOTSingleThreadedChildPointer const & childPointer, size_t binaryTrieDepth, std::map<size_t, size_t> & leafNodesPerDepth) const {
	if(childPointer.isLeaf()) {
		++leafNodesPerDepth[binaryTrieDepth];
	} else {
		childPointer.executeForSpecificNodeType(true, [&, this](auto &node) {
			std::array<uint8_t, 32> binaryEntryDepthsInNode = node.getEntryDepths();
			size_t i=0;
			for(HOTSingleThreadedChildPointer const & pointer : node) {
				this->getBinaryTrieValueDistribution(pointer, binaryTrieDepth + binaryEntryDepthsInNode[i], leafNodesPerDepth);
				++i;
			}
		});
	}
}

template<typename ValueType, template <typename> typename KeyExtractor>
bool HOTSingleThreaded<ValueType, KeyExtractor>::hasTheSameKey(intptr_t tid, HOTSingleThreaded<ValueType, KeyExtractor>::KeyType  const & key) {
	KeyType const & storedKey = extractKey(idx::contenthelpers::tidToValue<ValueType>(tid));
	return idx::contenthelpers::contentEquals(storedKey, key);
}

template<typename ValueType, template <typename> typename KeyExtractor>
HOTSingleThreadedDeletionInformation HOTSingleThreaded<ValueType, KeyExtractor>::determineDeletionInformation(
	const std::array<HOTSingleThreadedInsertStackEntry, 64> &searchStack, unsigned int currentDepth) {
	HOTSingleThreadedInsertStackEntry const & currentEntry = searchStack[currentDepth];
	uint32_t indexOfEntryToRemove = currentEntry.mSearchResultForInsert.mEntryIndex;
	return currentEntry.mChildPointer->executeForSpecificNodeType(false, [&](auto const & currentNode) {
		return currentNode.getDeletionInformation(indexOfEntryToRemove);
	});

}

template<typename ValueType, template <typename> typename KeyExtractor>
size_t HOTSingleThreaded<ValueType, KeyExtractor>::getHeight() const {
	return isEmpty() ? 0 : mRoot.getHeight();

}


} }

#endif

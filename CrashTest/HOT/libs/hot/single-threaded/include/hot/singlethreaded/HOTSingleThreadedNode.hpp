#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_NODE__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_NODE__

#include <cstdint>

#include <idx/contenthelpers/OptionalValue.hpp>

#include <hot/commons/BiNode.hpp>
#include <hot/commons/BiNodeInformation.hpp>
#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/NodeAllocationInformations.hpp>
#include <hot/commons/NodeMergeInformation.hpp>
#include <hot/commons/PartialKeyMappingHelpers.hpp>
#include <hot/commons/SearchResultForInsert.hpp>
#include <hot/commons/SparsePartialKeys.hpp>
#include <hot/commons/TwoEntriesNode.hpp>

#include "hot/singlethreaded/HOTSingleThreadedNodeBase.hpp"
#include "hot/singlethreaded/HOTSingleThreadedNodeInterface.hpp"

#include "HOTSingleThreadedChildPointer.hpp"

namespace hot { namespace singlethreaded {

constexpr uint32_t calculatePointerSize(uint16_t const numberEntries) {
	constexpr uint32_t childPointerSize = static_cast<uint32_t>(sizeof(HOTSingleThreadedChildPointer));
	return childPointerSize * ((numberEntries < 3ul) ? 3u : numberEntries);
}

constexpr static uint32_t convertNumbeEntriesToEntriesMask(uint16_t numberEntries) {
	return (UINT32_MAX >> (32 - numberEntries));
}

template<typename PartialKeyType> struct NextPartialKeyType {
};

template<> struct NextPartialKeyType<uint8_t> {
	using Type = uint16_t;
};

template<> struct NextPartialKeyType<uint16_t> {
	using Type = uint32_t;
};

template<> struct NextPartialKeyType<uint32_t> {
	using Type = uint32_t;
};

template<typename NewDiscriminativeBitsRepresentation, typename ExistingPartialKeyType> struct ToPartialKeyType {
	using Type = ExistingPartialKeyType;
};

template<> struct ToPartialKeyType<hot::commons::MultiMaskPartialKeyMapping<2>, uint8_t> {
	using Type = uint16_t;
};

template<> struct ToPartialKeyType<hot::commons::MultiMaskPartialKeyMapping<4>, uint16_t> {
	using Type = uint32_t;
};

template<typename NewDiscriminativeBitsRepresentation, typename PartialKeyType> struct ToDiscriminativeBitsRepresentation {
	using Type = NewDiscriminativeBitsRepresentation;
};

template<> struct ToDiscriminativeBitsRepresentation<hot::commons::MultiMaskPartialKeyMapping<2u>, uint32_t> {
	using Type = hot::commons::MultiMaskPartialKeyMapping<4>;
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> void* HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::operator new (size_t /* baseSize */, uint16_t const numberEntries) {
	hot::commons::NodeAllocationInformation const & allocationInformation = hot::commons::NodeAllocationInformations<HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>>::getAllocationInformation(numberEntries);

	//at least two entries must be contained in the node
	assert(numberEntries >= 2);
	/*NodeAllocationInformation const & allocationInformation = mAllocationInformation[numberEntries];*/
	uint16_t sizeInBytes = allocationInformation.mTotalSizeInBytes;
	void* memory = HOTSingleThreadedNodeBase::getMemoryPool()->alloc(sizeInBytes/sizeof(uint64_t));
	return memory;
	/*void* memoryForNode = nullptr;
	uint error = posix_memalign(&memoryForNode, SIMD_COB_TRIE_NODE_ALIGNMENT, allocationInformation.mTotalSizeInBytes);
	if(error != 0) {
		//"Got error on alignment"
		throw std::bad_alloc();
	}

	return memoryForNode;*/
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> void HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::operator delete (void * rawMemory) {
	//free(rawMemory);
	size_t previousNumberEntries = reinterpret_cast<HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>*>(rawMemory)->getNumberEntries();
	hot::commons::NodeAllocationInformation const & allocationInformation = hot::commons::NodeAllocationInformations<HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>>::getAllocationInformation(previousNumberEntries);
	HOTSingleThreadedNodeBase::getMemoryPool()->returnToPool(allocationInformation.mTotalSizeInBytes/sizeof(uint64_t), rawMemory);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>
inline hot::commons::NodeAllocationInformation HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getNodeAllocationInformation(uint16_t const numberEntries) {
	constexpr uint32_t entriesMasksBaseSize = static_cast<uint32_t>(sizeof(hot::commons::SparsePartialKeys<PartialKeyType>));
	constexpr uint32_t baseSize = static_cast<uint32_t>(sizeof(HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>)) - entriesMasksBaseSize;

	uint32_t pointersSize = calculatePointerSize(numberEntries);
	uint16_t pointerOffset = hot::commons::SparsePartialKeys<PartialKeyType>::estimateSize(numberEntries) + baseSize;
	uint32_t rawSize = pointersSize + pointerOffset;
	assert((rawSize % 8) == 0);

	return hot::commons::NodeAllocationInformation(convertNumbeEntriesToEntriesMask(numberEntries), rawSize, pointerOffset);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTSingleThreadedNode(uint16_t const height, uint16_t const numberEntries, DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation)
	: HOTSingleThreadedNodeBase(height, HOTSingleThreadedNode::getNodeAllocationInformation(numberEntries)), mDiscriminativeBitsRepresentation(discriminativeBitsRepresentation)
{
}

//add entry into node
template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTSingleThreadedNode(
	HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
	uint16_t const newNumberEntries,
	DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	hot::commons::InsertInformation const & insertInformation,
	HOTSingleThreadedChildPointer const & newValue
) : HOTSingleThreadedNode(sourceNode.mHeight, newNumberEntries, discriminativeBitsRepresentation) {

	unsigned int oldNumberEntries = (newNumberEntries - 1u);

	HOTSingleThreadedChildPointer const * __restrict__ existingPointers = sourceNode.getPointers();
	HOTSingleThreadedChildPointer * __restrict__ targetPointers = getPointers();

	SourcePartialKeyType __restrict__ const * existingMasks = sourceNode.mPartialKeys.mEntries;
	PartialKeyType __restrict__ * targetMasks = mPartialKeys.mEntries;

	hot::commons::DiscriminativeBit const & keyInformation = insertInformation.mKeyInformation;
	PartialKeyConversionInformation const & conversionInformation = getConversionInformation(sourceNode, keyInformation);

	unsigned int firstIndexInAffectedSubtree = insertInformation.getFirstIndexInAffectedSubtree();
	unsigned int numberEntriesInAffectedSubtree = insertInformation.getNumberEntriesInAffectedSubtree();

	for(unsigned int i = 0u; i < firstIndexInAffectedSubtree; ++i) {
		targetMasks[i] = _pdep_u32(existingMasks[i], conversionInformation.mConversionMask);
		targetPointers[i] = existingPointers[i];
	}

	uint32_t convertedSubTreePrefixMask = _pdep_u32(insertInformation.mSubtreePrefixPartialKey, conversionInformation.mConversionMask);
	unsigned int firstIndexAfterAffectedSubtree = firstIndexInAffectedSubtree + numberEntriesInAffectedSubtree;
	if(keyInformation.mValue) {
		for (unsigned int i = firstIndexInAffectedSubtree; i < firstIndexAfterAffectedSubtree; ++i) {
			targetMasks[i] = _pdep_u32(existingMasks[i], conversionInformation.mConversionMask);
			targetPointers[i] = existingPointers[i];
		}
		targetMasks[firstIndexAfterAffectedSubtree] = static_cast<PartialKeyType>(convertedSubTreePrefixMask | conversionInformation.mAdditionalMask);
		targetPointers[firstIndexAfterAffectedSubtree] = newValue;
	} else {
		targetMasks[firstIndexInAffectedSubtree] = static_cast<PartialKeyType>(convertedSubTreePrefixMask);
		targetPointers[firstIndexInAffectedSubtree] = newValue;
		for (unsigned int i = firstIndexInAffectedSubtree; i < firstIndexAfterAffectedSubtree; ++i) {
			unsigned int targetIndex = i + 1u;
			targetMasks[targetIndex] = static_cast<PartialKeyType>(_pdep_u32(existingMasks[i], conversionInformation.mConversionMask) | conversionInformation.mAdditionalMask);
			targetPointers[targetIndex] = existingPointers[i];
		}
	}

	for(unsigned int i = firstIndexAfterAffectedSubtree; i < oldNumberEntries; ++i) {
		unsigned int targetIndex = i + 1u;
		targetMasks[targetIndex] = _pdep_u32(existingMasks[i], conversionInformation.mConversionMask);
		targetPointers[targetIndex] = existingPointers[i];
	}
}

//remove entry from node
template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTSingleThreadedNode(
	HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
	uint16_t const newNumberEntries,
	DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	SourcePartialKeyType compressionMask,
	uint32_t firstIndexInRange,
	uint32_t numberEntriesInRange
) : HOTSingleThreadedNode(sourceNode.mHeight, newNumberEntries, discriminativeBitsRepresentation) {

	compressRangeIntoNewNode(sourceNode, compressionMask, firstIndexInRange, 0, numberEntriesInRange);

	//This is important for the tree to have fast lookup and maintain integrity!! the first mask always is zero!!
	mPartialKeys.mEntries[0] = 0u;

	assert(getMaskForLargerEntries() != mUsedEntriesMask);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTSingleThreadedNode(
	HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
	uint16_t const newNumberEntries,
	DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	SourcePartialKeyType compressionMask,
	uint32_t firstIndexInRange,
	uint32_t numberEntriesInRange,
	hot::commons::InsertInformation const & insertInformation,
	HOTSingleThreadedChildPointer const & newValue
) : HOTSingleThreadedNode(sourceNode.mHeight, newNumberEntries, discriminativeBitsRepresentation) {
	hot::commons::DiscriminativeBit const & keyInformation = insertInformation.mKeyInformation;
	PartialKeyConversionInformation const & conversionInformation = getConversionInformationForCompressionMask(compressionMask, keyInformation);

	PartialKeyType additionalBitConversionMask = conversionInformation.mConversionMask;

	PartialKeyType newMask = static_cast<PartialKeyType>(_pdep_u32(_pext_u32(insertInformation.mSubtreePrefixPartialKey, compressionMask), additionalBitConversionMask))
		| (keyInformation.mValue * conversionInformation.mAdditionalMask);

	unsigned int numberEntriesBeforeAffectedSubtree = insertInformation.getFirstIndexInAffectedSubtree() - firstIndexInRange;

	HOTSingleThreadedChildPointer const * __restrict__ existingPointers = sourceNode.getPointers();
	HOTSingleThreadedChildPointer * __restrict__ targetPointers = getPointers();

	SourcePartialKeyType __restrict__ const * existingMasks = sourceNode.mPartialKeys.mEntries;
	PartialKeyType __restrict__ * targetMasks = mPartialKeys.mEntries;

	for(unsigned int targetIndex = 0; targetIndex < numberEntriesBeforeAffectedSubtree; ++targetIndex) {
		unsigned int sourceIndex = firstIndexInRange + targetIndex;
		targetMasks[targetIndex] = _pdep_u32(_pext_u32(existingMasks[sourceIndex], compressionMask), additionalBitConversionMask);
		targetPointers[targetIndex] = existingPointers[sourceIndex];
	}

	unsigned int newBitForExistingEntries = (1 - keyInformation.mValue);
	unsigned int firstTargetIndexInAffectedSubtree = numberEntriesBeforeAffectedSubtree + newBitForExistingEntries;
	unsigned int additionalMaskForExistingEntries = conversionInformation.mAdditionalMask  * newBitForExistingEntries;

	for (unsigned int indexInAffectedSubtree = 0; indexInAffectedSubtree <  insertInformation.getNumberEntriesInAffectedSubtree(); ++indexInAffectedSubtree) {
		unsigned int sourceIndex = insertInformation.getFirstIndexInAffectedSubtree() + indexInAffectedSubtree;
		unsigned int targetIndex = firstTargetIndexInAffectedSubtree + indexInAffectedSubtree;

		targetMasks[targetIndex] = _pdep_u32(_pext_u32(existingMasks[sourceIndex], compressionMask), additionalBitConversionMask) | additionalMaskForExistingEntries;
		targetPointers[targetIndex] = existingPointers[sourceIndex];
	}

	unsigned int sourceNumberEntriesInRangeUntilEndOfAffectedSubtree = numberEntriesBeforeAffectedSubtree + insertInformation.getNumberEntriesInAffectedSubtree();
	unsigned int sourceIndexAfterAffectedSubtree = firstIndexInRange + sourceNumberEntriesInRangeUntilEndOfAffectedSubtree;
	unsigned int firstTargeIndexAfterAffectedSubtree = sourceNumberEntriesInRangeUntilEndOfAffectedSubtree + 1;
	unsigned int numberEntriesAfterAffectedSubtree = numberEntriesInRange - sourceNumberEntriesInRangeUntilEndOfAffectedSubtree;
	for(unsigned int indexAfterAffectedSubtree = 0; indexAfterAffectedSubtree < numberEntriesAfterAffectedSubtree; ++indexAfterAffectedSubtree) {
		unsigned int sourceIndex = sourceIndexAfterAffectedSubtree + indexAfterAffectedSubtree;
		unsigned int targetIndex = firstTargeIndexAfterAffectedSubtree + indexAfterAffectedSubtree;
		targetMasks[targetIndex] = _pdep_u32(_pext_u32(existingMasks[sourceIndex], compressionMask), additionalBitConversionMask);
		targetPointers[targetIndex] = existingPointers[sourceIndex];
	}

	uint32_t targetIndexForNewValue = numberEntriesBeforeAffectedSubtree + (keyInformation.mValue * insertInformation.getNumberEntriesInAffectedSubtree());
	targetMasks[targetIndexForNewValue] = newMask;
	targetPointers[targetIndexForNewValue] = newValue;

	//This is important for the tree to have fast lookup and maintain integrity!!
	targetMasks[0] = 0ul;

	assert(getMaskForLargerEntries() != mUsedEntriesMask);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTSingleThreadedNode(
	HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode, uint16_t const numberEntries,
	DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	HOTSingleThreadedDeletionInformation const & deletionInformation
) : HOTSingleThreadedNode(sourceNode.mHeight, numberEntries, discriminativeBitsRepresentation) {

	uint32_t indexOfEntryToRemove = deletionInformation.getIndexOfEntryToRemove();
	compressRangeIntoNewNode(sourceNode, deletionInformation.getCompressionMask(), 0, 0, indexOfEntryToRemove);
	compressRangeIntoNewNode(sourceNode, deletionInformation.getCompressionMask(), indexOfEntryToRemove + 1, indexOfEntryToRemove, numberEntries - indexOfEntryToRemove);

	uint32_t lastIndexInRange = deletionInformation.getAffectedBiNode().mRight.getLastIndexInRange();
	uint32_t deleteUnusedBitMask = ~_pext_u32(deletionInformation.getAffectedBiNode().mDiscriminativeBitMask, deletionInformation.getCompressionMask());

	for(uint32_t i=deletionInformation.getAffectedBiNode().mLeft.mFirstIndexInRange; i < lastIndexInRange; ++i) {
		mPartialKeys.mEntries[i] = mPartialKeys.mEntries[i] & deleteUnusedBitMask;
	}

	//This is important for the tree to have fast lookup and maintain integrity!! the first mask always is zero!!
	mPartialKeys.mEntries[0] = 0u;

	assert(getMaskForLargerEntries() != mUsedEntriesMask);
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTSingleThreadedNode(
	HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode, uint16_t const numberEntries, DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	HOTSingleThreadedDeletionInformation const & deletionInformation, hot::commons::DiscriminativeBit const & keyInformation, HOTSingleThreadedChildPointer const & newValue
) : HOTSingleThreadedNode(sourceNode.mHeight, numberEntries, discriminativeBitsRepresentation) {

	const PartialKeyConversionInformation & conversionInformation = getConversionInformationForCompressionMask(
		deletionInformation.getCompressionMask(), keyInformation
	);
	
	uint32_t numberEntriesInSourceNodeToCopy = numberEntries - 1;
	uint32_t newValueIndex = keyInformation.mValue * numberEntriesInSourceNodeToCopy;
	getPointers()[newValueIndex] = newValue;
	mPartialKeys.mEntries[newValueIndex] = 0; //actual bit will be added by add HighBitToMasksInRightHalf

	uint32_t targetIndexOffset = 1 - keyInformation.mValue;
	copyAndRemove(sourceNode, deletionInformation, conversionInformation.mConversionMask, targetIndexOffset);

	addHighBitToMasksInRightHalf((keyInformation.mValue * (numberEntriesInSourceNodeToCopy  - 1)) + 1);

	//This is important for the tree to have fast lookup and maintain integrity!! the first mask always is zero!!
	mPartialKeys.mEntries[0] = 0u;
};


template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename LeftNodeType, typename RightNodeType> inline HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTSingleThreadedNode(
	uint16_t const numberEntries, DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	LeftNodeType const & leftSourceNode,
	uint32_t leftRecodingMask,
	RightNodeType const & rightSourceNode,
	uint32_t rightRecodingMask,
	HOTSingleThreadedDeletionInformation const & deletionInformation,
	bool entryToDeleteIsInRightSide
) : HOTSingleThreadedNode(leftSourceNode.mHeight, numberEntries, discriminativeBitsRepresentation) {
	assert(leftSourceNode.mHeight == rightSourceNode.mHeight);
	assert(numberEntries == (leftSourceNode.getNumberEntries() + rightSourceNode.getNumberEntries() - 1));
	uint32_t entriesInLeftSide = leftSourceNode.getNumberEntries();
	//in right side
	if(entryToDeleteIsInRightSide) {
		copyAndRecode(leftSourceNode, leftRecodingMask, 0);
		copyAndRemove(rightSourceNode, deletionInformation, rightRecodingMask, entriesInLeftSide);
		addHighBitToMasksInRightHalf(entriesInLeftSide);
	} else {
		copyAndRemove(leftSourceNode, deletionInformation, leftRecodingMask, 0);
		copyAndRecode(rightSourceNode, rightRecodingMask, entriesInLeftSide - 1);
		addHighBitToMasksInRightHalf(entriesInLeftSide  - 1);
	}

	//ensure that first entry is always zero
	mPartialKeys.mEntries[0] = 0;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceNodeType> void HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::copyAndRemove(
	SourceNodeType const & sourceNode, HOTSingleThreadedDeletionInformation const & deletionInformation, uint32_t sourceRecodingMask, uint32_t targetStartIndex
) {
	uint32_t numberSourceEntries = sourceNode.getNumberEntries();
	uint32_t indexOfEntryToRemove = deletionInformation.getIndexOfEntryToRemove();
	uint32_t compressionMask = deletionInformation.getCompressionMask();
	HOTSingleThreadedChildPointer const* sourceValues = sourceNode.getPointers();

	for(uint32_t i=0; i < indexOfEntryToRemove; ++i) {
		size_t writeIndex = i + targetStartIndex;
		mPartialKeys.mEntries[writeIndex] = _pdep_u32(_pext_u32(sourceNode.mPartialKeys.mEntries[i], compressionMask), sourceRecodingMask);
		mFirstChildPointer[writeIndex] = sourceValues[i];
	}

	for(uint32_t i=indexOfEntryToRemove + 1; i < numberSourceEntries; ++i) {
		size_t writeIndex = i + targetStartIndex - 1;
		mPartialKeys.mEntries[writeIndex] = _pdep_u32(_pext_u32(sourceNode.mPartialKeys.mEntries[i], compressionMask), sourceRecodingMask);
		mFirstChildPointer[writeIndex] = sourceValues[i];
	}

	uint32_t lastIndexInRange = deletionInformation.getAffectedBiNode().mRight.getLastIndexInRange();
	uint32_t deleteUnusedBitMask = ~_pdep_u32(_pext_u32(deletionInformation.getAffectedBiNode().mDiscriminativeBitMask, deletionInformation.getCompressionMask()), sourceRecodingMask);
	for(uint32_t i=deletionInformation.getAffectedBiNode().mLeft.mFirstIndexInRange; i < lastIndexInRange; ++i) {
		uint32_t targetIndex = i + targetStartIndex;
		mPartialKeys.mEntries[targetIndex] = mPartialKeys.mEntries[targetIndex] & deleteUnusedBitMask;
	}
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceNodeType>
	void HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::copyAndRecode(
		SourceNodeType const & sourceNode, uint32_t recodingMask, uint32_t targetStartIndex
) {
	uint32_t numberSourceEntries = sourceNode.getNumberEntries();
	for(uint32_t i=0; i < numberSourceEntries; ++i) {
		mPartialKeys.mEntries[targetStartIndex + i] = _pdep_u32(sourceNode.mPartialKeys.mEntries[i], recodingMask);
	}
	std::memmove(mFirstChildPointer + targetStartIndex, sourceNode.getPointers(), numberSourceEntries * sizeof(HOTSingleThreadedChildPointer));
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> void HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::addHighBitToMasksInRightHalf(
	uint32_t firstIndexInRightHalf
) {
	uint32_t maskForHighestBit = mDiscriminativeBitsRepresentation.getMaskForHighestBit();
	uint32_t totalNumberEntries = getNumberEntries();
	for(uint32_t i=firstIndexInRightHalf; i < totalNumberEntries; ++i) {
		mPartialKeys.mEntries[i] = mPartialKeys.mEntries[i] | maskForHighestBit;
	}
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline HOTSingleThreadedChildPointer const * HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::search(uint8_t const * keyBytes) const {
	return getPointers() + toResultIndex(mPartialKeys.search(mDiscriminativeBitsRepresentation.extractMask(keyBytes)));
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>
inline HOTSingleThreadedChildPointer* HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::searchForInsert(hot::commons::SearchResultForInsert & searchResultOut, uint8_t const * keyBytes) const {
	uint32_t resultIndex = toResultIndex(mPartialKeys.search(mDiscriminativeBitsRepresentation.extractMask(keyBytes)));
	searchResultOut.init(resultIndex, mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex);
	return mFirstChildPointer + resultIndex;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline hot::commons::InsertInformation HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getInsertInformation(
	uint entryIndex, hot::commons::DiscriminativeBit const & discriminativeBit
) const {
	PartialKeyType existingEntryMask = mPartialKeys.mEntries[entryIndex];
	assert(([&]() -> bool {
		uint resultIndex = toResultIndex(mPartialKeys.search(existingEntryMask));
		bool isCorrectResultIndex = resultIndex == entryIndex;
		if(!isCorrectResultIndex) {
			std::cout << "Result Index is :: " << resultIndex << " but expected entry index " << entryIndex;
		};
		return isCorrectResultIndex;
	})());

	PartialKeyType prefixBits = mDiscriminativeBitsRepresentation.template getPrefixBitsMask<PartialKeyType>(discriminativeBit);
	PartialKeyType subtreePrefixMask = existingEntryMask & prefixBits;

	uint32_t affectedSubtreeMask = mPartialKeys.getAffectedSubtreeMask(prefixBits, subtreePrefixMask) & mUsedEntriesMask;

	assert(affectedSubtreeMask != 0);
	uint32_t firstIndexInAffectedSubtree = __builtin_ctz(affectedSubtreeMask);
	uint32_t numberEntriesInAffectedSubtree = _mm_popcnt_u32(affectedSubtreeMask);

	return { subtreePrefixMask, firstIndexInAffectedSubtree, numberEntriesInAffectedSubtree, discriminativeBit };
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline HOTSingleThreadedDeletionInformation HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getDeletionInformation(
	uint32_t indexOfEntryToRemove
) const {
	uint32_t compressionMask = mPartialKeys.getRelevantBitsForAllExceptOneEntry(getNumberEntries(), indexOfEntryToRemove);
	return HOTSingleThreadedDeletionInformation(toChildPointer(), compressionMask, indexOfEntryToRemove, getParentBiNodeInformationOfEntry(indexOfEntryToRemove));
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  inline
HOTSingleThreadedChildPointer HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::addEntry(
	hot::commons::InsertInformation const & insertInformation, HOTSingleThreadedChildPointer const & newValue
) const
{
	uint16_t newNumberEntries = getNumberEntries() + 1;
	HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType> const & self = *this;

	return mDiscriminativeBitsRepresentation.insert(insertInformation.mKeyInformation,
            [&](auto const & newDiscriminativeBitsRepresentation)  {
		using NewConstDiscriminativeBitsRepresentationType = typename std::remove_reference<decltype(newDiscriminativeBitsRepresentation)>::type;
		using IntermediateNewDiscriminativeBitsRepresentationType = typename std::remove_const<NewConstDiscriminativeBitsRepresentationType>::type;
		typedef typename ToDiscriminativeBitsRepresentation<IntermediateNewDiscriminativeBitsRepresentationType, PartialKeyType>::Type NewDiscriminativeBitsRepresentationType;
		return (newDiscriminativeBitsRepresentation.calculateNumberBitsUsed() <= (sizeof(PartialKeyType) * 8))
			? (new (newNumberEntries) HOTSingleThreadedNode<NewDiscriminativeBitsRepresentationType,
                    typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType, PartialKeyType>::Type>(
				self, newNumberEntries, newDiscriminativeBitsRepresentation, insertInformation, newValue
			  ))->toChildPointer()
			: (new (newNumberEntries) HOTSingleThreadedNode<typename ToDiscriminativeBitsRepresentation<NewDiscriminativeBitsRepresentationType,
                    typename NextPartialKeyType<PartialKeyType>::Type>::Type,
					typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType,
                    typename NextPartialKeyType<PartialKeyType>::Type>::Type> (
				self, newNumberEntries, newDiscriminativeBitsRepresentation, insertInformation, newValue
			))->toChildPointer();
	});
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  inline HOTSingleThreadedChildPointer HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::removeEntry(HOTSingleThreadedDeletionInformation const & deletionInformation) const {
	size_t numberEntries = getNumberEntries();
	size_t newNumberEntries = numberEntries - 1;

	HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType> const &self = *this;

	return (newNumberEntries > 1)
		? hot::commons::extractAndExecuteWithCorrectMaskAndDiscriminativeBitsRepresentation(mDiscriminativeBitsRepresentation, deletionInformation.getCompressionMask(),
			[&](auto const &finalDiscriminativeBitsRepresentation, auto maximumMask) {
				using FinalDiscriminativeBitsRepresentationType = typename std::remove_const<
					typename std::remove_reference<decltype(finalDiscriminativeBitsRepresentation)>::type
				>::type;

				return (new(
					newNumberEntries) HOTSingleThreadedNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
					self, newNumberEntries, finalDiscriminativeBitsRepresentation, deletionInformation
				))->toChildPointer();
			}
		)
		: getPointers()[1-deletionInformation.getIndexOfEntryToRemove()];
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  inline HOTSingleThreadedChildPointer HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::removeAndAddEntry(
	HOTSingleThreadedDeletionInformation const & deletionInformation, hot::commons::DiscriminativeBit const & keyInformation, HOTSingleThreadedChildPointer const & newValue
) const {
	size_t numberEntries = getNumberEntries();
	size_t newNumberEntries = numberEntries;

	HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType> const &self = *this;

	return (newNumberEntries > 2)
			? hot::commons::extractAndAddAndExecuteWithCorrectMaskAndDiscriminativeBitsRepresentation(mDiscriminativeBitsRepresentation, deletionInformation.getCompressionMask(), keyInformation,
				[&](auto const &finalDiscriminativeBitsRepresentation, auto maximumMask) {
					using FinalDiscriminativeBitsRepresentationType = typename std::remove_const<
						typename std::remove_reference<decltype(finalDiscriminativeBitsRepresentation)>::type
					>::type;
					return (new (newNumberEntries) HOTSingleThreadedNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
						self, newNumberEntries, finalDiscriminativeBitsRepresentation, deletionInformation, keyInformation, newValue
					))->toChildPointer();
				}
			)
			: hot::commons::createTwoEntriesNode<HOTSingleThreadedChildPointer, HOTSingleThreadedNode>(hot::commons::BiNode<HOTSingleThreadedChildPointer>::createFromExistingAndNewEntry(
				keyInformation, getPointers()[1-deletionInformation.getIndexOfEntryToRemove()], newValue
			))->toChildPointer();
};


template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  inline
HOTSingleThreadedChildPointer HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::compressEntries(uint32_t firstIndexInRange, uint16_t numberEntriesInRange) const
{
	PartialKeyType relevantBits = mPartialKeys.getRelevantBitsForRange(firstIndexInRange, numberEntriesInRange);

	if(numberEntriesInRange > 1) {
		HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType> const &self = *this;

		return hot::commons::extractAndExecuteWithCorrectMaskAndDiscriminativeBitsRepresentation(mDiscriminativeBitsRepresentation, relevantBits,
			[&](auto const &finalDiscriminativeBitsRepresentation, auto maximumMask) {
			using FinalDiscriminativeBitsRepresentationType = typename std::remove_const<
				typename std::remove_reference<decltype(finalDiscriminativeBitsRepresentation)>::type
			>::type;

			return (new (numberEntriesInRange) HOTSingleThreadedNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
				self, numberEntriesInRange, finalDiscriminativeBitsRepresentation, relevantBits, firstIndexInRange, numberEntriesInRange
			))->toChildPointer();
		});
	} else {
		return getPointers()[firstIndexInRange];
	}
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  inline
HOTSingleThreadedChildPointer HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::compressEntriesAndAddOneEntryIntoNewNode(
	uint32_t firstIndexInRange, uint16_t numberEntriesInRange, hot::commons::InsertInformation const & insertInformation, HOTSingleThreadedChildPointer const & newValue
) const
{
	if(numberEntriesInRange != 1) {
		PartialKeyType relevantBits = mPartialKeys.getRelevantBitsForRange(firstIndexInRange, numberEntriesInRange);
		uint16_t nextNumberEntries = numberEntriesInRange + 1;
		HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType> const & self = *this;

		return mDiscriminativeBitsRepresentation.extract(relevantBits, [&](auto const & intermediateDiscriminativeBitsRepresentation)  {
			return intermediateDiscriminativeBitsRepresentation.insert(insertInformation.mKeyInformation, [&](auto const & newDiscriminativeBitsRepresentation) {
				return newDiscriminativeBitsRepresentation.executeWithCorrectMaskAndDiscriminativeBitsRepresentation([&](auto const & finalDiscriminativeBitsRepresentation, auto maximumMask) {
					using FinalDiscriminativeBitsRepresentationType = typename std::remove_const<
						typename std::remove_reference<decltype(finalDiscriminativeBitsRepresentation)>::type
					>::type;

					return (new (nextNumberEntries) HOTSingleThreadedNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
						self, nextNumberEntries, finalDiscriminativeBitsRepresentation, relevantBits, firstIndexInRange, numberEntriesInRange, insertInformation, newValue
					))->toChildPointer();
				});
			});
		});
	} else {
		hot::commons::BiNode<HOTSingleThreadedChildPointer> const & binaryNode = hot::commons::BiNode<HOTSingleThreadedChildPointer>::createFromExistingAndNewEntry(insertInformation.mKeyInformation, getPointers()[firstIndexInRange], newValue);
		return hot::commons::createTwoEntriesNode<HOTSingleThreadedChildPointer, HOTSingleThreadedNode>(binaryNode)->toChildPointer();
	}
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  inline
uint16_t HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getLeastSignificantDiscriminativeBitForEntry(unsigned int entryIndex) const {
	unsigned int nextEntryIndex = entryIndex + 1;
	uint32_t mask = nextEntryIndex < getNumberEntries()
		? (static_cast<uint32_t>(mPartialKeys.mEntries[entryIndex]) | static_cast<uint32_t>(mPartialKeys.mEntries[nextEntryIndex]))
		: mPartialKeys.mEntries[entryIndex];

	return mDiscriminativeBitsRepresentation.getLeastSignificantBitIndex(mask);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>
inline hot::commons::BiNode<HOTSingleThreadedChildPointer> HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::split(
	hot::commons::InsertInformation const & insertInformation, HOTSingleThreadedChildPointer const & newValue) const
{
	uint32_t largerEntries = getMaskForLargerEntries();
	assert(largerEntries != 0);

	uint32_t numberLargerEntries = _mm_popcnt_u32(largerEntries);
	uint32_t numberSmallerEntries = getNumberEntries() - numberLargerEntries;

	assert(insertInformation.getNumberEntriesInAffectedSubtree() != getNumberEntries());

	uint16_t newLevel = mHeight + 1;

	return (insertInformation.mFirstIndexInAffectedSubtree >= numberSmallerEntries)
		   ? hot::commons::BiNode<HOTSingleThreadedChildPointer> { mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex, newLevel, compressEntries(0, numberSmallerEntries), compressEntriesAndAddOneEntryIntoNewNode(numberSmallerEntries, numberLargerEntries, insertInformation, newValue) }
		   : hot::commons::BiNode<HOTSingleThreadedChildPointer> { mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex, newLevel, compressEntriesAndAddOneEntryIntoNewNode(0, numberSmallerEntries, insertInformation, newValue), compressEntries(numberSmallerEntries, numberLargerEntries) };
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline HOTSingleThreadedChildPointer HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::toChildPointer() const {
	return { mNodeType, this };
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>
idx::contenthelpers::OptionalValue<std::pair<uint16_t, HOTSingleThreadedChildPointer*>>
	HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getDirectNeighbourOfEntry(size_t entryIndex) const {
	idx::contenthelpers::OptionalValue<std::pair<uint16_t, HOTSingleThreadedChildPointer*>> result;
	size_t numberOfEntries = getNumberEntries();
	bool bitValueOfEntry = mPartialKeys.determineValueOfDiscriminatingBit(entryIndex, numberOfEntries);
	size_t indexOfPotentialNeighbour = bitValueOfEntry ? entryIndex - 1 : entryIndex + 1;
	size_t rightEntryIndex = bitValueOfEntry ? entryIndex : entryIndex - 1;
	if(mPartialKeys.determineValueOfDiscriminatingBit(indexOfPotentialNeighbour, numberOfEntries) == bitValueOfEntry) {
		result = idx::contenthelpers::OptionalValue<std::pair<uint16_t, HOTSingleThreadedChildPointer*>>(
			true,
			std::pair<uint16_t , HOTSingleThreadedChildPointer*>(
				mDiscriminativeBitsRepresentation.getLeastSignificantBitIndex(mPartialKeys.mEntries[rightEntryIndex]),
				getPointers() + indexOfPotentialNeighbour
			)
		);
	}
	return result;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>
hot::commons::BiNodeInformation HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getParentBiNodeInformationOfEntry(size_t entryIndex) const {
	size_t numberOfEntries = getNumberEntries();
	uint32_t discriminativeBitValueOfEntry = static_cast<uint32_t>(mPartialKeys.determineValueOfDiscriminatingBit(entryIndex, numberOfEntries));

	uint32_t maskOfLastEntryInLeftSubtree = mPartialKeys.mEntries[entryIndex - discriminativeBitValueOfEntry];
	uint32_t maskOfFirstEntryInRightSubtree = mPartialKeys.mEntries[entryIndex + (1 - discriminativeBitValueOfEntry)];

	uint32_t expectedPrefixBits = maskOfLastEntryInLeftSubtree&maskOfFirstEntryInRightSubtree;
	uint32_t discriminativeBitMask = (expectedPrefixBits)^maskOfFirstEntryInRightSubtree;
	uint32_t discriminativeBitPosition = mDiscriminativeBitsRepresentation.getLeastSignificantBitIndex(discriminativeBitMask);

	uint32_t prefixBitsMask = mDiscriminativeBitsRepresentation.template getPrefixBitsMask<PartialKeyType>(hot::commons::DiscriminativeBit(discriminativeBitPosition));

	uint32_t affectedSubtreeMask = mPartialKeys.getAffectedSubtreeMask(prefixBitsMask, expectedPrefixBits) & mUsedEntriesMask;

	uint32_t firstIndexInAffectedSubtree = affectedSubtreeMask == 0u ? 0u : __builtin_ctz(affectedSubtreeMask);
	uint32_t numberEntriesInAffectedSubtree = _mm_popcnt_u32(affectedSubtreeMask);

	uint32_t numberEntriesInLeftSubtree = (numberEntriesInAffectedSubtree - discriminativeBitValueOfEntry) * discriminativeBitValueOfEntry + (1 - discriminativeBitValueOfEntry);
	uint32_t numberEntriesInRightSubtree = numberEntriesInAffectedSubtree - numberEntriesInLeftSubtree;

	uint32_t firstIndexInRightSubtree = firstIndexInAffectedSubtree + numberEntriesInLeftSubtree;

	return hot::commons::BiNodeInformation(discriminativeBitPosition,
		 discriminativeBitMask,
		 hot::commons::EntriesRange(firstIndexInAffectedSubtree, numberEntriesInLeftSubtree),
		 hot::commons::EntriesRange(firstIndexInRightSubtree, numberEntriesInRightSubtree)
	);
}



template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline void HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::compressRangeIntoNewNode(
	HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const &sourceNode, uint32_t compressionMask,
	uint32_t firstIndexInSourceRange, uint32_t firstIndexInTarget, uint32_t numberEntriesInRange) {
	HOTSingleThreadedChildPointer const * __restrict__ sourcePointers = sourceNode.getPointers();
	HOTSingleThreadedChildPointer * __restrict__ targetPointers = getPointers();

	SourcePartialKeyType const * __restrict__ sourceMasks = sourceNode.mPartialKeys.mEntries;
	PartialKeyType * __restrict__ targetMasks = this->mPartialKeys.mEntries;

	uint32_t firstIndexOutOfRange = firstIndexInTarget + numberEntriesInRange;
	uint32_t sourceIndex = firstIndexInSourceRange;

	for(uint32_t targetIndex = firstIndexInTarget; targetIndex < firstIndexOutOfRange; ++targetIndex) {
		targetPointers[targetIndex] = sourcePointers[sourceIndex];
		targetMasks[targetIndex] = _pext_u32(sourceMasks[sourceIndex], compressionMask);
		++sourceIndex;
	}
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline
typename HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::PartialKeyConversionInformation HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getConversionInformation(
	HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode, hot::commons::DiscriminativeBit const & significantKeyInformation
) const
{
	return getConversionInformation(sourceNode.mDiscriminativeBitsRepresentation.getAllMaskBits(), significantKeyInformation);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourcePartialKeyType> inline
typename HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::PartialKeyConversionInformation HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getConversionInformationForCompressionMask(
	SourcePartialKeyType compressionMask, hot::commons::DiscriminativeBit const & significantKeyInformation
) const
{
	uint32_t allIntermediateMaskBits = _pext_u32(compressionMask, compressionMask);
	return getConversionInformation(allIntermediateMaskBits, significantKeyInformation);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline typename HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::PartialKeyConversionInformation
HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getConversionInformation(
	uint32_t sourceMaskBits, hot::commons::DiscriminativeBit const & significantKeyInformation
) const {
	uint32_t allTargetMaskBits = mDiscriminativeBitsRepresentation.getAllMaskBits();
	uint hasNewBit = sourceMaskBits != allTargetMaskBits;
	PartialKeyType additionalMask = mDiscriminativeBitsRepresentation.getMaskFor(significantKeyInformation);
	PartialKeyType conversionMask = allTargetMaskBits & (~(hasNewBit * additionalMask));

	return { additionalMask, conversionMask };
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline uint32_t HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getMaskForLargerEntries() const {
	uint64_t const maskWithMostSignificantBitSet = mDiscriminativeBitsRepresentation.getMaskForHighestBit();
	return mPartialKeys.findMasksByPattern(maskWithMostSignificantBitSet) & mUsedEntriesMask;
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline std::array<uint8_t, 32> HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getEntryDepths() const {
	std::array<uint8_t, 32> entryDepths;
	std::fill(entryDepths.begin(), entryDepths.end(), 0u);
	collectEntryDepth(entryDepths, 0, getNumberEntries(), 0ul, 0u);
	return entryDepths;
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline void HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::collectEntryDepth(std::array<uint8_t, 32> & entryDepths, size_t minEntryIndexInRange, size_t numberEntriesInRange, size_t currentDepth, uint32_t usedMaskBits) const {
	if(numberEntriesInRange > 1) {
		PartialKeyType mostSignificantMaskBitInRange = mDiscriminativeBitsRepresentation.getMostSignifikantMaskBit(
			mPartialKeys.mEntries[minEntryIndexInRange + numberEntriesInRange - 1] & (~usedMaskBits)
		);

		uint32_t rangeMask = ((UINT32_MAX >> (32 - numberEntriesInRange))) << minEntryIndexInRange;
		uint32_t upperEntriesMask = mPartialKeys.findMasksByPattern(mostSignificantMaskBitInRange) & rangeMask;
		size_t minimumUpperRangeEntryIndex = __tzcnt_u32(upperEntriesMask);
		size_t numberEntriesInLowerHalf = minimumUpperRangeEntryIndex - minEntryIndexInRange;
		size_t numberEntriesInUpperHalf = numberEntriesInRange - numberEntriesInLowerHalf;
		collectEntryDepth(entryDepths, minEntryIndexInRange, numberEntriesInLowerHalf, currentDepth + 1, usedMaskBits);
		collectEntryDepth(entryDepths, minimumUpperRangeEntryIndex, numberEntriesInUpperHalf, currentDepth + 1, usedMaskBits | mostSignificantMaskBitInRange);
	} else if(numberEntriesInRange == 1) {
		entryDepths[minEntryIndexInRange] = currentDepth;
	} else {
		assert(false);
	}
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline bool HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::isPartitionCorrect() const {
	std::array<int, 32> significantBitValueStack { -1 };
	std::array<uint, 32> entriesMaskStack { 0 };
	std::array<PartialKeyType, 32> prefixStack { 0 };
	std::array<PartialKeyType, 32> mostSignificantBitStack { 0 };
	std::array<PartialKeyType, 32> subtreeBitsUsedStack { 0 };
	significantBitValueStack[0] = 0;
	entriesMaskStack[0] = mUsedEntriesMask;
	PartialKeyType maskBitsUsed = mDiscriminativeBitsRepresentation.getAllMaskBits();
	mostSignificantBitStack[0] = mDiscriminativeBitsRepresentation.getMostSignifikantMaskBit(maskBitsUsed);
	subtreeBitsUsedStack[0] = maskBitsUsed & (~mostSignificantBitStack[0]);

	uint32_t entriesVisitedMask = 0;
	int stackIndex = 0;

	bool partitionIsCorrect = true;

	if(mPartialKeys.mEntries[0] != 0) {
		std::cout << "first mask is not zero" << std::endl;
		partitionIsCorrect = false;
	}

	while(partitionIsCorrect && stackIndex >= 0) {
		assert(stackIndex <= static_cast<int>(getNumberEntries()));
		uint32_t entriesMask = entriesMaskStack[stackIndex];

		int significantBitValue = significantBitValueStack[stackIndex];
		if(significantBitValue == -1) {
			if(_mm_popcnt_u32(entriesMask) == 1) {
				uint entryIndex = __tzcnt_u32(entriesMask);
				uint32_t entryMask = 1l << entryIndex;
				if((entryMask & entriesVisitedMask) != 0) {
					std::cout << "Mask for path [";
					for(int i=0; i < stackIndex; ++i) {
						std::cout << (significantBitValueStack[i] - 1) << std::endl;
					}
					std::cout << "] maps to entry " << entryIndex << " which was previously used." << std::endl;
					partitionIsCorrect = false;
				} else if(entryIndex > 0 && ((entriesVisitedMask & (entryMask >> 1)) == 0)) {
					std::cout << "Mask for path [";
					for(int i=0; i < stackIndex; ++i) {
						std::cout << significantBitValueStack[i] << std::endl;
					}
					std::cout << "] maps to entry " << entryIndex << " which does not continuosly follow" <<
							  " the previous entries ( " <<  entriesVisitedMask << ")." << std::endl;

					partitionIsCorrect = false;
				} else {
					entriesVisitedMask |= entryMask;
					--stackIndex;
					++significantBitValueStack[stackIndex];
				}
			} else {
				//calculate prefix + mostSignificantBit
				uint parentStackIndex = stackIndex - 1;
				uint32_t currentSubtreeMask = entriesMaskStack[stackIndex];
				assert(currentSubtreeMask != 0);
				unsigned int firstBitInRange = _tzcnt_u32(currentSubtreeMask);
				PartialKeyType subTreeBits = mPartialKeys.getRelevantBitsForRange(firstBitInRange, _mm_popcnt_u32(currentSubtreeMask));
				mostSignificantBitStack[stackIndex] = mDiscriminativeBitsRepresentation.getMostSignifikantMaskBit(subTreeBits);
				prefixStack[stackIndex] = prefixStack[parentStackIndex] + (significantBitValueStack[parentStackIndex] * mostSignificantBitStack[parentStackIndex]);
				subtreeBitsUsedStack[stackIndex] = subTreeBits & ~(mostSignificantBitStack[stackIndex]);

				++significantBitValueStack[stackIndex];
			}
		} else if(significantBitValue <= 1) {
			PartialKeyType subtreePrefixMask = prefixStack[stackIndex] + significantBitValueStack[stackIndex] * mostSignificantBitStack[stackIndex];
			PartialKeyType subtreeMatcherMask = subtreePrefixMask | subtreeBitsUsedStack[stackIndex];
			uint32_t affectedSubtree = mPartialKeys.findMasksByPattern(subtreePrefixMask) & mPartialKeys.search(subtreeMatcherMask) & mUsedEntriesMask;

			++stackIndex;

			entriesMaskStack[stackIndex] = affectedSubtree;
			significantBitValueStack[stackIndex] = -1;
		} else {
			--stackIndex;
			if(stackIndex >= 0) {
				++significantBitValueStack[stackIndex];
			}
		}
	}

	std::map<uint16_t, uint16_t> const & extractionMaskMapping = getExtractionMaskToEntriesMasksMapping();

	std::array<PartialKeyType, 32> correctOrderExtractionMask;

	int numberBits = extractionMaskMapping.size();
	for(size_t i=0; i < getNumberEntries(); ++i) {
		PartialKeyType rawMask = mPartialKeys.mEntries[i];
		PartialKeyType reorderedMask = 0u;

		int targetBit = numberBits - 1;
		for(auto extractionBitMapping : extractionMaskMapping) {
			uint16_t sourceBitPosition = extractionBitMapping.second;
			reorderedMask |= (((rawMask >> sourceBitPosition) & 1) << targetBit);
			--targetBit;
		}

		correctOrderExtractionMask[i] = reorderedMask;

		if(i > 0) {
			bool isCorrectOrder = correctOrderExtractionMask[i - 1] < correctOrderExtractionMask[i];
			if(!isCorrectOrder) {
				std::cout << "Mask of entry :: " << i << " is smaller than its predecessor" << std::endl;
				partitionIsCorrect = false;
			}
		}
	}

	if(!partitionIsCorrect) {
		mPartialKeys.printMasks(mUsedEntriesMask, extractionMaskMapping);
	}

	return partitionIsCorrect;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline size_t HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getNodeSizeInBytes() const {
	return hot::commons::NodeAllocationInformations<HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>>::getAllocationInformation(getNumberEntries()).mTotalSizeInBytes;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline std::map<uint16_t, uint16_t> HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::getExtractionMaskToEntriesMasksMapping() const {
	std::map<uint16_t, uint16_t> maskBitMapping;
	for(uint16_t extractionBitIndex : mDiscriminativeBitsRepresentation.getDiscriminativeBits()) {
		uint32_t singleBitMask = mDiscriminativeBitsRepresentation.getMaskFor({ extractionBitIndex, 1 });
		uint maskBitPosition = __tzcnt_u32(singleBitMask);
		maskBitMapping[extractionBitIndex] = maskBitPosition;
	}
	return maskBitMapping;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline void HOTSingleThreadedNode<DiscriminativeBitsRepresentation, PartialKeyType>::printPartialKeysWithMappings(
	std::ostream &out) const {
	return mPartialKeys.printMasks(mUsedEntriesMask, getExtractionMaskToEntriesMasksMapping(), out);
};

template<typename LeftDiscriminativeBitsRepresentation, typename LeftPartialKeyType, typename RightDiscriminativeBitsRepresentation, typename RightPartialKeyType> inline HOTSingleThreadedChildPointer mergeNodesAndRemoveEntry(
	hot::commons::NodeMergeInformation const & mergeInformation, HOTSingleThreadedNode<LeftDiscriminativeBitsRepresentation, LeftPartialKeyType> const & left, HOTSingleThreadedNode<RightDiscriminativeBitsRepresentation, RightPartialKeyType> const & right, HOTSingleThreadedDeletionInformation const & deletionInformation, bool entryToDeleteIsInRightSide
) {
	return mergeInformation.executeWithMergedDiscriminativeBitsRepresentationAndFittingPartialKeyType([&](auto const & mergedDiscriminativeBitsRepresentation, auto maximumMask, uint32_t leftRecodingMask, uint32_t rightRecodingMask) {
		using FinalDiscriminativeBitsRepresentationType = typename std::remove_const<
			typename std::remove_reference<decltype(mergedDiscriminativeBitsRepresentation)>::type
		>::type;

		size_t newNumberEntries = left.getNumberEntries() + right.getNumberEntries() - 1;
		return (new (newNumberEntries) HOTSingleThreadedNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
			newNumberEntries, mergedDiscriminativeBitsRepresentation,
			left, leftRecodingMask,
			right, rightRecodingMask,
			deletionInformation,
			entryToDeleteIsInRightSide
		))->toChildPointer();
	});
};


inline HOTSingleThreadedChildPointer mergeNodesAndRemoveEntryIfPossible(uint32_t rootDiscriminativeBitPosition, HOTSingleThreadedChildPointer const &left,
												HOTSingleThreadedChildPointer const &right,
												HOTSingleThreadedDeletionInformation const &deletionInformation,
												bool entryToDeleteIsInRightSide) {
	HOTSingleThreadedChildPointer const & deletionSide = entryToDeleteIsInRightSide ? right : left;
	HOTSingleThreadedChildPointer const & otherSide = entryToDeleteIsInRightSide ? left : right;

	return (deletionSide.getNumberEntries() > 2)
		? left.executeForSpecificNodeType(false, [&](auto const & leftNode) {
			return right.executeForSpecificNodeType(false, [&](auto const & rightNode) {
				hot::commons::NodeMergeInformation const & mergeInformation =
					entryToDeleteIsInRightSide
					? rightNode.mDiscriminativeBitsRepresentation.extract(deletionInformation.getCompressionMask(), [&](auto const &rightDiscriminativeBitsRepresentation) {
						return hot::commons::NodeMergeInformation(rootDiscriminativeBitPosition, leftNode.mDiscriminativeBitsRepresentation, rightDiscriminativeBitsRepresentation);
					})
					: leftNode.mDiscriminativeBitsRepresentation.extract(deletionInformation.getCompressionMask(), [&](auto const &leftDiscriminativeBitsRepresentation) {
						return hot::commons::NodeMergeInformation(rootDiscriminativeBitPosition, leftDiscriminativeBitsRepresentation, rightNode.mDiscriminativeBitsRepresentation);
					});

				return mergeInformation.isValid() ? mergeNodesAndRemoveEntry(mergeInformation, leftNode, rightNode, deletionInformation, entryToDeleteIsInRightSide) : HOTSingleThreadedChildPointer();
			});
		})
		: otherSide.executeForSpecificNodeType(false, [&](auto const & otherNode) {
			assert(deletionSide.getNumberEntries() == 2);
			hot::commons::InsertInformation const & insertInformation = otherNode.getInsertInformation((otherSide.getNumberEntries() - 1) * entryToDeleteIsInRightSide, hot::commons::DiscriminativeBit(rootDiscriminativeBitPosition, entryToDeleteIsInRightSide));
			return otherNode.addEntry(insertInformation, *deletionInformation.getDirectNeighbourIfAvailable());
		});
}

} }

#endif

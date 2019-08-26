#ifndef __HOT__ROWEX__SIMD_COB_TRIE_NODE__
#define __HOT__ROWEX__SIMD_COB_TRIE_NODE__

#include <cstdint>
#include <algorithm>
#include <iostream>

#include <hot/commons/BiNode.hpp>
#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/SparsePartialKeys.hpp>
#include <hot/commons/MultiMaskPartialKeyMapping.hpp>
#include <hot/commons/NodeAllocationInformations.hpp>
#include <hot/commons/SearchResultForInsert.hpp>
#include <hot/commons/TwoEntriesNode.hpp>

#include "hot/rowex/HOTRowexNodeBase.hpp"
#include "hot/rowex/HOTRowexNodeInterface.hpp"
#include "hot/rowex/HOTRowexChildPointer.hpp"

namespace hot { namespace rowex {

constexpr uint32_t calculatePointerSize(uint16_t const numberEntries) {
	constexpr uint32_t childPointerSize = static_cast<uint32_t>(sizeof(HOTRowexChildPointer));
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

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  void* HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::operator new (size_t , uint16_t const numberEntries) {
	hot::commons::NodeAllocationInformation const & allocationInformation = hot::commons::NodeAllocationInformations<HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>>::getAllocationInformation(numberEntries);
	assert(numberEntries >= 2);

	void* memoryForNode = nullptr;
	uint error = posix_memalign(&memoryForNode, SIMD_COB_TRIE_NODE_ALIGNMENT, allocationInformation.mTotalSizeInBytes);
	if(error != 0) {
		//"Got error on alignment"
		throw std::bad_alloc();
	}
	return memoryForNode;
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> void HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::operator delete (void * rawMemory) {
	free(rawMemory);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline hot::commons::NodeAllocationInformation HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getNodeAllocationInformation(uint16_t const numberEntries) {
	constexpr uint32_t entriesMasksBaseSize = static_cast<uint32_t>(sizeof(hot::commons::SparsePartialKeys<PartialKeyType>));
	constexpr uint32_t baseSize = static_cast<uint32_t>(sizeof(HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>)) - entriesMasksBaseSize;

	uint32_t pointersSize = calculatePointerSize(numberEntries);
	uint16_t pointerOffset = hot::commons::SparsePartialKeys<PartialKeyType>::estimateSize(numberEntries) + baseSize;
	uint32_t rawSize = pointersSize + pointerOffset;
	assert((rawSize % 8) == 0);

	return hot::commons::NodeAllocationInformation(convertNumbeEntriesToEntriesMask(numberEntries), rawSize, pointerOffset);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTRowexNode(uint16_t const height, uint16_t const numberEntries, DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation)
	: HOTRowexNodeBase(height, HOTRowexNode::getNodeAllocationInformation(numberEntries)), mDiscriminativeBitsRepresentation(discriminativeBitsRepresentation)
{
}

//add entry into node
template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTRowexNode(
	HOTRowexNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
	uint16_t const newNumberEntries,
	DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	hot::commons::InsertInformation const & insertInformation,
	HOTRowexChildPointer const & newValue
) : HOTRowexNode (sourceNode.mHeight, newNumberEntries, discriminativeBitsRepresentation) {

	unsigned int oldNumberEntries = (newNumberEntries - 1u);

	HOTRowexChildPointer const * __restrict__ existingPointers = sourceNode.getPointers();
	HOTRowexChildPointer * __restrict__ targetPointers = this->getPointers();

	SourcePartialKeyType __restrict__ const * existingMasks = sourceNode.mPartialKeys.mEntries;
	PartialKeyType __restrict__ * targetMasks = mPartialKeys.mEntries;

	PartialKeyConversionInformation const & conversionInformation = getConversionInformation(sourceNode, insertInformation);
	hot::commons::DiscriminativeBit const & keyInformation = insertInformation.mKeyInformation;

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
template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTRowexNode(
	HOTRowexNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
	uint16_t const newNumberEntries,
	DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	SourcePartialKeyType compressionMask,
	uint32_t firstIndexInRange,
	uint32_t numberEntriesInRange
) : HOTRowexNode(sourceNode.mHeight, newNumberEntries, discriminativeBitsRepresentation) {

	HOTRowexChildPointer const * __restrict__ sourcePointers = sourceNode.getPointers();
	HOTRowexChildPointer* __restrict__ targetPointers = this->getPointers();

	SourcePartialKeyType const * __restrict__ sourceMasks = sourceNode.mPartialKeys.mEntries;
	PartialKeyType * __restrict__ targetMasks = mPartialKeys.mEntries;

	targetPointers[0] = sourcePointers[firstIndexInRange];
	//This is important for the tree to have fast lookup and maintain integrity!! the first mask always is zero!!
	targetMasks[0] = 0u;

	for(uint32_t targetIndex = 1; targetIndex < numberEntriesInRange; ++targetIndex) {
		uint32_t sourceIndex = firstIndexInRange + targetIndex;
		targetPointers[targetIndex] = sourcePointers[sourceIndex];
		targetMasks[targetIndex] = _pext_u32(sourceMasks[sourceIndex], compressionMask);
	}

	assert(getMaskForLargerEntries() != this->mUsedEntriesMask);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::HOTRowexNode(
	HOTRowexNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
	uint16_t const newNumberEntries,
	DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
	SourcePartialKeyType compressionMask,
	uint32_t firstIndexInRange,
	uint32_t numberEntriesInRange,
	hot::commons::InsertInformation const & insertInformation,
	HOTRowexChildPointer const & newValue
) : HOTRowexNode(sourceNode.mHeight, newNumberEntries, discriminativeBitsRepresentation) {
	PartialKeyConversionInformation const & conversionInformation = getConversionInformationForCompressionMask(compressionMask, insertInformation);
	hot::commons::DiscriminativeBit const & keyInformation = insertInformation.mKeyInformation;

	PartialKeyType additionalBitConversionMask = conversionInformation.mConversionMask;

	PartialKeyType newMask = static_cast<PartialKeyType>(_pdep_u32(_pext_u32(insertInformation.mSubtreePrefixPartialKey, compressionMask), additionalBitConversionMask))
		| (keyInformation.mValue * conversionInformation.mAdditionalMask);

	//%32 to ensure it works in case of 0 for affected subtree
	uint32_t originalFirstIndexInAffectedSubtree =	insertInformation.getFirstIndexInAffectedSubtree();

	unsigned int numberEntriesInAffectedSubtree = insertInformation.getNumberEntriesInAffectedSubtree();
	unsigned int numberEntriesBeforeAffectedSubtree = originalFirstIndexInAffectedSubtree - firstIndexInRange;

	HOTRowexChildPointer const * __restrict__ existingPointers = sourceNode.getPointers();
	HOTRowexChildPointer * __restrict__ targetPointers = this->getPointers();

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

	for (unsigned int indexInAffectedSubtree = 0; indexInAffectedSubtree <  numberEntriesInAffectedSubtree; ++indexInAffectedSubtree) {
		unsigned int sourceIndex = originalFirstIndexInAffectedSubtree + indexInAffectedSubtree;
		unsigned int targetIndex = firstTargetIndexInAffectedSubtree + indexInAffectedSubtree;

		targetMasks[targetIndex] = _pdep_u32(_pext_u32(existingMasks[sourceIndex], compressionMask), additionalBitConversionMask) | additionalMaskForExistingEntries;
		targetPointers[targetIndex] = existingPointers[sourceIndex];
	}

	unsigned int sourceNumberEntriesInRangeUntilEndOfAffectedSubtree = numberEntriesBeforeAffectedSubtree + numberEntriesInAffectedSubtree;
	unsigned int sourceIndexAfterAffectedSubtree = firstIndexInRange + sourceNumberEntriesInRangeUntilEndOfAffectedSubtree;
	unsigned int firstTargeIndexAfterAffectedSubtree = sourceNumberEntriesInRangeUntilEndOfAffectedSubtree + 1;
	unsigned int numberEntriesAfterAffectedSubtree = numberEntriesInRange - sourceNumberEntriesInRangeUntilEndOfAffectedSubtree;
	for(unsigned int indexAfterAffectedSubtree = 0; indexAfterAffectedSubtree < numberEntriesAfterAffectedSubtree; ++indexAfterAffectedSubtree) {
		unsigned int sourceIndex = sourceIndexAfterAffectedSubtree + indexAfterAffectedSubtree;
		unsigned int targetIndex = firstTargeIndexAfterAffectedSubtree + indexAfterAffectedSubtree;
		targetMasks[targetIndex] = _pdep_u32(_pext_u32(existingMasks[sourceIndex], compressionMask), additionalBitConversionMask);
		targetPointers[targetIndex] = existingPointers[sourceIndex];
	}

	uint32_t targetIndexForNewValue = numberEntriesBeforeAffectedSubtree + (keyInformation.mValue * numberEntriesInAffectedSubtree);
	targetMasks[targetIndexForNewValue] = newMask;
	targetPointers[targetIndexForNewValue] = newValue;

	//This is important for the tree to have fast lookup and maintain integrity!!
	targetMasks[0] = 0ul;

	assert(getMaskForLargerEntries() != this->mUsedEntriesMask);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline HOTRowexChildPointer const * HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::search(uint8_t const * keyBytes) const {
	return this->getPointers() + this->toResultIndex(mPartialKeys.search(mDiscriminativeBitsRepresentation.extractMask(keyBytes)));
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline HOTRowexChildPointer* HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::searchForInsert(hot::commons::SearchResultForInsert & searchResultOut, uint8_t const * keyBytes) const {
	uint32_t resultIndex = this->toResultIndex(mPartialKeys.search(mDiscriminativeBitsRepresentation.extractMask(keyBytes)));
	searchResultOut.init(resultIndex, mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex);
	return this->mFirstChildPointer + resultIndex;
}

inline void reportInvalidResultIndex(uint resultIndex, uint entryIndex);

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline hot::commons::InsertInformation HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getInsertInformation(
	uint entryIndex, hot::commons::DiscriminativeBit const & discriminativeBit
) const {
	PartialKeyType existingEntryMask = mPartialKeys.mEntries[entryIndex];
	assert(([&]() -> bool {
		uint resultIndex = this->toResultIndex(mPartialKeys.search(existingEntryMask));
		bool isCorrectResultIndex = resultIndex == entryIndex;
		if(!isCorrectResultIndex) {
			reportInvalidResultIndex(resultIndex, entryIndex);
		};
		return isCorrectResultIndex;
	})());

	PartialKeyType prefixBits = mDiscriminativeBitsRepresentation.template getPrefixBitsMask<PartialKeyType>(discriminativeBit);
	PartialKeyType subtreePrefixMask = existingEntryMask & prefixBits;

	uint32_t affectedSubtreeMask = mPartialKeys.getAffectedSubtreeMask(prefixBits, subtreePrefixMask) & this->mUsedEntriesMask;

	assert(affectedSubtreeMask != 0);
	uint32_t firstIndexInAffectedSubtree = __builtin_ctz(affectedSubtreeMask);
	uint32_t numberEntriesInAffectedSubtree = _mm_popcnt_u32(affectedSubtreeMask);

	return { subtreePrefixMask, firstIndexInAffectedSubtree, numberEntriesInAffectedSubtree, discriminativeBit };
}

inline void reportInvalidResultIndex(uint resultIndex, uint entryIndex) {
	int indexToReport = static_cast<int>(resultIndex) - 1; //only
	++indexToReport;
	std::cout << "Result Index is :: " << indexToReport << " but expected entry index " << entryIndex;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline
HOTRowexChildPointer HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::addEntry(
	hot::commons::InsertInformation const & insertInformation, HOTRowexChildPointer const & newValue
) const
{
	uint16_t newNumberEntries = this->getNumberEntries() + 1;
	HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType> const & self = *this;

	return mDiscriminativeBitsRepresentation.insert(insertInformation.mKeyInformation, [&](auto const & newDiscriminativeBitsRepresentation)  {
		using NewConstDiscriminativeBitsRepresentationType = typename std::remove_reference<decltype(newDiscriminativeBitsRepresentation)>::type;
		using IntermediateNewDiscriminativeBitsRepresentationType = typename std::remove_const<NewConstDiscriminativeBitsRepresentationType>::type;
		typedef typename ToDiscriminativeBitsRepresentation<IntermediateNewDiscriminativeBitsRepresentationType, PartialKeyType>::Type NewDiscriminativeBitsRepresentationType;
        HOTRowexChildPointer newChild;
		if (newDiscriminativeBitsRepresentation.calculateNumberBitsUsed() <= (sizeof(PartialKeyType) * 8)) {
			newChild = (new (newNumberEntries) HOTRowexNode<NewDiscriminativeBitsRepresentationType, typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType, PartialKeyType>::Type>(
				self, newNumberEntries, newDiscriminativeBitsRepresentation, insertInformation, newValue))->toChildPointer();
            hot::commons::mfence();
            hot::commons::NodeAllocationInformation const & allocationInformation =
            hot::commons::NodeAllocationInformations<HOTRowexNode<NewDiscriminativeBitsRepresentationType, typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType, PartialKeyType>::Type>>::getAllocationInformation(newNumberEntries);
            hot::commons::clflush(reinterpret_cast<char *> (newChild.getNode()), allocationInformation.mTotalSizeInBytes);
        } else {
			newChild = (new (newNumberEntries) HOTRowexNode<typename ToDiscriminativeBitsRepresentation<NewDiscriminativeBitsRepresentationType,
                    typename NextPartialKeyType<PartialKeyType>::Type>::Type, typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType,typename NextPartialKeyType<PartialKeyType>::Type>::Type> (
				self, newNumberEntries, newDiscriminativeBitsRepresentation, insertInformation, newValue))->toChildPointer();
            hot::commons::mfence();
            hot::commons::NodeAllocationInformation const & allocationInformation =
            hot::commons::NodeAllocationInformations<HOTRowexNode<typename ToDiscriminativeBitsRepresentation<NewDiscriminativeBitsRepresentationType, typename NextPartialKeyType<PartialKeyType>::Type>::Type, typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType,typename NextPartialKeyType<PartialKeyType>::Type>::Type>>::getAllocationInformation(newNumberEntries);
            hot::commons::clflush(reinterpret_cast<char *> (newChild.getNode()), allocationInformation.mTotalSizeInBytes);
        }
        return newChild;
#if 0
		return (newDiscriminativeBitsRepresentation.calculateNumberBitsUsed() <= (sizeof(PartialKeyType) * 8))
			? (new (newNumberEntries) HOTRowexNode<NewDiscriminativeBitsRepresentationType, typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType, PartialKeyType>::Type>(
				self, newNumberEntries, newDiscriminativeBitsRepresentation, insertInformation, newValue
			  ))->toChildPointer()
			: (new (newNumberEntries) HOTRowexNode<
					typename ToDiscriminativeBitsRepresentation<NewDiscriminativeBitsRepresentationType, typename NextPartialKeyType<PartialKeyType>::Type>::Type,
					typename ToPartialKeyType<NewDiscriminativeBitsRepresentationType,typename NextPartialKeyType<PartialKeyType>::Type>::Type
				> (
				self, newNumberEntries, newDiscriminativeBitsRepresentation, insertInformation, newValue
			))->toChildPointer();
#endif
	});
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline
HOTRowexChildPointer HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::compressEntries(uint32_t firstIndexInRange, uint16_t numberEntriesInRange) const
{
	PartialKeyType relevantBits = mPartialKeys.getRelevantBitsForRange(firstIndexInRange, numberEntriesInRange);

	if(numberEntriesInRange > 1) {
		HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType> const &self = *this;

		return mDiscriminativeBitsRepresentation.extract(relevantBits, [&](auto const &newDiscriminativeBitsRepresentation) {
			return newDiscriminativeBitsRepresentation.executeWithCorrectMaskAndDiscriminativeBitsRepresentation([&](auto const &finalDiscriminativeBitsRepresentation, auto maximumMask) {
				using FinalDiscriminativeBitsRepresentationType = typename std::remove_const<
					typename std::remove_reference<decltype(finalDiscriminativeBitsRepresentation)>::type
				>::type;

				HOTRowexChildPointer newChild = (new (numberEntriesInRange) HOTRowexNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
					self, numberEntriesInRange, finalDiscriminativeBitsRepresentation, relevantBits, firstIndexInRange, numberEntriesInRange
				))->toChildPointer();
                hot::commons::mfence();
                hot::commons::NodeAllocationInformation const & allocationInformation =
                hot::commons::NodeAllocationInformations<HOTRowexNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>>::getAllocationInformation(numberEntriesInRange);
                hot::commons::clflush(reinterpret_cast <char *> (newChild.getNode()), allocationInformation.mTotalSizeInBytes);
                return newChild;
//				return (new (numberEntriesInRange) HOTRowexNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
//					self, numberEntriesInRange, finalDiscriminativeBitsRepresentation, relevantBits, firstIndexInRange, numberEntriesInRange
//				))->toChildPointer();
			});
		});
	} else {
		return this->getPointers()[firstIndexInRange];
	}
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline
HOTRowexChildPointer HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::compressEntriesAndAddOneEntryIntoNewNode(
	uint32_t firstIndexInRange, uint16_t numberEntriesInRange, hot::commons::InsertInformation const & insertInformation, HOTRowexChildPointer const & newValue
) const
{
	if(numberEntriesInRange != 1) {
		PartialKeyType relevantBits = mPartialKeys.getRelevantBitsForRange(firstIndexInRange, numberEntriesInRange);
		uint16_t nextNumberEntries = numberEntriesInRange + 1;
		HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType> const & self = *this;

		return mDiscriminativeBitsRepresentation.extract(relevantBits, [&](auto const & intermediateDiscriminativeBitsRepresentation)  {
			return intermediateDiscriminativeBitsRepresentation.insert(insertInformation.mKeyInformation, [&](auto const & newDiscriminativeBitsRepresentation) {
				return newDiscriminativeBitsRepresentation.executeWithCorrectMaskAndDiscriminativeBitsRepresentation([&](auto const & finalDiscriminativeBitsRepresentation, auto maximumMask) {
					using FinalDiscriminativeBitsRepresentationType = typename std::remove_const<
						typename std::remove_reference<decltype(finalDiscriminativeBitsRepresentation)>::type
					>::type;

					HOTRowexChildPointer newChild = (new (nextNumberEntries) HOTRowexNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
						self, nextNumberEntries, finalDiscriminativeBitsRepresentation, relevantBits, firstIndexInRange, numberEntriesInRange, insertInformation, newValue
					))->toChildPointer();
                    hot::commons::mfence();
                    hot::commons::NodeAllocationInformation const & allocationInformation =
                    hot::commons::NodeAllocationInformations<HOTRowexNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>>::getAllocationInformation(nextNumberEntries);
                    hot::commons::clflush(reinterpret_cast <char *> (newChild.getNode()), allocationInformation.mTotalSizeInBytes);
                    return newChild;
//					return (new (nextNumberEntries) HOTRowexNode<FinalDiscriminativeBitsRepresentationType, decltype(maximumMask)>(
//						self, nextNumberEntries, finalDiscriminativeBitsRepresentation, relevantBits, firstIndexInRange, numberEntriesInRange, insertInformation, newValue
//					))->toChildPointer();
				});
			});
		});
	} else {
		hot::commons::BiNode<HOTRowexChildPointer> const & binaryNode = hot::commons::BiNode<HOTRowexChildPointer>::createFromExistingAndNewEntry(insertInformation.mKeyInformation, this->getPointers()[firstIndexInRange], newValue);
		return hot::commons::createTwoEntriesNode<HOTRowexChildPointer, HOTRowexNode>(binaryNode)->toChildPointer();
	}
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  inline
uint16_t HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getLeastSignificantDiscriminativeBitForEntry(
	unsigned int entryIndex) const
{
	unsigned int nextEntryIndex = entryIndex + 1;
	uint32_t mask = nextEntryIndex < this->getNumberEntries()
		? (static_cast<uint32_t>(mPartialKeys.mEntries[entryIndex]) | static_cast<uint32_t>(mPartialKeys.mEntries[nextEntryIndex]))
		: mPartialKeys.mEntries[entryIndex];

	return mDiscriminativeBitsRepresentation.getLeastSignificantBitIndex(mask);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline hot::commons::BiNode<HOTRowexChildPointer> HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::split(
	hot::commons::InsertInformation const & insertInformation, HOTRowexChildPointer const & newValue
) const {
	uint32_t largerEntries = getMaskForLargerEntries();
	assert(largerEntries != 0);

	uint32_t numberLargerEntries = _mm_popcnt_u32(largerEntries);
	uint32_t numberSmallerEntries = this->getNumberEntries() - numberLargerEntries;

	assert(insertInformation.getNumberEntriesInAffectedSubtree() > 0);

	uint16_t newHeight = this->mHeight + 1;

	return (insertInformation.getFirstIndexInAffectedSubtree() >= numberSmallerEntries)
		   ? hot::commons::BiNode<HOTRowexChildPointer> { mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex, newHeight, compressEntries(0, numberSmallerEntries), compressEntriesAndAddOneEntryIntoNewNode(numberSmallerEntries, numberLargerEntries, insertInformation, newValue) }
		   : hot::commons::BiNode<HOTRowexChildPointer> { mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex, newHeight, compressEntriesAndAddOneEntryIntoNewNode(0, numberSmallerEntries, insertInformation, newValue), compressEntries(numberSmallerEntries, numberLargerEntries) };
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline HOTRowexChildPointer HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::toChildPointer() const {
	return { mNodeType, this };
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline
typename HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::PartialKeyConversionInformation HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getConversionInformation(
	HOTRowexNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode, hot::commons::InsertInformation const & insertionInformation
) const
{
	return getConversionInformation(sourceNode.mDiscriminativeBitsRepresentation.getAllMaskBits(), insertionInformation);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType>  template<typename SourcePartialKeyType> inline
typename HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::PartialKeyConversionInformation HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getConversionInformationForCompressionMask(
	SourcePartialKeyType compressionMask, hot::commons::InsertInformation const & insertionInformation
) const
{
	uint32_t allIntermediateMaskBits = _pext_u32(compressionMask, compressionMask);
	return getConversionInformation(allIntermediateMaskBits, insertionInformation);
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline typename HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::PartialKeyConversionInformation
HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getConversionInformation(
	uint32_t sourceMaskBits, hot::commons::InsertInformation const & insertionInformation
) const {
	uint32_t allTargetMaskBits = mDiscriminativeBitsRepresentation.getAllMaskBits();
	uint hasNewBit = sourceMaskBits != allTargetMaskBits;
	PartialKeyType additionalMask = mDiscriminativeBitsRepresentation.getMaskFor(insertionInformation.mKeyInformation);
	PartialKeyType conversionMask = allTargetMaskBits & (~(hasNewBit * additionalMask));

	return { additionalMask, conversionMask };
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline uint32_t HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getMaskForLargerEntries() const {
	uint64_t const maskWithMostSignificantBitSet = mDiscriminativeBitsRepresentation.getMaskForHighestBit();
	return mPartialKeys.findMasksByPattern(maskWithMostSignificantBitSet) & this->mUsedEntriesMask;
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline std::array<uint8_t, 32> HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getEntryDepths() const {
	std::array<uint8_t, 32> entryDepths;
	std::fill(entryDepths.begin(), entryDepths.end(), 0u);
	collectEntryDepth(entryDepths, 0, this->getNumberEntries(), 0ul, 0u);
	return entryDepths;
};

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline void HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::collectEntryDepth(std::array<uint8_t, 32> & entryDepths, size_t minEntryIndexInRange, size_t numberEntriesInRange, size_t currentDepth, uint32_t usedMaskBits) const {
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

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline bool HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::isPartitionCorrect() const {
	std::array<int, 32> significantBitValueStack { -1 };
	std::array<uint, 32> entriesMaskStack { 0 };
	std::array<PartialKeyType, 32> prefixStack { 0 };
	std::array<PartialKeyType, 32> mostSignificantBitStack { 0 };
	std::array<PartialKeyType, 32> subtreeBitsUsedStack { 0 };
	significantBitValueStack[0] = 0;
	entriesMaskStack[0] = this->mUsedEntriesMask;
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

	uint16_t maxChildHeight = 0;
	for(size_t i = 0; i < this->getNumberEntries(); ++i) {
		HOTRowexChildPointer const & child = this->getPointers()[i];
		if(child.isNode()) {
			maxChildHeight = std::max(maxChildHeight, child.getHeight());
		}
	}
	if(partitionIsCorrect && ((maxChildHeight + 1) != this->mHeight)) {
		std::cout << "maximum childHeight " << maxChildHeight << " does not match node height " << this->mHeight << std::endl;
		partitionIsCorrect = false;
	}

	while(partitionIsCorrect && stackIndex >= 0) {
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
			uint32_t affectedSubtree = mPartialKeys.findMasksByPattern(subtreePrefixMask) & mPartialKeys.search(subtreeMatcherMask) & this->mUsedEntriesMask;

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
	for(size_t i=0; i < this->getNumberEntries(); ++i) {
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
		mPartialKeys.printMasks(this->mUsedEntriesMask, extractionMaskMapping);
	}

	return partitionIsCorrect;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline size_t HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getNodeSizeInBytes() const {
	return hot::commons::NodeAllocationInformations<HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>>::getAllocationInformation(this->getNumberEntries()).mTotalSizeInBytes;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline std::map<uint16_t, uint16_t> HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::getExtractionMaskToEntriesMasksMapping() const {
	std::map<uint16_t, uint16_t> maskBitMapping;
	for(uint16_t extractionBitIndex : mDiscriminativeBitsRepresentation.getDiscriminativeBits()) {
		uint32_t singleBitMask = mDiscriminativeBitsRepresentation.getMaskFor({ extractionBitIndex, 1 });
		uint maskBitPosition = __tzcnt_u32(singleBitMask);
		maskBitMapping[extractionBitIndex] = maskBitPosition;
	}
	return maskBitMapping;
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> inline void HOTRowexNode<DiscriminativeBitsRepresentation, PartialKeyType>::printPartialKeysWithMappings(
	std::ostream &out) const {
	return mPartialKeys.printMasks(this->mUsedEntriesMask, getExtractionMaskToEntriesMasksMapping(), out);
};

}}

#endif
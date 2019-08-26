#ifndef __HOT__COMMONS__SPARSE_PARTIAL_KEYS__
#define __HOT__COMMONS__SPARSE_PARTIAL_KEYS__

#include <immintrin.h>

#include <bitset>
#include <cassert>
#include <cstdint>

#include <hot/commons/Algorithms.hpp>

namespace hot { namespace commons {

constexpr uint16_t alignToNextHighestValueDivisableBy8(uint16_t size) {
	return static_cast<uint16_t>((size % 8 == 0) ? size : ((size & (~7)) + 8));
}

/**
 * Sparse Partial Keys are used to store the discriminative bits required to distinguish entries in a linearized binary patricia trie.
 *
 * For a single binary patricia trie the set of discriminative bits consists of all discriminative bit positions used in its BiNodes.
 * Partial Keys therefore consists only of those bits of a the stored key which correspon to a discriminative bit.
 * Sparse partial keys are an optimization of partial keys and can be used to construct linearized representations of binary patricia tries.
 * For each entry only those bits in the sparse partial keys are set, which correspond to a BiNodes along the path from the binary patricia trie root.
 * All other bits are set to 0 and are therefore intentially left undefined.
 *
 * To clarify the notion of sparse partial keys we illustrate the conversion of a binary patricia trie consisting of 7 entries into its linearized representation.
 * This is an ASCII art version of Figure 5 of "HOT: A Height Optimized Trie Index"
 *
 *             (bit 3, 3-bit prefix 011)           |Values |   Raw Key   |Bit Positions|Partial key (dense) |Partial key (sparse)|
 *                    /  \                         |=======|=============|=============|====================|====================|
 *                  /     \                        |  v1   |  0110100101 |  {3,6,8,}   |     0 1 0 0 1      |     0 0 0 0 0      |
 *              010/       \1                      |-------|-------------|-------------|--------------------|--------------------|
 *               /          \                      |  v2   |  0110100110 |  {3,6,8}    |     0 1 0 1 0      |     0 0 0 1 0      |
 *              /            \                     |-------|-------------|-------------|--------------------|--------------------|
 *        (bit 6))          (bit 4)                |  v3   |  0110101010 |  {3,6,9}    |     0 1 1 1 0      |     0 0 1 0 0      |
 *         /     \            /  \                 |-------|-------------|-------------|--------------------|--------------------|
 *      01/    101\    010110/    \ 1010           |  v4   |  0110101011 |  {3,6,9}    |     0 1 1 1 1      |     0 0 1 0 1      |
 *       /        \         /      \               |-------|-------------|-------------|--------------------|--------------------|
 *    (bit 8)  (bit 9)    v5    (bit 8)            |  v5   |  0111010110 |   {3,4}     |     1 0 0 1 0      |     1 0 0 0 0      |
 *      / \      / \              / \              |-------|-------------|-------------|--------------------|--------------------|
 *   01/   \10 0/  \1        01  /   \ 11          |  v6   |  0111101001 |  {3,4,8}    |     1 1 1 0 1      |     1 1 0 0 0      |
 *    /     \  /    \           /     \            |-------|-------------|-------------|--------------------|--------------------|
 *   v1     v2 v3   v4         v6      v7          |  v7   |  0111101011 |  {3,4,8}    |     1 1 1 1 1      |     1 1 0 1 0      |
 *                                                 |=====================|=============|====================|====================|
 */
template<typename PartialKeyType>
struct alignas(8) SparsePartialKeys {
	void *operator new(size_t /* baseSize */, uint16_t const numberEntries);

	void operator delete(void *);

	PartialKeyType mEntries[1];

	/**
	 * Search returns a mask corresponding to all partial keys complying to the dense partial search key.
	 * The bit positions in the result mask are indexed from the least to the most singificant bit
	 *
	 * Compliance for a sparse partial key is defined in the following:
	 *
	 * (densePartialKey & sparsePartialKey) === sparsePartialKey
	 *
	 *
	 * @param densePartialSearchKey the dense partial key of the search key to search matching entries for
	 * @return the resulting mask with each bit representing the result of a single compressed mask. bit 0 (least significant) correspond to the mask 0, bit 1 corresponds to mask 1 and so forth.
	 */
	inline uint32_t search(PartialKeyType const densePartialSearchKey) const;

	/**
	 * Determines all dense partial keys which matche a given partial key pattern.
	 *
	 * This method exectues for each sparse partial key the following operation (sparsePartialKey[i]): sparsePartialKey[i] & partialKeyPattern == partialKeyPattern
	 *
	 * @param partialKeyPattern the pattern to use for searchin compressed mask
	 * @return the resulting mask with each bit representing the result of a single compressed mask. bit 0 (least significant) correspond to the mask 0, bit 1 corresponds to mask 1 and so forth.
	 */
	inline uint32_t findMasksByPattern(PartialKeyType const partialKeyPattern) const {
		__m256i searchRegister = broadcastToSIMDRegister(partialKeyPattern);

		return findMasksByPattern(searchRegister, searchRegister);
	}

private:
	inline uint32_t findMasksByPattern(__m256i const usedBitsMask, __m256i const expectedBitsMask) const;

	inline __m256i broadcastToSIMDRegister(PartialKeyType const mask) const;

public:

	/**
	 * This method determines all entries which are contained in a common subtree.
	 * The subtree is defined, by all prefix bits used and the expected prefix bits value
	 *
	 * @param usedPrefixBitsPattern a partial key pattern, which has all bits set, which correspond to parent BiNodes of the  regarding subtree
	 * @param expectedPrefixBits  a partial key pattern, which has all bits set, according to the paths taken from the root node to the parent BiNode of the regarding subtree
	 * @return a resulting bit mask with each bit represent whether the corresponding entry is part of the requested subtree or not.
	 */
	inline uint32_t getAffectedSubtreeMask(PartialKeyType usedPrefixBitsPattern, PartialKeyType const expectedPrefixBits) const {
		//assumed this should be the same as
		//affectedMaskBitsRegister = broadcastToSIMDRegister(affectedBitsMask)
		//expectedMaskBitsRegister = broadcastToSIMDRegister(expectedMaskBits)
		//return findMasksByPattern(affectedMaskBitsRegister, expectedMaskBitsRegister) & usedEntriesMask;

		__m256i prefixBITSSIMDMask = broadcastToSIMDRegister(usedPrefixBitsPattern);
		__m256i prefixMask = broadcastToSIMDRegister(expectedPrefixBits);

		unsigned int affectedSubtreeMask = findMasksByPattern(prefixBITSSIMDMask, prefixMask);

		//uint affectedSubtreeMask = findMasksByPattern(mEntries[entryIndex] & subtreePrefixMask) & usedEntriesMask;
		//at least the zero mask must match
		assert(affectedSubtreeMask != 0);

		return affectedSubtreeMask;
	}

	/**
	 *
	 * in the case of the following tree structure:
	 *               d
	 *            /    \
	 *           b      f
	 *          / \    / \
	 *         a  c   e  g
	 * Index   0  1   2  3
	 *
	 * If the provided index is 2 corresponding the smallest common subtree containing e consists of the nodes { e, f, g }
	 * and therefore the discriminative bit value for this entry is 0 (in the left side of the subtree).
	 *
	 * If the provided index is 1 corresponding to entry c, the smallest common subtree containing c consists of the nodes { a, b, c }
	 * and therefore the discriminative bit value of this entries 1 (in the right side of the subtree).
	 *
	 * in the case of the following tree structure
	 *                    f
	 *                 /    \
	 *                d      g
	 *              /   \
	 *             b     e
	 *            / \
	 *           a   c
	 * Index     0   1   2   3
	 *
	 * If the provided index is 2 correspondig to entry e, the smallest common subtree containing e consists of the nodes { a, b, c, d, e }
	 * As e is on the right side of this subtree, the discriminative bit's value in this case is 1
	 *
	 *
	 * @param indexOfEntry The index of the entry to obtain the discriminative bits value for
	 * @return The discriminative bit value of the discriminative bit discriminating an entry from its direct neighbour (regardless if the direct neighbour is an inner or a leaf node).
	 */
	inline bool determineValueOfDiscriminatingBit(size_t indexOfEntry, size_t mNumberEntries) const {
		bool discriminativeBitValue;

		if(indexOfEntry == 0) {
			discriminativeBitValue = false;
		} else if(indexOfEntry == (mNumberEntries - 1)) {
			discriminativeBitValue = true;
		} else {
			//Be aware that the masks are not order preserving, as the bits may not be in the correct order little vs. big endian and several included bytes
			discriminativeBitValue = (mEntries[indexOfEntry - 1]&mEntries[indexOfEntry]) >= (mEntries[indexOfEntry]&mEntries[indexOfEntry + 1]);
		}
		return discriminativeBitValue;
	}

	/**
	 * Get Relevant bits detects the key bits used for discriminating new entries in the given range.
	 * These bits are determined by comparing successing masks in this range.
	 * Whenever a mask has a bit set which is not set in its predecessor these bit is added to the set of relevant bits.
	 * The reason is that if masks are stored in an orderpreserving way for a mask to be large than its predecessor it has to set
	 * exactly one more bit.
	 * By using this algorithm the bits of the first mask occuring in the range of masks are always ignored.
	 *
	 * @param firstIndexInRange the first index of the range of entries to determine the relevant bits for
	 * @param numberEntriesInRange the number entries in the range of entries to use for determining the relevant bits
	 * @return a mask with only the relevant bits set.
	 */
	inline PartialKeyType getRelevantBitsForRange(uint32_t const firstIndexInRange, uint32_t const numberEntriesInRange) const {
		PartialKeyType relevantBits = 0;

		uint32_t firstIndexOutOfRange = firstIndexInRange + numberEntriesInRange;
		for(uint32_t i = firstIndexInRange + 1; i < firstIndexOutOfRange; ++i) {
			relevantBits |= (mEntries[i] & ~mEntries[i - 1]);
		}
		return relevantBits;
	}

	/**
	 * Gets a partial key which has all discriminative bits set, which are required to distinguish all but the entry, which is intended to be removed.
	 *
	 * @param numberEntries the total number of entries
	 * @param indexOfEntryToIgnore  the index of the entry, which shall be removed
	 * @return partial key which has all neccessary discriminative bits set
	 */
	inline PartialKeyType getRelevantBitsForAllExceptOneEntry(uint32_t const numberEntries, uint32_t indexOfEntryToIgnore) const {
		size_t numberEntriesInFirstRange = indexOfEntryToIgnore + static_cast<size_t>(!determineValueOfDiscriminatingBit(indexOfEntryToIgnore, numberEntries));

		PartialKeyType relevantBitsInFirstPart = getRelevantBitsForRange(0, numberEntriesInFirstRange);
		PartialKeyType relevantBitsInSecondPart = getRelevantBitsForRange(numberEntriesInFirstRange, numberEntries - numberEntriesInFirstRange);
		return relevantBitsInFirstPart | relevantBitsInSecondPart;
	}

	static inline uint16_t estimateSize(uint16_t numberEntries) {
		return alignToNextHighestValueDivisableBy8(numberEntries * sizeof(PartialKeyType));
	}

	/**
	 * @param maskBitMapping maps from the absoluteBitPosition to its maskPosition
	 */
	inline void printMasks(uint32_t maskOfEntriesToPrint, std::map<uint16_t, uint16_t> const & maskBitMapping, std::ostream & outputStream = std::cout) const {
		while(maskOfEntriesToPrint > 0) {
			uint entryIndex = __tzcnt_u32(maskOfEntriesToPrint);
			std::bitset<sizeof(PartialKeyType) * 8> maskBits(mEntries[entryIndex]);
			outputStream << "mask[" << entryIndex << "] = \toriginal: " << maskBits << "\tmapped: ";
			printMaskWithMapping(mEntries[entryIndex], maskBitMapping, outputStream);
			outputStream << std::endl;
			maskOfEntriesToPrint &= (~(1u << entryIndex));
		}
	}

	static inline void printMaskWithMapping(PartialKeyType mask, std::map<uint16_t, uint16_t> const & maskBitMapping, std::ostream & outputStream) {
		std::bitset<convertBytesToBits(sizeof(PartialKeyType))> maskBits(mask);
		for(auto mapEntry : maskBitMapping) {
			uint64_t maskBitIndex = mapEntry.second;
			outputStream << maskBits[maskBitIndex];
		}
	}


	inline void printMasks(uint32_t maskOfEntriesToPrint, std::ostream & outputStream = std::cout) const {
		while(maskOfEntriesToPrint > 0) {
			uint entryIndex = __tzcnt_u32(maskOfEntriesToPrint);

			std::bitset<sizeof(PartialKeyType) * 8> maskBits(mEntries[entryIndex]);
			outputStream << "mask[" << entryIndex << "] = " << maskBits << std::endl;

			maskOfEntriesToPrint &= (~(1u << entryIndex));
		}
	}

private:
	// Prevent heap allocation
	void * operator new   (size_t) = delete;
	void * operator new[] (size_t) = delete;
	void operator delete[] (void*) = delete;
};

template<typename PartialKeyType> void* SparsePartialKeys<PartialKeyType>::operator new (size_t /* baseSize */, uint16_t const numberEntries) {
	assert(numberEntries >= 2);

	constexpr size_t paddingElements = (32 - 8)/sizeof(PartialKeyType);
	size_t estimatedNumberElements = estimateSize(numberEntries)/sizeof(PartialKeyType);


	return new PartialKeyType[estimatedNumberElements + paddingElements];
};
template<typename PartialKeyType> void SparsePartialKeys<PartialKeyType>::operator delete (void * rawMemory) {
	PartialKeyType* masks = reinterpret_cast<PartialKeyType*>(rawMemory);
	delete [] masks;
}

template<>
inline __attribute__((always_inline)) uint32_t SparsePartialKeys<uint8_t>::search(uint8_t const uncompressedSearchMask) const {
	__m256i searchRegister = _mm256_set1_epi8(uncompressedSearchMask); //2 instr

	__m256i haystack = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
#ifdef USE_AVX512
	uint32_t const resultMask = _mm256_cmpeq_epi8_mask (_mm256_and_si256(haystack, searchRegister), haystack);
#else
	__m256i searchResult = _mm256_cmpeq_epi8(_mm256_and_si256(haystack, searchRegister), haystack);
	uint32_t const resultMask = static_cast<uint32_t>(_mm256_movemask_epi8(searchResult));
#endif
	return resultMask;
}

template<>
inline __attribute__((always_inline)) uint32_t SparsePartialKeys<uint16_t>::search(uint16_t const uncompressedSearchMask) const {
#ifdef USE_AVX512
	__m512i searchRegister = _mm512_set1_epi16(uncompressedSearchMask); //2 instr
	__m512i haystack = _mm512_loadu_si512(reinterpret_cast<__m512i const *>(mEntries)); //3 instr
	return static_cast<uint32_t>(_mm512_cmpeq_epi16_mask(_mm512_and_si512(haystack, searchRegister), haystack));
#else
	__m256i searchRegister = _mm256_set1_epi16(uncompressedSearchMask); //2 instr

	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16)); //4 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack1, searchRegister), haystack1);
	__m256i searchResult2 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack2, searchRegister), haystack2);

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		searchResult1, searchResult2
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
#endif
}

template<>
inline __attribute__((always_inline)) uint32_t SparsePartialKeys<uint32_t>::search(uint32_t const uncompressedSearchMask) const {
	__m256i searchRegister = _mm256_set1_epi32(uncompressedSearchMask); //2 instr

	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries));
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 8));
	__m256i haystack3 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16));
	__m256i haystack4 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 24)); //27 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack1, searchRegister), haystack1);
	__m256i searchResult2 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack2, searchRegister), haystack2);
	__m256i searchResult3 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack3, searchRegister), haystack3);
	__m256i searchResult4 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack4, searchRegister), haystack4); //35 + 8 = 43

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult1, searchResult2), perm_mask),
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult3, searchResult4), perm_mask)
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
}

template<>
inline uint32_t SparsePartialKeys<uint8_t>::findMasksByPattern(__m256i consideredBitsRegister, __m256i expectedBitsRegister) const {
	__m256i haystack = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
	__m256i searchResult = _mm256_cmpeq_epi8(_mm256_and_si256(haystack, consideredBitsRegister), expectedBitsRegister);
	return static_cast<uint32_t>(_mm256_movemask_epi8(searchResult));
}

template<>
inline uint32_t SparsePartialKeys<uint16_t>::findMasksByPattern(__m256i consideredBitsRegister, __m256i expectedBitsRegister) const {
	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16)); //4 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack1, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult2 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack2, consideredBitsRegister), expectedBitsRegister);

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		searchResult1, searchResult2
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
}

template<>
inline uint32_t SparsePartialKeys<uint32_t>::findMasksByPattern(__m256i consideredBitsRegister, __m256i expectedBitsRegister) const {
	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries));
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 8));
	__m256i haystack3 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16));
	__m256i haystack4 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 24)); //27 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack1, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult2 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack2, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult3 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack3, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult4 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack4, consideredBitsRegister), expectedBitsRegister); //35 + 8 = 43

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult1, searchResult2), perm_mask),
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult3, searchResult4), perm_mask)
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
}

template<>
inline __m256i SparsePartialKeys<uint8_t>::broadcastToSIMDRegister(uint8_t const mask) const {
	return _mm256_set1_epi8(mask);
}

template<>
inline __m256i SparsePartialKeys<uint16_t>::broadcastToSIMDRegister(uint16_t const mask) const {
	return _mm256_set1_epi16(mask);
}

template<>
inline __m256i SparsePartialKeys<uint32_t>::broadcastToSIMDRegister(uint32_t const mask) const {
	return _mm256_set1_epi32(mask);
}

}}

#endif

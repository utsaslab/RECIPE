#ifndef __HOT__COMMONS__SINGLE_MASK_PARTIAL_KEY_MAPPING_HPP___
#define __HOT__COMMONS__SINGLE_MASK_PARTIAL_KEY_MAPPING_HPP___

#include <immintrin.h>

#include <cstring>
#include <set>

#include "hot/commons/Algorithms.hpp"
#include "hot/commons/DiscriminativeBit.hpp"

#include "hot/commons/PartialKeyMappingBase.hpp"
#include "hot/commons/SingleMaskPartialKeyMappingInterface.hpp"
#include "hot/commons/MultiMaskPartialKeyMapping.hpp"

namespace hot { namespace commons {

constexpr uint64_t SUCCESSIVE_EXTRACTION_MASK_WITH_HIGHEST_BIT_SET = 1ul << 63;

inline SingleMaskPartialKeyMapping::SingleMaskPartialKeyMapping(SingleMaskPartialKeyMapping const & src)
	: PartialKeyMappingBase()
{
	_mm_storeu_si128(reinterpret_cast<__m128i*>(this), _mm_loadu_si128(reinterpret_cast<__m128i const *>(&src)));
}

inline SingleMaskPartialKeyMapping::SingleMaskPartialKeyMapping(DiscriminativeBit const & discriminativeBit)
	: PartialKeyMappingBase(discriminativeBit.mAbsoluteBitIndex, discriminativeBit.mAbsoluteBitIndex),
	  mOffsetInBytes(getSuccesiveByteOffsetForMostRightByte(discriminativeBit.mByteIndex)),
	  mSuccessiveExtractionMask(getSuccessiveMaskForBit(discriminativeBit.mByteIndex, discriminativeBit.mByteRelativeBitIndex)) {
	assert(mOffsetInBytes < 255);
}

inline SingleMaskPartialKeyMapping::SingleMaskPartialKeyMapping(
	uint8_t const * extractionBytePositions,
	uint8_t const * extractionByteData,
	uint32_t const extractionBytesUsedMask,
	uint16_t const mostSignificantBitIndex,
	uint16_t const leastSignificantBitIndex
) : PartialKeyMappingBase(mostSignificantBitIndex, leastSignificantBitIndex),
	mOffsetInBytes(getSuccesiveByteOffsetForLeastSignificantBitIndex(leastSignificantBitIndex)),
	mSuccessiveExtractionMask(getSuccessiveExtractionMaskFromRandomBytes(extractionBytePositions, extractionByteData, extractionBytesUsedMask, mOffsetInBytes))
{
	assert(mOffsetInBytes < 255);
}


inline SingleMaskPartialKeyMapping::SingleMaskPartialKeyMapping(
	SingleMaskPartialKeyMapping const & existing,
	DiscriminativeBit const & discriminatingBit
) : PartialKeyMappingBase(existing, discriminatingBit),
	mOffsetInBytes(getSuccesiveByteOffsetForLeastSignificantBitIndex(mLeastSignificantDiscriminativeBitIndex)),
	mSuccessiveExtractionMask(
		getSuccessiveMaskForBit(discriminatingBit.mByteIndex, discriminatingBit.mByteRelativeBitIndex)
		| (existing.mSuccessiveExtractionMask >> (convertBytesToBits(mOffsetInBytes - existing.mOffsetInBytes)))
	)
{
	assert(mOffsetInBytes < 255);
}



inline SingleMaskPartialKeyMapping::SingleMaskPartialKeyMapping(
	SingleMaskPartialKeyMapping const & existing,
	uint32_t const & maskBitsNeeded
) : SingleMaskPartialKeyMapping(existing, existing.getSuccessiveMaskForMask(maskBitsNeeded))
{
	assert(_mm_popcnt_u32(maskBitsNeeded) >= 1);
	assert(mOffsetInBytes < 255);
}

inline uint16_t SingleMaskPartialKeyMapping::calculateNumberBitsUsed() const {
	return _mm_popcnt_u64(mSuccessiveExtractionMask);
}

template<typename PartialKeyType> inline PartialKeyType
SingleMaskPartialKeyMapping::getPrefixBitsMask(DiscriminativeBit const &significantKeyInformation) const {
	int relativeMissmatchingByteIndex = significantKeyInformation.mByteIndex - mOffsetInBytes;

	//Extracts a bit masks corresponding to 111111111|00000000 with the first zero bit marking the missmatching bit index.

	//PAPER describe that little endian byte order must be respected.
	//due to little endian encoding inside the byte the shift direction is reversed
	//it has the form 1110000 111111111....
	uint64_t singleBytePrefixMask = ((UINT64_MAX >> significantKeyInformation.mByteRelativeBitIndex) ^ (UINT64_MAX << 56)) * (relativeMissmatchingByteIndex >= 0);
	//mask where highest byte (most right byte in little endian) is set to the deleted mask
	//results ins a mask like this 010111|1111111111111111...... where the pipe masks the end of the first byte
	//in the next step this mask is moved to the right to mask the prefix byte (in little endian move high byte to the right actually means moving high byte to the left hence turning the most right bytes 0 (after the prefix))
	uint64_t subtreeSuccesiveBytesMask = relativeMissmatchingByteIndex > 7 ? UINT64_MAX : (singleBytePrefixMask >> ((7 - relativeMissmatchingByteIndex) * 8));


	return extractMaskFromSuccessiveBytes(subtreeSuccesiveBytesMask);
}

template<typename Operation> inline auto SingleMaskPartialKeyMapping::insert(DiscriminativeBit const & discriminativeBit,
        Operation const & operation) const {
	bool isSingleMaskPartialKeyMapping =
		((static_cast<int>(discriminativeBit.mByteIndex - getByteIndex(mMostSignificantDiscriminativeBitIndex))) < 8)
		& ((static_cast<int>(getByteIndex(mLeastSignificantDiscriminativeBitIndex) - discriminativeBit.mByteIndex)) < 8);

	return isSingleMaskPartialKeyMapping
		? operation(SingleMaskPartialKeyMapping { *this, discriminativeBit })
		: (hasUnusedBytes()
			? operation(MultiMaskPartialKeyMapping<1u> { *this, discriminativeBit })
		   	: operation(MultiMaskPartialKeyMapping<2u> { *this, discriminativeBit })
		  );
}

template<typename Operation> inline auto SingleMaskPartialKeyMapping::extract(uint32_t bitsUsed, Operation const & operation) const {
	assert(_mm_popcnt_u32(bitsUsed) >= 1);
	return operation(SingleMaskPartialKeyMapping { *this, bitsUsed } );
}

template<typename Operation> inline auto SingleMaskPartialKeyMapping::executeWithCorrectMaskAndDiscriminativeBitsRepresentation(Operation const & operation) const {
	assert(getMaximumMaskByteIndex(calculateNumberBitsUsed()) <= 3);
	switch(getMaximumMaskByteIndex(calculateNumberBitsUsed())) {
		case 0:
			return operation(*this, static_cast<uint8_t>(UINT8_MAX));
		case 1:
			return operation(*this, static_cast<uint16_t>(UINT16_MAX));
		default: //case 2 + 3
			return operation(*this, static_cast<uint32_t>(UINT32_MAX));
	}
}

inline bool SingleMaskPartialKeyMapping::hasUnusedBytes() const {
	return getUsedBytesMask() != UINT8_MAX;
}

inline uint32_t SingleMaskPartialKeyMapping::getUsedBytesMask() const {
	__m64 extractionMaskRegister = getRegister();
	return _mm_movemask_pi8(_mm_cmpeq_pi8(extractionMaskRegister, _mm_setzero_si64())) ^ UINT8_MAX;
}

inline uint32_t SingleMaskPartialKeyMapping::getByteOffset() const {
	return mOffsetInBytes;
}


inline uint8_t SingleMaskPartialKeyMapping::getExtractionByte(unsigned int byteIndex) const {
	return reinterpret_cast<uint8_t const *>(&mSuccessiveExtractionMask)[byteIndex];
}

inline uint8_t SingleMaskPartialKeyMapping::getExtractionBytePosition(unsigned int byteIndex) const {
	return byteIndex + mOffsetInBytes;
}

inline uint32_t SingleMaskPartialKeyMapping::getMaskForHighestBit() const {
	return extractMaskFromSuccessiveBytes(getSuccessiveMaskForAbsoluteBitPosition(mMostSignificantDiscriminativeBitIndex));
}

inline uint32_t SingleMaskPartialKeyMapping::getMaskFor(DiscriminativeBit const & discriminativeBit) const {
	return extractMaskFromSuccessiveBytes(getSuccessiveMaskForBit(discriminativeBit.mByteIndex, discriminativeBit.mByteRelativeBitIndex));
}

inline uint32_t SingleMaskPartialKeyMapping::getAllMaskBits() const {
	return _pext_u64(mSuccessiveExtractionMask, mSuccessiveExtractionMask);
}

inline __attribute__((always_inline)) uint32_t SingleMaskPartialKeyMapping::extractMask(uint8_t const * keyBytes) const {
	return  extractMaskFromSuccessiveBytes(*reinterpret_cast<uint64_t const*>(keyBytes + mOffsetInBytes));
}

inline std::array<uint8_t, 256> SingleMaskPartialKeyMapping::createIntermediateKeyWithOnlySignificantBitsSet() const {
	std::array<uint8_t, 256> intermediateKey;
	std::memset(intermediateKey.data(), 0, 256);
	std::memmove(intermediateKey.data() + mOffsetInBytes, &mSuccessiveExtractionMask, sizeof(mSuccessiveExtractionMask));
	return intermediateKey;
};

inline SingleMaskPartialKeyMapping::SingleMaskPartialKeyMapping(
	SingleMaskPartialKeyMapping const & existing, uint64_t const newExtractionMaskWithSameOffset
) : PartialKeyMappingBase(
		convertBytesToBits(existing.mOffsetInBytes) + calculateRelativeMostSignificantBitIndex(newExtractionMaskWithSameOffset),
		convertBytesToBits(existing.mOffsetInBytes) + calculateRelativeLeastSignificantBitIndex(newExtractionMaskWithSameOffset)
	),
	mOffsetInBytes(static_cast<uint32_t>(getSuccesiveByteOffsetForLeastSignificantBitIndex(mLeastSignificantDiscriminativeBitIndex))),
	mSuccessiveExtractionMask(newExtractionMaskWithSameOffset << (convertBytesToBits(existing.mOffsetInBytes - mOffsetInBytes)))
{
}

inline __attribute__((always_inline)) uint32_t SingleMaskPartialKeyMapping::extractMaskFromSuccessiveBytes(uint64_t const inputMask) const {
	return static_cast<uint32_t>( _pext_u64(inputMask, mSuccessiveExtractionMask));
}

inline __m64 SingleMaskPartialKeyMapping::getRegister() const {
	return _mm_cvtsi64_m64(mSuccessiveExtractionMask);
}

inline uint64_t SingleMaskPartialKeyMapping::getSuccessiveMaskForBit(uint const bytePosition, uint const byteRelativeBitPosition) const {
	return 1ul << convertToIndexOfOtherEndiness(bytePosition - mOffsetInBytes, byteRelativeBitPosition);
}

inline uint SingleMaskPartialKeyMapping::convertToIndexOfOtherEndiness(uint const maskRelativeBytePosition, uint const byteRelativeBitPosition) {
	return (maskRelativeBytePosition * 8) + 7 - byteRelativeBitPosition;
}

inline uint64_t SingleMaskPartialKeyMapping::getSuccessiveMaskForAbsoluteBitPosition(uint absoluteBitPosition) const {
	return getSuccessiveMaskForBit(getByteIndex(absoluteBitPosition), bitPositionInByte(absoluteBitPosition));
}

inline uint64_t SingleMaskPartialKeyMapping::getSuccessiveMaskForMask(uint32_t const mask) const {
	return _pdep_u64(mask, mSuccessiveExtractionMask);
}

inline uint SingleMaskPartialKeyMapping::getSuccesiveByteOffsetForLeastSignificantBitIndex(uint leastSignificantBitIndex) {
	return getSuccesiveByteOffsetForMostRightByte(getByteIndex(leastSignificantBitIndex));
}

inline uint16_t SingleMaskPartialKeyMapping::calculateRelativeMostSignificantBitIndex(uint64_t rawExtractionMask) {
	assert(rawExtractionMask != 0);
	uint64_t reverseMask = __builtin_bswap64(rawExtractionMask);
	return __lzcnt64(reverseMask);
}

inline uint16_t SingleMaskPartialKeyMapping::calculateRelativeLeastSignificantBitIndex(uint64_t rawExtractionMask) {
	assert(rawExtractionMask != 0);
	return 63 - __tzcnt_u64(__builtin_bswap64(rawExtractionMask));
}

inline uint64_t SingleMaskPartialKeyMapping::getSuccessiveExtractionMaskFromRandomBytes(
	uint8_t const * extractionBytePositions,
	uint8_t const * extractionByteData,
	uint32_t extractionBytesUsedMask,
	uint32_t const offsetInBytes
) {
	uint64_t successiveExtractionMask = 0ul;
	uint8_t* successiveExtractionBytes = reinterpret_cast<uint8_t*>(&successiveExtractionMask);
	while(extractionBytesUsedMask > 0) {
		uint extractionByteIndex = __tzcnt_u32(extractionBytesUsedMask);
		uint targetExtractionBytePosition = extractionBytePositions[extractionByteIndex] - offsetInBytes;
		successiveExtractionBytes[targetExtractionBytePosition] = extractionByteData[extractionByteIndex];
		extractionBytesUsedMask = _blsr_u32(extractionBytesUsedMask);
	}
	return successiveExtractionMask;
}

template<typename PartialKeyType> inline PartialKeyType SingleMaskPartialKeyMapping::getMostSignifikantMaskBit(PartialKeyType mask) const {
	uint64_t correspondingSuccessiveExtractionMask = getSuccessiveMaskForMask(mask);
	unsigned int byteShiftOffset = __tzcnt_u64(correspondingSuccessiveExtractionMask) & (~0b111);
	uint64_t mostSignificantExtractionByteMask = correspondingSuccessiveExtractionMask & (static_cast<uint64_t>(UINT8_MAX) << byteShiftOffset); //pad -> remove relative bit index
	uint64_t extractionMaskWithOnlyMostSignificanMaskBitSet =  SUCCESSIVE_EXTRACTION_MASK_WITH_HIGHEST_BIT_SET >> _lzcnt_u64(mostSignificantExtractionByteMask);
	return extractMaskFromSuccessiveBytes(extractionMaskWithOnlyMostSignificanMaskBitSet);
}

inline uint16_t SingleMaskPartialKeyMapping::getLeastSignificantBitIndex(uint32_t partialKey) const {
	return convertBytesToBits(mOffsetInBytes) + calculateRelativeLeastSignificantBitIndex(getSuccessiveMaskForMask(partialKey));
}

inline std::set<uint16_t> SingleMaskPartialKeyMapping::getDiscriminativeBits() const {
	uint64_t swapedExtractionMask = __builtin_bswap64(mSuccessiveExtractionMask);
	std::set<uint16_t> extractionBits;

	while(swapedExtractionMask != 0) {
		uint isZero = swapedExtractionMask == 0;
		uint notIsZero = 1 - isZero;

		uint16_t bitIndex = (isZero * 63) + (notIsZero * __lzcnt64(swapedExtractionMask));
		uint64_t extractionBit = (1ul << 63) >> bitIndex;
		swapedExtractionMask &= (~extractionBit);

		extractionBits.insert(convertBytesToBits(mOffsetInBytes) + bitIndex);
	}

	return extractionBits;
}

} }

#endif

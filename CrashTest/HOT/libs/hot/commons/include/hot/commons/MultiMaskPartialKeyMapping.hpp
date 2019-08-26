#ifndef __HOT__COMMONS__MULTI_MASK_PARTIAL_KEY_MAPPING___
#define __HOT__COMMONS__MULTI_MASK_PARTIAL_KEY_MAPPING___

#include <immintrin.h>

#include <array>
#include <set>

#include "hot/commons/Algorithms.hpp"
#include "hot/commons/DiscriminativeBit.hpp"
#include "hot/commons/MultiMaskPartialKeyMappingInterface.hpp"
#include "hot/commons/PartialKeyMappingBase.hpp"
#include "hot/commons/SingleMaskPartialKeyMapping.hpp"

namespace hot { namespace commons {

constexpr uint8_t BYTE_WITH_HIGHEST_BIT_SET = (1u << 7);

template<unsigned int numberExtractionMasks> inline typename MultiMaskPartialKeyMapping<numberExtractionMasks>::SIMDRegisterType
MultiMaskPartialKeyMapping<numberExtractionMasks>::getPositionsRegister() const {
	return SIMDHelperType::toRegister(mExtractionPositions);
}

template<unsigned int numberExtractionMasks> inline typename MultiMaskPartialKeyMapping<numberExtractionMasks>::SIMDRegisterType
MultiMaskPartialKeyMapping<numberExtractionMasks>::getExtractionDataRegister() const {
	return SIMDHelperType::toRegister(mExtractionData);
}

template<unsigned int numberExtractionMasks> inline void
MultiMaskPartialKeyMapping<numberExtractionMasks>::setPositions(typename MultiMaskPartialKeyMapping<numberExtractionMasks>::SIMDRegisterType positions) {
	return SIMDHelperType::store(positions, mExtractionPositions);
}

template<unsigned int numberExtractionMasks> inline void
MultiMaskPartialKeyMapping<numberExtractionMasks>::setExtractionData(typename MultiMaskPartialKeyMapping<numberExtractionMasks>::SIMDRegisterType extractionData) {
	return SIMDHelperType::store(extractionData, mExtractionData);
}

template<> inline MultiMaskPartialKeyMapping<1u>::MultiMaskPartialKeyMapping(MultiMaskPartialKeyMapping<1u> const & src)
	: PartialKeyMappingBase()
{
	*reinterpret_cast<uint64_t*>(this) = *reinterpret_cast<uint64_t const *>(&src);
	SIMDHelper<128>::store(SIMDHelper<128>::toRegister(&(src.mExtractionPositions)), &mExtractionPositions);
}

template<> inline MultiMaskPartialKeyMapping<2u>::MultiMaskPartialKeyMapping(MultiMaskPartialKeyMapping<2u> const & src)
	: PartialKeyMappingBase()
{
	*reinterpret_cast<uint64_t*>(this) = *reinterpret_cast<uint64_t const *>(&src);
	SIMDHelper<256>::store(SIMDHelper<256>::toRegister(&(src.mExtractionPositions)), &mExtractionPositions);
}

template<> inline MultiMaskPartialKeyMapping<4u>::MultiMaskPartialKeyMapping(MultiMaskPartialKeyMapping<4u> const & src)
	: PartialKeyMappingBase()
{
	*reinterpret_cast<uint64_t*>(this) = *reinterpret_cast<uint64_t const *>(&src);
	setPositions(src.getPositionsRegister());
	setExtractionData(src.getExtractionDataRegister());
}

template<> inline MultiMaskPartialKeyMapping<4u>::MultiMaskPartialKeyMapping(MultiMaskPartialKeyMapping<2u> const & src)
	: PartialKeyMappingBase()
{
	*reinterpret_cast<uint64_t*>(this) = *reinterpret_cast<uint64_t const *>(&src);
	setPositions(SIMDHelper<256u>::convertWithZeroExtend(src.getPositionsRegister()));
	setExtractionData(SIMDHelper<256u>::convertWithZeroExtend(src.getExtractionDataRegister()));
}

template<> inline MultiMaskPartialKeyMapping<2u>::MultiMaskPartialKeyMapping(MultiMaskPartialKeyMapping<1u> const & src)
	: PartialKeyMappingBase()
{
	*reinterpret_cast<uint64_t*>(this) = *reinterpret_cast<uint64_t const *>(&src);
	setPositions(SIMDHelper<128u>::convertWithZeroExtend(src.getPositionsRegister()));
	setExtractionData(SIMDHelper<128u>::convertWithZeroExtend(src.getExtractionDataRegister()));
}

template<unsigned int numberExtractionMasks> inline MultiMaskPartialKeyMapping<numberExtractionMasks>::MultiMaskPartialKeyMapping(
	SingleMaskPartialKeyMapping const & existing, DiscriminativeBit const & significantKeyInformation
) : PartialKeyMappingBase(existing, significantKeyInformation)
{
	initializeDataAndPositionsWithZero();

	uint32_t usedBytes = existing.getUsedBytesMask();
	uint32_t existingNumberRandomBytes = _mm_popcnt_u32(usedBytes);
	unsigned int isBefore = significantKeyInformation.mAbsoluteBitIndex < existing.mMostSignificantDiscriminativeBitIndex;

	unsigned int newValueIndex = (1 - isBefore) * existingNumberRandomBytes;
	setExtractionByte(newValueIndex, significantKeyInformation.getExtractionByte());
	setExtractionBytePosition(newValueIndex, significantKeyInformation.mByteIndex);

	unsigned int targetIndex = isBefore;
	while(usedBytes > 0) {
		unsigned int sourceByteIndex = __tzcnt_u32(usedBytes);
		usedBytes = _blsr_u32(usedBytes);
		setExtractionBytePosition(targetIndex, existing.getExtractionBytePosition(sourceByteIndex));
		setExtractionByte(targetIndex, existing.getExtractionByte(sourceByteIndex));
		++targetIndex;
	}
	mNumberExtractionBytes = existingNumberRandomBytes + 1;
	mNumberKeyBits = existing.calculateNumberBitsUsed() + 1;
}

//detect insert position -> add byte|position -> finish
template<unsigned int numberExtractionMasks> inline MultiMaskPartialKeyMapping<numberExtractionMasks>::MultiMaskPartialKeyMapping(
	MultiMaskPartialKeyMapping<numberExtractionMasks> const & existing, DiscriminativeBit const & significantKeyInformation, unsigned int extractionByteIndex
)  : MultiMaskPartialKeyMapping(existing)
{
	//this is a performance optimization to not call the BaseDiscriminativeBitsRepresentation constructor and use copying
	mMostSignificantDiscriminativeBitIndex = std::min(mMostSignificantDiscriminativeBitIndex, significantKeyInformation.mAbsoluteBitIndex);
	mLeastSignificantDiscriminativeBitIndex = std::max(mLeastSignificantDiscriminativeBitIndex, significantKeyInformation.mAbsoluteBitIndex);

	uint8_t existingExtractionByte = getExtractionByte(extractionByteIndex);
	uint8_t newExtractionByte = getExtractionByte(extractionByteIndex) | significantKeyInformation.getExtractionByte();
	setExtractionByte(extractionByteIndex, newExtractionByte);
	bool isNewBit = (existingExtractionByte ^ newExtractionByte) > 0;
	mNumberKeyBits += isNewBit;
}

template<unsigned int numberExtractionMasks> inline MultiMaskPartialKeyMapping<numberExtractionMasks>::MultiMaskPartialKeyMapping(
	MultiMaskPartialKeyMapping<numberExtractionMasks> const & existing, DiscriminativeBit const & significantKeyInformation, unsigned int const extractionByteIndex,
	typename MultiMaskPartialKeyMapping<numberExtractionMasks>::SIMDRegisterType const & maskForLessSignificantBytes
)  : PartialKeyMappingBase(existing, significantKeyInformation), mNumberExtractionBytes(existing.mNumberExtractionBytes + 1), mNumberKeyBits(existing.mNumberKeyBits + 1)
{
	SIMDRegisterType extractionPositionRegister = existing.getPositionsRegister();
	SIMDRegisterType movedPositions = SIMDHelperType::shiftLeftOneByte(SIMDHelperType::binaryAnd(maskForLessSignificantBytes, extractionPositionRegister));
	setPositions(SIMDHelperType::binaryOr(
		SIMDHelperType::binaryAndNot(maskForLessSignificantBytes, extractionPositionRegister),
		movedPositions
	));

	SIMDRegisterType extractionDataRegister = existing.getExtractionDataRegister();
	setExtractionData(SIMDHelperType::binaryOr(
		SIMDHelperType::binaryAndNot(maskForLessSignificantBytes, extractionDataRegister),
		SIMDHelperType::shiftLeftOneByte(SIMDHelperType::binaryAnd(maskForLessSignificantBytes, extractionDataRegister))
	));

	setExtractionByte(extractionByteIndex, significantKeyInformation.getExtractionByte());
	setExtractionBytePosition(extractionByteIndex, significantKeyInformation.mByteIndex);
}

template<> template<> inline MultiMaskPartialKeyMapping<2u>::MultiMaskPartialKeyMapping(
	MultiMaskPartialKeyMapping<1u> const & existing, DiscriminativeBit const & significantKeyInformation, unsigned int const extractionByteIndex,
	typename MultiMaskPartialKeyMapping<1u>::SIMDRegisterType const & maskForLessSignificantBytes
) :  MultiMaskPartialKeyMapping<2u>(MultiMaskPartialKeyMapping<2u>{ existing }, significantKeyInformation, extractionByteIndex, MultiMaskPartialKeyMapping<2>::SIMDHelperType::convertWithZeroExtend(maskForLessSignificantBytes))
{
}

template<> template<> inline MultiMaskPartialKeyMapping<4u>::MultiMaskPartialKeyMapping(
	MultiMaskPartialKeyMapping<2u> const & existing, DiscriminativeBit const & significantKeyInformation, unsigned int const extractionByteIndex,
	typename MultiMaskPartialKeyMapping<2u>::SIMDRegisterType const & maskForLessSignificantBytes
) :  MultiMaskPartialKeyMapping<4u>(MultiMaskPartialKeyMapping<4u>{ existing }, significantKeyInformation, extractionByteIndex, MultiMaskPartialKeyMapping<4>::SIMDHelperType::convertWithZeroExtend(maskForLessSignificantBytes))
{
}

template<unsigned int numberExtractionMasks> template<unsigned int sourceNumberExtractionMasks>
inline MultiMaskPartialKeyMapping<numberExtractionMasks>::MultiMaskPartialKeyMapping(
	MultiMaskPartialKeyMapping<sourceNumberExtractionMasks> const &existing, uint32_t bytesUsedMask, typename MultiMaskPartialKeyMapping<sourceNumberExtractionMasks>::ExtractionDataArray const & extractionDataUsed,
	uint16_t const mostSignificantBitIndex, uint16_t const leastSignificantBitIndex,
	uint16_t const numberBytesUsed, uint16_t const bitsUsed
) : MultiMaskPartialKeyMapping<numberExtractionMasks>(mostSignificantBitIndex, leastSignificantBitIndex, numberBytesUsed, bitsUsed) {
	assert(_mm_popcnt_u32(bitsUsed) >= 1);

	initializeDataAndPositionsWithZero();
	uint8_t const * extractionBytesUsed = reinterpret_cast<uint8_t const *>(extractionDataUsed.data());

	int i=0;
	while(bytesUsedMask > 0) {
		unsigned int extractionByteIndex = __tzcnt_u32(bytesUsedMask);
		setExtractionBytePosition(i, existing.getExtractionBytePosition(extractionByteIndex));
		setExtractionByte(i, extractionBytesUsed[extractionByteIndex]);
		bytesUsedMask = _blsr_u32(bytesUsedMask);
		++i;
	}
}

template<unsigned int numberExtractionMasks> inline MultiMaskPartialKeyMapping<numberExtractionMasks>::MultiMaskPartialKeyMapping(
	uint16_t const numberBytesUsed, uint16_t const numberBitsUsed,
	ExtractionDataArray const & extractionPositions,
	ExtractionDataArray const & extractionData
) : PartialKeyMappingBase(getMostSignificantBitIndexInByte(getExtractionByteAt(extractionData,0)) + (getExtractionByteAt(extractionPositions, 0) * 8), getLeastSignificantBitIndexInByte(getExtractionByteAt(extractionData, numberBytesUsed - 1)) + (getExtractionByteAt(extractionPositions, numberBytesUsed - 1) * 8)),
	mNumberExtractionBytes(numberBytesUsed), mNumberKeyBits(numberBitsUsed),
	mExtractionPositions(extractionPositions),
	mExtractionData(extractionData)
{
}

template<unsigned int numberExtractionMasks>
inline MultiMaskPartialKeyMapping<numberExtractionMasks>::MultiMaskPartialKeyMapping(
	uint16_t const mostSignificantBitIndex, uint16_t const leastSignificantBitIndex,
	uint16_t const numberBytesUsed, uint16_t const bitsUsed
) : PartialKeyMappingBase(mostSignificantBitIndex, leastSignificantBitIndex), mNumberExtractionBytes(numberBytesUsed), mNumberKeyBits(bitsUsed) {
}

template<unsigned int numberExtractionMasks> inline __attribute__((always_inline)) uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::extractMask(uint8_t const * keyBytes) const {
	return extractMaskForMappedInput(mapInput(keyBytes));
}

/**
 * Extracts a bit masks corresponding to 111111111|00000000 with the first zero bit marking the misssmatching bit index.
 */
template<unsigned int numberExtractionMasks> template<typename PartialKeyType> inline PartialKeyType MultiMaskPartialKeyMapping<numberExtractionMasks>::getPrefixBitsMask(DiscriminativeBit const &significantKeyInformation) const {
	//initialized to zero
	ExtractionDataArray prefixBits = zeroInitializedArray();
	uint8_t subtreeMissmatchingByte = static_cast<uint8_t >(UINT8_MAX) << (8 - significantKeyInformation.mByteRelativeBitIndex);

	//PERFORMANCE use SIMD helper and replace with operation on positions array -> smaller or equal in combination with sugestion from SingleMaskPartialKeyMapping
	for (int i = 0; i < mNumberExtractionBytes; ++i) {
		unsigned int byteIndex = getExtractionBytePosition(i);
		if (byteIndex < significantKeyInformation.mByteIndex) {
			setExtractionByteAt(prefixBits, i, 0xFF);
		} else if (byteIndex == significantKeyInformation.mByteIndex) {
			setExtractionByteAt(prefixBits, i, subtreeMissmatchingByte);
		}
	}

	return extractMaskForMappedInput(prefixBits);
}

template<unsigned int numberExtractionMasks> inline uint16_t MultiMaskPartialKeyMapping<numberExtractionMasks>::calculateNumberBitsUsed() const {
	return mNumberKeyBits;
}

template<unsigned int numberExtractionMasks> template<typename Operation> inline auto MultiMaskPartialKeyMapping<numberExtractionMasks>::insert(DiscriminativeBit const & significantKeyInformation, Operation const & operation) const {
	SIMDRegisterType simdMask = getMaskForPositionsLargerOrEqualTo(significantKeyInformation.mByteIndex);
	//| (UINT32_MAX << mNumberExtractionBytes) is important here. it corresponds to all bytes which are currently unused. Hence, they are expected to be larger
	uint32_t maskForPositionsLargerOrEqual = SIMDHelperType::moveMask8(simdMask) | (UINT32_MAX << mNumberExtractionBytes);
	unsigned int possibleExtractionByteIndex = __tzcnt_u32(maskForPositionsLargerOrEqual);
	bool isByteContainedAtPosition = (possibleExtractionByteIndex < mNumberExtractionBytes) & (getExtractionBytePosition(possibleExtractionByteIndex) == significantKeyInformation.mByteIndex);

	constexpr unsigned int nextNumberExtractionMasks = numberExtractionMasks == 4 ? numberExtractionMasks : numberExtractionMasks * 2;
	return (isByteContainedAtPosition)
		? operation(MultiMaskPartialKeyMapping<numberExtractionMasks>(*this, significantKeyInformation, possibleExtractionByteIndex))
		: (mNumberExtractionBytes < (numberExtractionMasks * 8)
		   ? operation(MultiMaskPartialKeyMapping<numberExtractionMasks>(*this, significantKeyInformation, possibleExtractionByteIndex, simdMask))
		   : operation(MultiMaskPartialKeyMapping<nextNumberExtractionMasks>(*this, significantKeyInformation, possibleExtractionByteIndex, simdMask))
		  );
}

template<unsigned int numberExtractionMasks> template<typename Operation>
inline auto MultiMaskPartialKeyMapping<numberExtractionMasks>::extract(uint32_t bitsUsed, Operation const &operation) const {
	assert(_mm_popcnt_u32(bitsUsed) >= 1);

	ExtractionDataArray extractionDataForBitsUsed = getUsedExtractionBitsForMask(bitsUsed);
	uint32_t bytesUsedMask = getBytesUsedMaskForExtractionData(extractionDataForBitsUsed);

	unsigned int mostSignificantExtractionByteIndex = _tzcnt_u32(bytesUsedMask);
	unsigned int leastSignificantExtractionByteIndex = getLeastSignificantBytIndexForBytesUsedMask(bytesUsedMask);

	unsigned int leastSignificantExtractionBytePosition = getExtractionBytePosition(leastSignificantExtractionByteIndex);
	unsigned int mostSignificantExtractionBytePosition = getExtractionBytePosition(mostSignificantExtractionByteIndex);

	unsigned int leastSignificantBytePositionInBits = convertBytesToBits(leastSignificantExtractionBytePosition);
	unsigned int mostSignificantBytePositionInBits = convertBytesToBits(mostSignificantExtractionBytePosition);

	uint8_t leastSignificantExtractedByte = getExtractionByteAt(extractionDataForBitsUsed, leastSignificantExtractionByteIndex);
	uint16_t leastSignificantBitPosition = leastSignificantBytePositionInBits + getLeastSignificantBitIndexInByte(leastSignificantExtractedByte);

	uint8_t mostSignificantExtractedByte = getExtractionByteAt(extractionDataForBitsUsed, mostSignificantExtractionByteIndex);
	uint16_t mostSignificantBitPosition = mostSignificantBytePositionInBits + getMostSignificantBitIndexInByte(mostSignificantExtractedByte);

	unsigned int isRandomExtractionMask = (leastSignificantExtractionBytePosition - mostSignificantExtractionBytePosition) >= 8;

	uint16_t numberBytesUsed = _mm_popcnt_u32(bytesUsedMask);
	//log2 + shift right ---> numberBytes 0-7 > 0, numberBytes 7 - 16 > 1, numberBytes 17 - 24 > 2, numberBytes 25 - 32 > 3
	uint32_t numberRandomExtractionMasksMinusOne = (numberBytesUsed-1)/8;

	assert((numberRandomExtractionMasksMinusOne + isRandomExtractionMask) <= numberExtractionMasks);
	assert((numberRandomExtractionMasksMinusOne + isRandomExtractionMask) <= 4);
	switch(numberRandomExtractionMasksMinusOne + isRandomExtractionMask) {
		case 0:
			return operation(SingleMaskPartialKeyMapping(
				reinterpret_cast<uint8_t const *>(mExtractionPositions.data()),
				reinterpret_cast<uint8_t const *>(extractionDataForBitsUsed.data()),
				bytesUsedMask,
				mostSignificantBitPosition,
				leastSignificantBitPosition
			));
		case 1:
			return operation(MultiMaskPartialKeyMapping<1u>(
				(MultiMaskPartialKeyMapping<numberExtractionMasks> const &)(*this), bytesUsedMask, extractionDataForBitsUsed,
				mostSignificantBitPosition, leastSignificantBitPosition, numberBytesUsed, static_cast<uint16_t>(_mm_popcnt_u32(bitsUsed))
			));
		case 2:
			if(numberExtractionMasks >= 2) {
				return operation(MultiMaskPartialKeyMapping<2u>(
					*this, bytesUsedMask, extractionDataForBitsUsed,
					mostSignificantBitPosition, leastSignificantBitPosition, numberBytesUsed, static_cast<uint16_t>(_mm_popcnt_u32(bitsUsed))
				));
			}
      // fall through
		default: //case 3+4
			if(numberExtractionMasks >= 4) {
				return operation(MultiMaskPartialKeyMapping<4u>(
					*this, bytesUsedMask, extractionDataForBitsUsed,
					mostSignificantBitPosition, leastSignificantBitPosition, numberBytesUsed, static_cast<uint16_t>(_mm_popcnt_u32(bitsUsed))
				));
			}
	}
	assert(false); //unreachable next is to only compile correctly without warning
	return operation(*reinterpret_cast<MultiMaskPartialKeyMapping<4u>*>(0ul));
}

template<unsigned int numberExtractionMasks> inline std::array<uint8_t, 256> MultiMaskPartialKeyMapping<numberExtractionMasks>::createIntermediateKeyWithOnlySignificantBitsSet() const {
	std::array<uint8_t, 256> intermediateKey;
	std::memset(intermediateKey.data(), 0, 256);
	for(size_t i=0u; i < mNumberExtractionBytes; ++i) {
		intermediateKey[getExtractionBytePosition(i)] = getExtractionByte(i);
	}
	return intermediateKey;
};


template<> template<typename Operation> inline auto MultiMaskPartialKeyMapping<1u>::executeWithCorrectMaskAndDiscriminativeBitsRepresentation(Operation const & operation) const {
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

template<> template<typename Operation> inline auto MultiMaskPartialKeyMapping<2u>::executeWithCorrectMaskAndDiscriminativeBitsRepresentation(Operation const & operation) const {
	return (mNumberKeyBits <= 16)
		   ? operation(*this, static_cast<uint16_t>(UINT16_MAX))
		   : MultiMaskPartialKeyMapping<4u>(*this).executeWithCorrectMaskAndDiscriminativeBitsRepresentation(operation);
}

template<> template<typename Operation> inline auto MultiMaskPartialKeyMapping<4u>::executeWithCorrectMaskAndDiscriminativeBitsRepresentation(Operation const & operation) const {
	return operation(*this, static_cast<uint32_t>(UINT32_MAX));
}

template<unsigned int numberExtractionMasks> template<typename Operation> inline auto MultiMaskPartialKeyMapping<numberExtractionMasks>::executeWithCompressedDiscriminativeBitsRepresentation(Operation const & operation) const {
	uint8_t leasSignificantExtractionBytePosition = getExtractionBytePosition(getNumberExtractionBytes() - 1);
	uint8_t mostSignificantExtractionBytePosition = getExtractionBytePosition(0);
	size_t extractionByteRange = leasSignificantExtractionBytePosition - mostSignificantExtractionBytePosition;
	if(extractionByteRange < 8) {
		operation(SingleMaskPartialKeyMapping(reinterpret_cast<uint8_t const*>(mExtractionPositions.data()), reinterpret_cast<uint8_t const*>(mExtractionData.data()), getMaskForExtractionBytesUsed(), mMostSignificantDiscriminativeBitIndex, mLeastSignificantDiscriminativeBitIndex));
	} else if(getNumberExtractionBytes() <= 8) {
		std::array<uint64_t, 1> temporaryExtractionData = { mExtractionData[0] };
		std::array<uint64_t, 1> temporaryExtractionPositions = { mExtractionPositions[0] };
		operation(MultiMaskPartialKeyMapping<1>(mNumberExtractionBytes, mNumberKeyBits, temporaryExtractionPositions, temporaryExtractionData));
	} else if(getNumberExtractionBytes() <= 16) {
		std::array<uint64_t, 2> temporaryExtractionData = { mExtractionData[0], mExtractionData[1] };
		std::array<uint64_t, 2> temporaryExtractionPositions = { mExtractionPositions[0], mExtractionPositions[1] };
		operation(MultiMaskPartialKeyMapping<2>(mNumberExtractionBytes, mNumberKeyBits, temporaryExtractionPositions, temporaryExtractionData));
	} else {
		operation(*this);
	}
};

template<> inline __m64 MultiMaskPartialKeyMapping<1u>::getMaskForPositionsLargerOrEqualTo(unsigned int bytePosition) const {
	__m64 bytePositionSearchRegister = _mm_set1_pi8(bytePosition);
	__m64 positionsRegister = getPositionsRegister();
	return _mm_cmpeq_pi8( positionsRegister, _mm_max_pu8(bytePositionSearchRegister, positionsRegister));
}

template<> inline __m128i MultiMaskPartialKeyMapping<2u>::getMaskForPositionsLargerOrEqualTo(unsigned int bytePosition) const {
	__m128i bytePositionSearchRegister = _mm_set1_epi8(bytePosition);
	__m128i positionsRegister= getPositionsRegister();
	return _mm_cmpeq_epi8( positionsRegister, _mm_max_epu8(bytePositionSearchRegister, positionsRegister));
}

template<> inline __m256i MultiMaskPartialKeyMapping<4u>::getMaskForPositionsLargerOrEqualTo(unsigned int bytePosition) const {
	__m256i bytePositionSearchRegister = _mm256_set1_epi8(bytePosition);
	__m256i positionsRegister= getPositionsRegister();
	return _mm256_cmpeq_epi8(positionsRegister, _mm256_max_epu8(bytePositionSearchRegister, positionsRegister));
}

template<> inline void MultiMaskPartialKeyMapping<1u>::initializeDataAndPositionsWithZero() {
	mExtractionPositions[0] = 0;
	mExtractionData[0] = 0;
}

template<> inline void MultiMaskPartialKeyMapping<2u>::initializeDataAndPositionsWithZero() {
	_mm256_storeu_si256(reinterpret_cast<__m256i*>(mExtractionPositions.data()), _mm256_setzero_si256());
}

template<> inline void MultiMaskPartialKeyMapping<4u>::initializeDataAndPositionsWithZero() {
	__m256i zero = _mm256_setzero_si256();
	_mm256_storeu_si256(reinterpret_cast<__m256i*>(mExtractionPositions.data()), zero);
	_mm256_storeu_si256(reinterpret_cast<__m256i*>(mExtractionData.data()), zero);
}

template<unsigned int numberExtractionMasks> inline typename MultiMaskPartialKeyMapping<numberExtractionMasks>::ExtractionDataArray MultiMaskPartialKeyMapping<numberExtractionMasks>::mapInput(uint8_t const __restrict__ * keyBytes) const {
	ExtractionDataArray mappedInput = zeroInitializedArray();
	uint8_t* __restrict__ mappedInputBytes = reinterpret_cast<uint8_t*>(mappedInput.data());
	uint8_t const * __restrict__ positions = reinterpret_cast<uint8_t const* >(mExtractionPositions.data());
	for(int i=0; i < mNumberExtractionBytes; ++i) {
		mappedInputBytes[i] = keyBytes[positions[i]];
	}
	return std::move(mappedInput);
};

template<> inline __attribute__((always_inline)) uint32_t MultiMaskPartialKeyMapping<1u>::extractMaskForMappedInput(typename MultiMaskPartialKeyMapping<1u>::ExtractionDataArray const & mappedInputData) const {
	return _pext_u64(mappedInputData[0], mExtractionData[0]);
}

template<> inline __attribute__((always_inline)) uint32_t MultiMaskPartialKeyMapping<2u>::extractMaskForMappedInput(typename MultiMaskPartialKeyMapping<2u>::ExtractionDataArray const & mappedInputData) const {
	uint64_t const mask1 = mExtractionData[0];
	uint64_t const mask2 = mExtractionData[1];

	uint64_t firstMask = _pext_u64(mappedInputData[0], mask1);
	uint64_t secondMask = _pext_u64(mappedInputData[1], mask2);
	//larger byte positions result in larger bit position => the most significant bits correspond to the least significant bytes.
	return firstMask + (secondMask << _mm_popcnt_u64(mask1));
}

template<> inline __attribute__((always_inline)) uint32_t MultiMaskPartialKeyMapping<4u>::extractMaskForMappedInput(typename MultiMaskPartialKeyMapping<4u>::ExtractionDataArray const & mappedInputData) const {
	const uint64_t mask1 = mExtractionData[0];
	const uint64_t mask2 = mExtractionData[1];
	const uint64_t mask3 = mExtractionData[2];
	const uint64_t mask4 = mExtractionData[3];

	uint64_t firstMask = _pext_u64(mappedInputData[0], mask1);
	uint64_t secondMask = _pext_u64(mappedInputData[1], mask2);
	uint64_t thirdMask = _pext_u64(mappedInputData[2], mask3);
	uint64_t fourthMask = _pext_u64(mappedInputData[3], mask4);

	unsigned int firstOffset = _mm_popcnt_u64(mask1);
	unsigned int secondOffset = _mm_popcnt_u64(mask2) + firstOffset;
	unsigned int thirdOffset = _mm_popcnt_u64(mask3) + secondOffset;

	return firstMask + (secondMask << firstOffset) + (thirdMask << secondOffset) + (fourthMask << thirdOffset);
}


template<unsigned int numberExtractionMasks> inline uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getMaskForHighestBit() const {
	unsigned int byteRelativeBitPositionForHighestBit = mMostSignificantDiscriminativeBitIndex - convertBytesToBits(getExtractionBytePosition(0));

	uint64_t extractionByteForHighestBit = getExtractionByteWithBitSetAtRelativePosition(byteRelativeBitPositionForHighestBit);

	ExtractionDataArray extractionData = zeroInitializedArray();
	extractionData[0] = extractionByteForHighestBit;

	return extractMaskForMappedInput(extractionData);
}

template<unsigned int numberExtractionMasks> inline uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getMaskFor(DiscriminativeBit const & significantKeyInformation) const {
	ExtractionDataArray extractionData = zeroInitializedArray();
	setExtractionByteAt(
		extractionData,
		getExtractionByteIndexForPosition(significantKeyInformation.mByteIndex),
		significantKeyInformation.getExtractionByte()
	);
	return extractMaskForMappedInput(extractionData);
}

template<unsigned int numberExtractionMasks> inline uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getAllMaskBits() const {
	uint32_t numberUnusedBits = (32 - mNumberKeyBits);
	return UINT32_MAX >> numberUnusedBits;
}

template<unsigned int numberExtractionMasks> inline uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getExtractionByteIndexForPosition(uint16_t bytePosition) const {
	SIMDRegisterType needle = SIMDHelperType::set1_epi8(bytePosition);
	SIMDRegisterType haystack = getPositionsRegister();
	uint32_t bytePositionMask = SIMDHelperType::moveMask8(SIMDHelperType::cmpeq_epi8(needle, haystack)) & getMaskForExtractionBytesUsed();
	assert(bytePositionMask != 0);
	assert(_mm_popcnt_u32(bytePositionMask) == 1);
	return _tzcnt_u32(bytePositionMask);
}



template<unsigned int numberExtractionMasks> inline uint8_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getExtractionByteWithBitSetAtRelativePosition(uint16_t byteRelativeBitPosition) {
	return BYTE_WITH_HIGHEST_BIT_SET >> byteRelativeBitPosition;
}

template<> inline typename MultiMaskPartialKeyMapping<1u>::ExtractionDataArray MultiMaskPartialKeyMapping<1u>::getUsedExtractionBitsForMask(uint32_t usedMaskBits) const {
	return { _pdep_u64(usedMaskBits, mExtractionData[0]) };
}

template<> inline typename MultiMaskPartialKeyMapping<2u>::ExtractionDataArray MultiMaskPartialKeyMapping<2u>::getUsedExtractionBitsForMask(uint32_t usedMaskBits) const {
	uint64_t const extractionMask1 = mExtractionData[0];
	uint64_t const extractionMask2 = mExtractionData[1];

	uint64_t const usedBits1 = usedMaskBits;
	uint64_t const usedBits2 = usedMaskBits >> _mm_popcnt_u64(extractionMask1);

	return { _pdep_u64(usedBits1, extractionMask1), _pdep_u64(usedBits2, extractionMask2) };
}

template<> inline typename MultiMaskPartialKeyMapping<4u>::ExtractionDataArray MultiMaskPartialKeyMapping<4u>::getUsedExtractionBitsForMask(uint32_t usedMaskBits) const {
	//PERFORMANCE can this be generalized into template with loop, without performance impacts ??
	uint64_t const extractionMask1 = mExtractionData[0];
	uint64_t const extractionMask2 = mExtractionData[1];
	uint64_t const extractionMask3 = mExtractionData[2];
	uint64_t const extractionMask4 = mExtractionData[3];

	uint64_t const usedBits1 = usedMaskBits;
	uint64_t const usedBits2 = usedBits1 >> _mm_popcnt_u64(extractionMask1);
	uint64_t const usedBits3 = usedBits2 >> _mm_popcnt_u64(extractionMask2);
	uint64_t const usedBits4 = usedBits3 >> _mm_popcnt_u64(extractionMask3);

	return { _pdep_u64(usedBits1, extractionMask1), _pdep_u64(usedBits2, extractionMask2), _pdep_u64(usedBits3, extractionMask3), _pdep_u64(usedBits4, extractionMask4) };
}

template<> inline typename MultiMaskPartialKeyMapping<1u>::ExtractionDataArray MultiMaskPartialKeyMapping<1u>::zeroInitializedArray() {
	ExtractionDataArray data;
	data[0] = 0ul;
	return std::move(data);
}

template<> inline typename MultiMaskPartialKeyMapping<2u>::ExtractionDataArray MultiMaskPartialKeyMapping<2u>::zeroInitializedArray() {
	ExtractionDataArray data;
	data[0] = 0ul;
	data[1] = 0ul;
	return std::move(data);
}

template<> inline typename MultiMaskPartialKeyMapping<4u>::ExtractionDataArray MultiMaskPartialKeyMapping<4u>::zeroInitializedArray() {
	ExtractionDataArray data;
	_mm256_storeu_si256(reinterpret_cast<__m256i*>(data.data()), _mm256_setzero_si256());
	return std::move(data);
}

template<unsigned int numberExtractionMasks> inline uint8_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getExtractionByteAt(typename MultiMaskPartialKeyMapping<numberExtractionMasks>::ExtractionDataArray const &extractionData, uint32_t extractionByteIndex) {
	return reinterpret_cast<uint8_t const *>(extractionData.data())[extractionByteIndex];
}


template<unsigned int numberExtractionMasks>  inline void MultiMaskPartialKeyMapping<numberExtractionMasks>::setExtractionByteAt(typename MultiMaskPartialKeyMapping<numberExtractionMasks>::ExtractionDataArray  & extractionData, uint32_t extractionByteIndex, uint8_t extractionByte) {
	reinterpret_cast<uint8_t *>(extractionData.data())[extractionByteIndex] = extractionByte;
}

template<unsigned int numberExtractionMasks>  inline uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getMaskForExtractionBytesUsed() const {
	return (UINT32_MAX >> (32 - mNumberExtractionBytes));
}

template<unsigned int numberExtractionMasks> inline uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getBytesUsedMaskForExtractionData(typename MultiMaskPartialKeyMapping<numberExtractionMasks>::ExtractionDataArray const & extractionData) const {
	return ~SIMDHelperType::moveMask8(SIMDHelperType::cmpeq_epi8(SIMDHelperType::toRegister(extractionData), SIMDHelperType::zero())) & getMaskForExtractionBytesUsed();
}

template<unsigned int numberExtractionMasks> inline uint32_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getLeastSignificantBytIndexForBytesUsedMask(uint32_t bytesUsedMask) const {
	return 31 - _lzcnt_u32(bytesUsedMask);
}

template<unsigned int numberExtractionMasks> template<typename PartialKeyType> inline PartialKeyType MultiMaskPartialKeyMapping<numberExtractionMasks>::getMostSignifikantMaskBit(PartialKeyType mask) const {
	ExtractionDataArray usedExtractionBits = getUsedExtractionBitsForMask(mask);
	uint32_t usedExtractionBytesMask = getBytesUsedMaskForExtractionData(usedExtractionBits);
	uint32_t highestExtractionByteIndex = __tzcnt_u32(usedExtractionBytesMask);
	uint8_t highestByte = getExtractionByteAt(usedExtractionBits, highestExtractionByteIndex);
	uint8_t extractionByteWithHighestBitSet = BYTE_WITH_HIGHEST_BIT_SET >> (_lzcnt_u32(highestByte) - 24);

	ExtractionDataArray extractionData = zeroInitializedArray();
	setExtractionByteAt(extractionData, highestExtractionByteIndex, extractionByteWithHighestBitSet);

	return static_cast<PartialKeyType>(extractMaskForMappedInput(extractionData));
}

template<unsigned int numberExtractionMasks> inline uint16_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getLeastSignificantBitIndex(uint32_t mask) const {
	ExtractionDataArray usedExtractionBits = getUsedExtractionBitsForMask(mask);
	uint32_t lowestExtractionByteIndex = getLeastSignificantBytIndexForBytesUsedMask(getBytesUsedMaskForExtractionData(usedExtractionBits));
	uint8_t lowestByte = getExtractionByteAt(usedExtractionBits, lowestExtractionByteIndex);
	return convertBytesToBits(getExtractionBytePosition(lowestExtractionByteIndex)) + getLeastSignificantBitIndexInByte(lowestByte);
}


template<unsigned int numberExtractionMasks> inline uint8_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getExtractionBytePosition(unsigned int index) const {
	return reinterpret_cast<uint8_t const *>(mExtractionPositions.data())[index];
}

template<unsigned int numberExtractionMasks> inline void MultiMaskPartialKeyMapping<numberExtractionMasks>::setExtractionBytePosition(unsigned int index, uint8_t extractionByte) {
	reinterpret_cast<uint8_t *>(mExtractionPositions.data())[index] = extractionByte;
}

template<unsigned int numberExtractionMasks> inline uint8_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getExtractionByte(unsigned int index) const {
	return reinterpret_cast<uint8_t const *>(mExtractionData.data())[index];
}

template<unsigned int numberExtractionMasks> inline void MultiMaskPartialKeyMapping<numberExtractionMasks>::setExtractionByte(unsigned int index, uint8_t extractionByte) {
	reinterpret_cast<uint8_t*>(mExtractionData.data())[index] = extractionByte;
}

template<unsigned int numberExtractionMasks> inline std::set<uint16_t> MultiMaskPartialKeyMapping<numberExtractionMasks>::getDiscriminativeBits() const {
	std::set<uint16_t> extractionBits;
	for(int i = 0; i < mNumberExtractionBytes; ++i) {
		unsigned int extractionBasePosition = convertBytesToBits(getExtractionBytePosition(i));
		uint32_t extractionByte = getExtractionByte(i);
		while(extractionByte > 0) {
			uint32_t byteRelativeBitPosition = __lzcnt32(extractionByte) - 24;
			extractionBits.insert(extractionBasePosition + byteRelativeBitPosition);
			extractionByte &= ~getExtractionByteWithBitSetAtRelativePosition(byteRelativeBitPosition);
		}
	}
	return extractionBits;
}

template<unsigned int numberExtractionMasks> inline uint16_t MultiMaskPartialKeyMapping<numberExtractionMasks>::getNumberExtractionBytes() const {
	return mNumberExtractionBytes;
}

} }

#endif

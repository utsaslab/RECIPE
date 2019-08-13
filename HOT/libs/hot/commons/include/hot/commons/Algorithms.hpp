#ifndef __HOT__COMMONS__ALGORITHMS__
#define __HOT__COMMONS__ALGORITHMS__

#include <cassert>
#include <bitset>
#include <immintrin.h>
#include <iostream>

namespace hot { namespace commons {

inline uint32_t getBytesUsedInExtractionMask(uint64_t successiveExtractionMask) {
	uint32_t const unsetBytes = _mm_movemask_pi8(_mm_cmpeq_pi8(_mm_and_si64(_mm_set_pi64x(successiveExtractionMask), _mm_set_pi64x(UINT64_MAX)), _mm_setzero_si64()));
	//8 - numberUnsetBytes
	return 8 - _mm_popcnt_u32(unsetBytes);
}

inline uint16_t getMaximumMaskByteIndex(uint16_t bitsUsed) {
	return (bitsUsed - 1)/8;
}

template<uint numberExtractionMasks> inline std::array<uint64_t, numberExtractionMasks> getUsedExtractionBitsForMask(uint32_t usedBits, uint64_t  const * extractionMask);

template<uint numberExtractionMasks> inline __m256i extractionMaskToRegister(std::array<uint64_t, numberExtractionMasks> const & extractionData);

template<> inline __m256i extractionMaskToRegister<1>(std::array<uint64_t, 1> const & extractionData) {
	return _mm256_set_epi64x(extractionData[0], 0ul, 0ul, 0ul);
};

template<> inline __m256i extractionMaskToRegister<2>(std::array<uint64_t, 2> const & extractionData) {
	return _mm256_set_epi64x(extractionData[0], extractionData[1], 0ul, 0ul);
}

template<> inline __m256i extractionMaskToRegister<4>(std::array<uint64_t, 4> const & extractionData) {
	return _mm256_loadu_si256(reinterpret_cast<__m256i const *>(extractionData.data()));
}

template<size_t numberBytes> inline std::array<uint8_t, numberBytes>  extractSuccesiveFromRandomBytes(uint8_t const * bytes, uint8_t const * bytePositions) {
	std::array<uint8_t, numberBytes> succesiveBytes;
	for(uint i=0; i < numberBytes; ++i) {
		succesiveBytes[i] = bytes[bytePositions[i]];
	}
	return std::move(succesiveBytes);
}


/**
 * Given a bitindex this function returns its corresponding byte index
 *
 * @param bitIndex the bit index to convert to byte Level
 * @return the byte index
 */
inline unsigned int getByteIndex(unsigned int bitIndex) {
	return bitIndex/8;
}


/**
 * gets the number of bytes needed to represent the successive bytes from (inclusive) the byte containing
 * the mostSignificantBitIndex until (inclusive) the byte containing the leastSignificantBitIndex
 *
 * @param mostSignificantBitIndex the index of the most significant bit
 * @param leastSignificantBitIndex the index of the least significant bit
 * @return the size of the range in bytes
 */
inline uint getByteRangeSize(uint mostSignificantBitIndex, uint leastSignificantBitIndex) {
	return getByteIndex(leastSignificantBitIndex) - getByteIndex(mostSignificantBitIndex);
}

constexpr inline uint16_t convertBytesToBits(uint16_t const byteIndex) {
	return byteIndex * 8;
}

constexpr inline uint16_t bitPositionInByte(uint16_t const absolutBitPosition) {
	return absolutBitPosition % 8;
}

constexpr uint64_t HIGHEST_UINT64_BIT = (1ul << 63);

inline uint getSuccesiveByteOffsetForMostRightByte(uint mostRightByte) {
	return std::max(0, ((int) mostRightByte) - 7);
}

inline bool isNoMissmatch(std::pair<uint8_t const*, uint8_t const*> const & missmatch, uint8_t const* key1, uint8_t const* key2, size_t keyLength)  {
	return missmatch.first == (key1 + keyLength) && missmatch.second == (key2 + keyLength);
}

inline bool isBitSet(uint8_t const * existingRawKey, uint16_t const mAbsoluteBitIndex) {
	return (existingRawKey[getByteIndex(mAbsoluteBitIndex)] & (0b10000000 >> bitPositionInByte(mAbsoluteBitIndex))) > 0;
}

inline uint16_t getLeastSignificantBitIndexInByte(uint8_t byte) {
	return (7 - _tzcnt_u32(byte));
}

inline uint16_t getMostSignificantBitIndexInByte(uint8_t byte) {
	assert(byte > 0);
	return _lzcnt_u32(byte) - 24;
}

inline __attribute__((always_inline)) int getMostSignificantBitIndex(uint32_t number) {
	int msb;
	asm("bsr %1,%0" : "=r"(msb) : "r"(number));
	return msb;
}

} }

#endif

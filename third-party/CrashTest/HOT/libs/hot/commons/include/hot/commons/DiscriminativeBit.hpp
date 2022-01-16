#ifndef __HOT__COMMONS__DISCRIMINATIVE_BIT__
#define __HOT__COMMONS__DISCRIMINATIVE_BIT__

#include "hot/commons/Algorithms.hpp"
#include "idx/contenthelpers/OptionalValue.hpp"

namespace hot { namespace commons {

constexpr uint8_t BYTE_WITH_MOST_SIGNIFICANT_BIT = 0b10000000;

/**
 * Describes a keys single bit by its position and its value
 */
struct DiscriminativeBit {
	uint16_t const mByteIndex;
	uint16_t const mByteRelativeBitIndex;
	uint16_t const mAbsoluteBitIndex;
	uint mValue;


public:
	inline DiscriminativeBit(uint16_t const significantByteIndex, uint8_t const existingByte, uint8_t const newKeyByte);
	inline DiscriminativeBit(uint16_t const absoluteSignificantBitIndex, uint const newBitValue=1);

	inline uint8_t getExtractionByte() const;

private:
	static inline uint16_t getByteRelativeSignificantBitIndex(uint8_t const existingByte, uint8_t const newKeyByte);

};

inline DiscriminativeBit::DiscriminativeBit(uint16_t const significantByteIndex, uint8_t const existingByte, uint8_t const newKeyByte)
	: mByteIndex(significantByteIndex)
	, mByteRelativeBitIndex(getByteRelativeSignificantBitIndex(existingByte, newKeyByte))
	, mAbsoluteBitIndex(convertBytesToBits(mByteIndex) + mByteRelativeBitIndex)
	, mValue(((BYTE_WITH_MOST_SIGNIFICANT_BIT >> mByteRelativeBitIndex) & newKeyByte) != 0)
{
}

inline DiscriminativeBit::DiscriminativeBit(uint16_t const absoluteSignificantBitIndex, uint const newBitValue)
	: mByteIndex(getByteIndex(absoluteSignificantBitIndex))
	, mByteRelativeBitIndex(bitPositionInByte(absoluteSignificantBitIndex))
	, mAbsoluteBitIndex(absoluteSignificantBitIndex)
	, mValue(newBitValue)
{
}

inline uint8_t DiscriminativeBit::getExtractionByte() const {
	return 1 << (7 - mByteRelativeBitIndex);
}

inline uint16_t DiscriminativeBit::getByteRelativeSignificantBitIndex(uint8_t const existingByte, uint8_t const newKeyByte) {
	uint32_t mismatchByteBitMask = existingByte ^ newKeyByte;
	return __builtin_clz(mismatchByteBitMask) - 24;
}

template<typename Operation> inline bool executeForDiffingKeys(uint8_t const* existingKey, uint8_t const* newKey, uint16_t keyLengthInBytes, 
        Operation const & operation) {
	for(size_t index = 0; index < keyLengthInBytes; ++index) {
		uint8_t newByte = newKey[index];
		uint8_t existingByte = existingKey[index];
		if(existingByte != newByte) {
			operation(DiscriminativeBit {static_cast<uint16_t>(index), existingByte, newByte });
			return true;
		}
	}
	return false;
};

inline idx::contenthelpers::OptionalValue<DiscriminativeBit> getMismatchingBit(uint8_t const* existingKey, uint8_t const* newKey, uint16_t keyLengthInBytes) {
	for(size_t index = 0; index < keyLengthInBytes; ++index) {
		uint8_t newByte = newKey[index];
		uint8_t existingByte = existingKey[index];
		if(existingByte != newByte) {
			return { true, DiscriminativeBit { static_cast<uint16_t>(index), existingByte, newByte } };
		}
	}
	return { false, { 0, 0, 0 }};
};

} }

#endif

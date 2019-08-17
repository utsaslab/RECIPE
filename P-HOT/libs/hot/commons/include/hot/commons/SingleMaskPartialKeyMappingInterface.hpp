#ifndef __HOT__COMMONS__SINGLE_MASK_PARTIAL_KEY_MAPPING_INTERFACE_HPP___
#define __HOT__COMMONS__SINGLE_MASK_PARTIAL_KEY_MAPPING_INTERFACE_HPP___


#include "hot/commons/PartialKeyMappingBase.hpp"

namespace hot { namespace commons {

template<uint numberExtractionMasks> struct MultiMaskPartialKeyMapping;

/**
 * A partial key mapping which by using an offset and a 64 bit mask is able to extract partial keys consisting of discriminative bits contained in successive 64 bits.
 *
 */
class alignas(8) SingleMaskPartialKeyMapping : public PartialKeyMappingBase {
public:
	static constexpr uint MINIMUM_EXTRACTION_BIT_COUNT_SUPPORTED = 8;
private:
	uint32_t mOffsetInBytes;
	uint64_t mSuccessiveExtractionMask;

public:
	inline SingleMaskPartialKeyMapping(SingleMaskPartialKeyMapping const &src);

	/**
	 * Creates a single mask partial key mapping for a single discriminative bits.
	 * The resulting partial keys consists only of a single bit. And can therefore only be used to discriminate 2 entries.
	 *
	 * @param discriminativeBit the discriminative bit to create an extraction mask for
	 */
	inline SingleMaskPartialKeyMapping(DiscriminativeBit const &discriminativeBit);

	/**
	 * Creates a single mask partial key mapping from a set of byte positions and corresponding byte masks.
	 * Be aware that all byte positions must lie withing an 8 byte range
	 *
	 * @param extractionBytePositions the byte positions of the correspond byte masks
	 * @param extractionByteData the byte masks describing the discriminative bits used in each of the bytes specified previously by the extractionBytePositions
	 * @param extractionBytesUsedMask if not all of the above byte position/mask pairs schould be considered this mask can be used to determine, which of those pairs should actually be used.
	 * @param mostSignificantBitIndex
	 * @param leastSignificantBitIndex
	 */
	inline SingleMaskPartialKeyMapping(
		uint8_t const* extractionBytePositions,
		uint8_t const * extractionByteData,
		uint32_t const extractionBytesUsedMask,
		uint16_t const mostSignificantBitIndex,
		uint16_t const leastSignificantBitIndex
	);

	/**
	 * Creates a new single mask partial key mapping by adding a new discriminating bit.
	 * Be aware that this is only possible if the all existing discriminative bits and the new discriminating bit lie withing an 8 byte range
	 *
	 * @param existing the existing single mask partial key to add the new discriminating bit to.
	 * @param discriminatingBit the new discriminating bit to add
	 */
	inline SingleMaskPartialKeyMapping(SingleMaskPartialKeyMapping const &existing, DiscriminativeBit const &discriminatingBit);

	/**
	 * Creates a new single mask partial with only a subset of the original discriminating bits used
	 *
	 * @param existing the existing single mask mapping
	 * @param maskBitsNeeded a partial key which has only those bits set which should be represented in the new single mask mapping.
	 */
	inline SingleMaskPartialKeyMapping(SingleMaskPartialKeyMapping const &existing, uint32_t const &maskBitsNeeded);

	/**
	 *
	 * @return the number of discriminative bits represented by this single mask partial key mapping.
	 */
	inline uint16_t calculateNumberBitsUsed() const;

	/**
	 * Given a key information it generates a prefix up to the position represented by the key information and generates a mask corresponding to the bits defined in this extraction mask
	 *
	 * @param significantKeyInformation a description of the position creating a prefix of (the position is exclusive). eg. if the position is 3 the prefix is 11
	 * @return the mask containing the bits corresponding to the prefix described the key information
	 */
	template<typename PartialKeyType> inline PartialKeyType
	getPrefixBitsMask(DiscriminativeBit const &significantKeyInformation) const;

	/**
	 * inserts a new discriminating bit into this single mask mapping.
	 * The resulting new mask is passed to the provided operation
	 *
	 * @tparam Operation a callback which is able to handle the new partial key mapping (can be either of single or multi mask type=
	 * @param discriminativeBit
	 * @param operation the operation which process the new discriminative bits mapping
	 * @return the result of the provided operation
	 */
	template<typename Operation>
	inline auto insert(DiscriminativeBit const &discriminativeBit, Operation const &operation) const;

	/**
	 * extracts only a subset of the discriminative bits.
	 * The resulting new partial key mapping is passed to the provided operation
	 *
	 * @tparam Operation a callback which is able to handle the new partial key mapping (can only be of type single mask)
	 * @param bitsUsed a partial key having only those bits set which should be part of the new partial key mapping
	 * @param operation the operation which process the new discriminative bits mapping
	 * @return the result of the provided operation
	 */
	template<typename Operation>
	inline auto extract(uint32_t bitsUsed, Operation const &operation) const;

	/**
	 * a helper function which invokes a callback with the this partial key mapping itself and the smallest possible partial key type which is necessary to represent partial keys
	 * constructued by this partial key mappings.
	 *
	 * e.g.:
	 * 	+ for less or equal then 8 discriminative bits => uint8_t
	 *  + for less or equal then 16 discriminative bits => uint16_t
	 *  + for less or equal then 32 discriminative bits => uint32_t
	 *
	 *
	 * @tparam Operation the type of the callback to invoke
	 * @param operation the callback
	 * @return the result of the operation
	 */
	template<typename Operation>
	inline auto executeWithCorrectMaskAndDiscriminativeBitsRepresentation(Operation const &operation) const;

	/**
	 *
	 * @return whether all 8 bytes of the underlying mask are used
	 */
	inline bool hasUnusedBytes() const;

	/**
	 *
	 * @return a mask of all the bytes used in the underlying mask
	 */
	inline uint32_t getUsedBytesMask() const;

	/**
	 * @return the internal offset of the stored underlying mask
	 */
	inline uint32_t getByteOffset() const;

	inline uint8_t getExtractionByte(unsigned int byteIndex) const;
	inline uint8_t getExtractionBytePosition(unsigned int byteIndex) const;

	/**
	 *
	 * @return a partial key with only the highest bit set
	 */
	inline uint32_t getMaskForHighestBit() const;

	/**
	 *
	 * @param discriminativeBit the only discriminative bit to extract
	 * @return a partial key with only this discriminative bit set
	 */
	inline uint32_t getMaskFor(DiscriminativeBit const &discriminativeBit) const;

	/**
	 *
	 * @return a mask with all mask bits set. This results in a mask like 00111111 where the number of 1s is equal to the number of keybits
	 */
	inline uint32_t getAllMaskBits() const;

	inline uint32_t extractMask(uint8_t const *keyBytes) const;

	/**
	 *
	 * @return a key which has only those discriminative bits set which are represented by this partial key mapping
	 */
	inline std::array<uint8_t, 256> createIntermediateKeyWithOnlySignificantBitsSet() const;

private:
	//delegating constructor
	inline SingleMaskPartialKeyMapping(SingleMaskPartialKeyMapping const &existing, uint64_t const newExtractionMaskWithSameOffset);

	inline uint32_t extractMaskFromSuccessiveBytes(uint64_t const inputMask) const;

	inline __m64 getRegister() const;

	inline uint64_t getSuccessiveMaskForBit(uint const bytePosition, uint const byteRelativeBitPosition) const;

	static inline uint convertToIndexOfOtherEndiness(uint const maskRelativeBytePosition, uint const byteRelativeBitPosition);

	inline uint64_t getSuccessiveMaskForAbsoluteBitPosition(uint absoluteBitPosition) const;

	inline uint64_t getSuccessiveMaskForMask(uint32_t const mask) const;

	static inline uint getSuccesiveByteOffsetForLeastSignificantBitIndex(uint leastSignificantBitIndex);

	static inline uint16_t calculateRelativeMostSignificantBitIndex(uint64_t rawExtractionMask);

	static inline uint16_t calculateRelativeLeastSignificantBitIndex(uint64_t rawExtractionMask);

	static inline uint64_t getSuccessiveExtractionMaskFromRandomBytes(
		uint8_t const * extractionBytePositions,
		uint8_t const * extractionByteData,
		uint32_t extractionBytesUsedMask,
		uint32_t const offsetInBytes
	);

public:
	/**
	 * Gets a partial key with only the most significant bit, this function is solely need for debugging purposes
	 * Due to little, big endian differences the extraction information must be considered.
	 *
	 * e.g. bit for position 0 might be the 7th bit of the mask.
	 * With less significant bits stored in the bits 8 till 31
	 *
	 * @param mask the mask to extract the most significan bit from
	 * @return the most significant bit of the given mask.
	 */
	template<typename PartialKeyType> inline PartialKeyType getMostSignifikantMaskBit(PartialKeyType mask) const;

	/**
	 *
	 * @param partialKey
	 * @return the least significant bit set in the given partial key
	 */
	inline uint16_t getLeastSignificantBitIndex(uint32_t partialKey) const;

	/**
	 *
	 * @return a set of all the discriminative bit positions represnted by this partial key mapping
	 */
	inline std::set<uint16_t> getDiscriminativeBits() const;
};

} }

#endif
#ifndef __HOT__COMMONS__MULT_MASK_PARTIAL_KEY_MAPPING_INTERFACE___
#define __HOT__COMMONS__MULT_MASK_PARTIAL_KEY_MAPPING_INTERFACE___

#include <immintrin.h>

#include <array>
#include <utility>
#include <type_traits>

#include "hot/commons/PartialKeyMappingBase.hpp"
#include "hot/commons/SIMDHelper.hpp"

namespace hot { namespace commons {

class SingleMaskPartialKeyMapping;

template<unsigned int numberExtractionMasks> class MultiMaskPartialKeyMapping;

/**
 * A partial key mapping which uses an array of byte offstes and a correspond array of byte masks which is able to extract partial keys consisting of those bits, which are stored in the underlying byte masks
 *
 * @tparam numberExtractionMasks the number of the underlying 64 bit masks. 1 implies 8 different mask bytes, 2 implies 16 different mask bytes and 3 implies 32 different mask bytes.
 */
template<unsigned int numberExtractionMasks> class MultiMaskPartialKeyMapping : public PartialKeyMappingBase {
	friend class MultiMaskPartialKeyMapping<1u>;
	friend class MultiMaskPartialKeyMapping<2u>;
	friend class MultiMaskPartialKeyMapping<4u>;
public:
	static constexpr unsigned int MINIMUM_EXTRACTION_BIT_COUNT_SUPPORTED = numberExtractionMasks * 8;
	static constexpr unsigned int NUMBER_BITS = numberExtractionMasks * 64;
	using SIMDHelperType = SIMDHelper<NUMBER_BITS>;
	using SIMDRegisterType = typename SIMDHelperType::SIMDRegisterType;
	using ExtractionDataArray = std::array<uint64_t, numberExtractionMasks>;

private:

	uint16_t mNumberExtractionBytes;
	uint16_t mNumberKeyBits;

	ExtractionDataArray mExtractionPositions;
	ExtractionDataArray mExtractionData;

	inline SIMDRegisterType getPositionsRegister() const;
	inline SIMDRegisterType getExtractionDataRegister() const;
private:
	inline void setPositions(SIMDRegisterType positions);
	inline void setExtractionData(SIMDRegisterType data);

public:
	inline MultiMaskPartialKeyMapping(MultiMaskPartialKeyMapping<numberExtractionMasks> const & src);

	/**
	 * Creates a new multi mask partial key mapping from a compatible partial key mapping.
	 *
	 * @param src a compatible partial key mapping
	 */
	inline MultiMaskPartialKeyMapping(typename std::conditional<numberExtractionMasks == 1, SingleMaskPartialKeyMapping, MultiMaskPartialKeyMapping<numberExtractionMasks/2>>::type const &src);


	/**
	 * Creates a new single mask partial key mapping by adding a new discriminating bit.
	 *
	 * @param existing the existing partial key mapping to add the new discriminating bit to.
	 * @param discriminatingBit the new discriminating bit to add
	 */
	inline MultiMaskPartialKeyMapping(SingleMaskPartialKeyMapping const &existing, DiscriminativeBit const & significantKeyInformation);

	/**
	 * Creates a new single mask partial key mapping by adding a new discriminating bit.
	 *
	 * @param existing the existing partial key mapping to add the new discriminating bit to.
	 * @param discriminatingBit the new discriminating bit to add
	 */
	inline MultiMaskPartialKeyMapping(MultiMaskPartialKeyMapping<numberExtractionMasks> const & existing, DiscriminativeBit const & significantKeyInformation, unsigned int extractionByteIndex);

	inline MultiMaskPartialKeyMapping(
		MultiMaskPartialKeyMapping<numberExtractionMasks> const &existing, DiscriminativeBit const &significantKeyInformation, unsigned int const extractionByteIndex,
		typename MultiMaskPartialKeyMapping<numberExtractionMasks>::SIMDRegisterType const & maskForLessSignificantBytes
	);

	template<unsigned int sourceNumberExtractionMasks>
	inline MultiMaskPartialKeyMapping(
		MultiMaskPartialKeyMapping<sourceNumberExtractionMasks> const & existing, DiscriminativeBit const &significantKeyInformation, unsigned int const extractionByteIndex,
		typename MultiMaskPartialKeyMapping<sourceNumberExtractionMasks>::SIMDRegisterType const & maskForLessSignificantBytes
	);

	template<unsigned int sourceNumberExtractionMasks>
	inline MultiMaskPartialKeyMapping(
		MultiMaskPartialKeyMapping<sourceNumberExtractionMasks> const &existing,
		uint32_t bytesUsedMask,	typename MultiMaskPartialKeyMapping<sourceNumberExtractionMasks>::ExtractionDataArray const & extractionDataUsed,
		uint16_t const numberBytesUsed, uint16_t const bitsUsed,
		uint16_t const mostSignificantBitIndex, uint16_t const leastSignificantBitIndex
	);

	inline MultiMaskPartialKeyMapping(
		uint16_t const numberBytesUsed, uint16_t const bitsUsed,
		ExtractionDataArray const & mExtractionPositions,
		ExtractionDataArray const & mExtractionData
	);

protected:
	inline MultiMaskPartialKeyMapping(
		uint16_t const mostSignificantBitIndex, uint16_t const leastSignificantBitIndex,
		uint16_t const numberBytesUsed, uint16_t const bitsUsed
	);

public:
	inline uint32_t extractMask(uint8_t const *keyBytes) const;

	/**
	 * Given a key information it generates a prefix up to the position represented by the key information and generates a mask corresponding to the bits defined in this extraction mask
	 *
	 * @param significantKeyInformation a description of the position creating a prefix of (the position is exclusive). eg. if the position is 3 the prefix is 11
	 * @return the mask containing the bits corresponding to the prefix described the key information
	 */
	template<typename PartialKeyType> inline PartialKeyType getPrefixBitsMask(DiscriminativeBit const &significantKeyInformation) const;

	/**
	 * Given a key information it generates a prefix up to the position represented by the key information and generates a mask corresponding to the bits defined in this extraction mask
	 *
	 * @param significantKeyInformation a description of the position creating a prefix of (the position is exclusive). eg. if the position is 3 the prefix is 11
	 * @return the mask containing the bits corresponding to the prefix described the key information
	 */
	inline uint16_t calculateNumberBitsUsed() const;

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
	inline auto insert(DiscriminativeBit const &significantKeyInformation, Operation const &operation) const;

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

	inline std::array<uint8_t, 256> createIntermediateKeyWithOnlySignificantBitsSet() const;

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
	template<typename Operation> inline auto executeWithCorrectMaskAndDiscriminativeBitsRepresentation(Operation const & operation) const;

	/**
	 * compresses this partial key mapping to the most space efficient partial key mapping which is able to represent the discriminative bits represented by this partial key mapping
	 *
	 * @tparam Operation the callback type
	 * @param operation the callback receiving the compressed partial key mmapings
	 * @return  the result of the operation
	 */
	template<typename Operation> inline auto executeWithCompressedDiscriminativeBitsRepresentation(Operation const & operation) const;

public:
	inline bool hasUnusedBytes() const;

private:
	inline SIMDRegisterType getMaskForPositionsLargerOrEqualTo(unsigned int bytePosition) const;

	inline void initializeDataAndPositionsWithZero();

	inline ExtractionDataArray mapInput(uint8_t const __restrict__ *keyBytes) const;

	inline uint32_t extractMaskForMappedInput(ExtractionDataArray const &mappedInputData) const;

public:
	/**
	 *
	 * @return a partial key with only the highest bit set
	 */
	inline uint32_t getMaskForHighestBit() const;

	/**
	 * Gets a mask with only the bit set specified by the given key information
	 *
	 * @param significantKeyInformation the key information to specify the byte to extract and map to the output mask
	 * @return the mask with only the specified bit set
	 */
	inline uint32_t getMaskFor(DiscriminativeBit const &significantKeyInformation) const;

	/**
	 * @return a mask with all mask bits set. This results in a mask like 00111111 where the number of 1s is equal to the number of keybits
	 */
	inline uint32_t getAllMaskBits() const;

private:
	inline uint32_t getExtractionByteIndexForPosition(uint16_t bytePosition) const;

	/**
	 * Gets an extractio byte with a single bit set at a position relative to the most significant bit positoin of the byte
	 *
	 * Hence, for byteRelativeBitPosition 0 it returns the byte 0b10000000 and for 7 it returns the byte 0b00000000
	 *
	 * @param byteRelativeBitPosition the position of the bit to set relative to the most significant bit position
	 * @return the byte with the specified bit set
	 */
	static inline uint8_t getExtractionByteWithBitSetAtRelativePosition(uint16_t byteRelativeBitPosition);

	/**
	 * Gets the extraction Data for a given extraction Mask
	 *
	 * @param usedMaskBits the used masks bits
	 *
	 * @return the extraction Bits, which are set according to the mask bits
	 */
	inline ExtractionDataArray getUsedExtractionBitsForMask(uint32_t usedMaskBits) const;


	static inline ExtractionDataArray zeroInitializedArray();

	static inline uint8_t getExtractionByteAt(ExtractionDataArray const & extractionData, uint32_t extractionByteIndex);

	static inline void setExtractionByteAt(ExtractionDataArray &extractionData, uint32_t extractionByteIndex, uint8_t extractionByte);

	inline uint32_t getMaskForExtractionBytesUsed() const;

	inline uint32_t getBytesUsedMaskForExtractionData(ExtractionDataArray const & extractionData) const;

	inline uint32_t getLeastSignificantBytIndexForBytesUsedMask(uint32_t bytesUsedMask) const;

public:
	/**
	 * Gets a mask with only the most significan bit set of the given mask, this function is solely need for debugging purpos
	 * Due to little, big endian differences the extraction information must be considered.
	 *
	 * e.g. bit for position 0 might be the 7th bit of the mask.
	 * With less significant bits stored in the bits 8 till 31
	 *
	 * @param mask the mask to extract the most significan bit from
	 * @return the most significant bit of the given mask.
	 */
	template<typename PartialKeyType>
	inline PartialKeyType getMostSignifikantMaskBit(PartialKeyType mask) const;

	/**
	 *
	 * @param partialKey
	 * @return the least significant bit set in the given partial key
	 */
	inline uint16_t getLeastSignificantBitIndex(uint32_t mask) const;

	inline uint8_t getExtractionBytePosition(unsigned int index) const;

	inline void setExtractionBytePosition(unsigned int index, uint8_t byte);

	inline uint8_t getExtractionByte(unsigned int index) const;

	inline void setExtractionByte(unsigned int index, uint8_t byte);

	/**
	 *
	 * @return a set of all the discriminative bit positions represnted by this partial key mapping
	 */
	inline std::set<uint16_t> getDiscriminativeBits() const;

	/**
	 * @return the number of the underlying 8 byte masks which are currently not 0
	 */
	inline uint16_t getNumberExtractionBytes() const;
};

} }

#endif

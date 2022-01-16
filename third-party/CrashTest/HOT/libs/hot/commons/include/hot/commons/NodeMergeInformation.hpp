#ifndef __HOT__COMMONS__NODE_MERGE_INFORMATION__
#define __HOT__COMMONS__NODE_MERGE_INFORMATION__

#include <cstdint>
#include <array>
#include <type_traits>

#include <immintrin.h>

#include <idx/contenthelpers/OptionalValue.hpp>

#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/BitMask32.hpp>
#include <hot/commons/MultiMaskPartialKeyMapping.hpp>


namespace hot { namespace commons {

class NodeMergeInformation {
	std::array<uint8_t, 256> mKeyWithOnlyBitForLeftSourceInformation;
	std::array<uint8_t, 256> mKeyWithOnlyBitForRightSourceInformation;
	bool mHasMergedMask;
	alignas(std::alignment_of<MultiMaskPartialKeyMapping<4>>::value) std::array<uint8_t, sizeof(MultiMaskPartialKeyMapping<4>)> mRawMergedMask;

public:
	/**
	*
	* @tparam SourceDiscriminativeBitsRepresentation1
	* @tparam SourcePartialKeyType1
	* @tparam SourceDiscriminativeBitsRepresentation1
	* @tparam SourcePartialKeyType2
	* @param left
	* @param right
	*/
	template<typename SourceDiscriminativeBitsRepresentation1, typename SourceDiscriminativeBitsRepresentation2> NodeMergeInformation(
		uint16_t rootBit, SourceDiscriminativeBitsRepresentation1 const & left, SourceDiscriminativeBitsRepresentation2 const & right
	) : mKeyWithOnlyBitForLeftSourceInformation(left.createIntermediateKeyWithOnlySignificantBitsSet()),
		mKeyWithOnlyBitForRightSourceInformation(right.createIntermediateKeyWithOnlySignificantBitsSet()),
		mHasMergedMask(false)
	{
		initializeMergedMask(rootBit, mKeyWithOnlyBitForLeftSourceInformation, mKeyWithOnlyBitForRightSourceInformation);
	};

	template<typename Operation> inline auto executeWithMergedDiscriminativeBitsRepresentationAndFittingPartialKeyType(Operation const & operation) const {
		assert(isValid());
		MultiMaskPartialKeyMapping<4> const & temporaryExtractionMask = *reinterpret_cast<MultiMaskPartialKeyMapping<4> const *>(mRawMergedMask.data());

		decltype(operation(temporaryExtractionMask, static_cast<uint32_t>(0u), static_cast<uint32_t>(0u), static_cast<uint32_t>(0u))) result;
		temporaryExtractionMask.executeWithCompressedDiscriminativeBitsRepresentation([&](auto const & extractionInformation) {
			extractionInformation.executeWithCorrectMaskAndDiscriminativeBitsRepresentation([&](auto const & mergedDiscriminativeBitsRepresentation, auto maximumMask) {
				uint32_t leftRecodingMask = mergedDiscriminativeBitsRepresentation.extractMask(mKeyWithOnlyBitForLeftSourceInformation.data());
				uint32_t rightRecodingMask = mergedDiscriminativeBitsRepresentation.extractMask(mKeyWithOnlyBitForRightSourceInformation.data());

				result = operation(mergedDiscriminativeBitsRepresentation, maximumMask, leftRecodingMask, rightRecodingMask);
			});
		});
		return result;
	}

	bool isValid() const {
	  return mHasMergedMask;
  	}

private:
    void initializeMergedMask(uint16_t rootBit, std::array<uint8_t, 256> const & first, std::array<uint8_t, 256> const & second) {
		//AVX-512 ?
		alignas(8) std::array<uint64_t, 4> bytePositions;
		alignas(8) std::array<uint64_t, 4> byteMasks;
		alignas(8) std::array<uint8_t, 256> result;

		std::fill(byteMasks.begin(), byteMasks.end(), 0);
		std::fill(bytePositions.begin(), bytePositions.end(), 0);

		uint8_t* rawBytePosition = reinterpret_cast<uint8_t*>(bytePositions.data());
		uint8_t* rawByteMasks = reinterpret_cast<uint8_t*>(byteMasks.data());

		uint16_t nextBytePositionToUse = 0u;


		__m256i zero = _mm256_setzero_si256();

		for(size_t i=0; i < 256; i+= sizeof(__m256i)) {
			__m256i firstPortion = _mm256_loadu_si256((__m256i const *) (first.data() + i));
			__m256i secondPortion = _mm256_loadu_si256((__m256i const *) (second.data() + i));
			__m256i bothBitsSet = _mm256_or_si256(firstPortion, secondPortion);

			BitMask32 usedByteIndexes(~(_mm256_movemask_epi8(_mm256_cmpeq_epi8(zero, bothBitsSet))));

			_mm256_storeu_si256(reinterpret_cast<__m256i*>(result.data() + i),  _mm256_or_si256(firstPortion, secondPortion));
			//iterate over potentially set bytes
			for(uint32_t relativeUsedBytesIndex : usedByteIndexes) {
				size_t absoluteUsedBytesIndex = i + relativeUsedBytesIndex;
				uint8_t currentByteMask = result[absoluteUsedBytesIndex];
				//handle first byte
				if(nextBytePositionToUse == 0) {
					unsigned int rootByteIndex = getByteIndex(rootBit);
					uint8_t rootByteMask = DiscriminativeBit(rootBit, 1).getExtractionByte();
					if(rootByteIndex < absoluteUsedBytesIndex) {
						rawBytePosition[0] = rootByteIndex;
						rawByteMasks[0] = rootByteMask;
						nextBytePositionToUse = 1;
					} else {
						currentByteMask |= rootByteMask;
					}
				}

				//handle overflow
				if(nextBytePositionToUse == 32) {
					return; //no optional value
				}
				rawBytePosition[nextBytePositionToUse] = absoluteUsedBytesIndex;
				rawByteMasks[nextBytePositionToUse] = currentByteMask;
				++nextBytePositionToUse;
			}
		}

		uint16_t numberDiscriminativeBits = popcount(byteMasks);
		if(numberDiscriminativeBits <= 32) {
			mHasMergedMask = true;
			new (mRawMergedMask.data()) MultiMaskPartialKeyMapping<4>(
				nextBytePositionToUse, numberDiscriminativeBits, bytePositions, byteMasks
			);
		} else {
			assert(false);
			std::cout << "HERE MAY BE AN ERROR " << std::endl;
		}
    }

    static size_t popcount(std::array<uint64_t, 4> const & rawByteMasks) {
		return _mm_popcnt_u64(rawByteMasks[0])
			+ _mm_popcnt_u64(rawByteMasks[1])
			+ _mm_popcnt_u64(rawByteMasks[2])
			+ _mm_popcnt_u64(rawByteMasks[3]);
    }

};

}}

#endif
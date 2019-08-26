#ifndef __HOT__COMMONS__PARTIAL_KEY_MAPPING_BASE_HPP___
#define __HOT__COMMONS__PARTIAL_KEY_MAPPING_BASE_HPP___

#include "hot/commons/DiscriminativeBit.hpp"

namespace hot { namespace commons {

/**
 * A Base class for all partial key mapping informations
 * A Partial key mapping must be able to extract a set of discriminative bits and form partial keys consisting only of those bits
 *
 */
class PartialKeyMappingBase {
public:
	uint16_t mMostSignificantDiscriminativeBitIndex;
	uint16_t mLeastSignificantDiscriminativeBitIndex;

protected:
	//This does not initialize the fields and is only allowed to be called from subclasses
	//This can be used if both fields are copied together with another field using 64bit operations or better simd or avx instructions
	PartialKeyMappingBase() {
	}

protected:
	PartialKeyMappingBase(uint16_t mostSignificantBitIndex, uint16_t leastSignificantBitIndex) : mMostSignificantDiscriminativeBitIndex(mostSignificantBitIndex), mLeastSignificantDiscriminativeBitIndex(leastSignificantBitIndex) {
	}

	PartialKeyMappingBase(PartialKeyMappingBase const existing, DiscriminativeBit const & significantKeyInformation)
		: PartialKeyMappingBase(
			std::min(existing.mMostSignificantDiscriminativeBitIndex, significantKeyInformation.mAbsoluteBitIndex),
		  	std::max(existing.mLeastSignificantDiscriminativeBitIndex, significantKeyInformation.mAbsoluteBitIndex)
		)
	{
	}
};

} }

#endif

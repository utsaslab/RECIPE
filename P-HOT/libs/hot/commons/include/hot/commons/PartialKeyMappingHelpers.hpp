#ifndef __HOT__COMMONS__PARTIAL_KEY_MAPPING_HELPERS__
#define __HOT__COMMONS__PARTIAL_KEY_MAPPING_HELPERS__

#include "hot/commons/DiscriminativeBit.hpp"

namespace hot { namespace commons {


template<typename DiscriminativeBitsRepresentation, typename PartialKeyType, typename Operation> inline auto extractAndExecuteWithCorrectMaskAndDiscriminativeBitsRepresentation(DiscriminativeBitsRepresentation const & extractionInformation, PartialKeyType compressionMask, Operation const & operation) {
	return extractionInformation.extract(compressionMask, [&](auto const &newDiscriminativeBitsRepresentation) {
		return newDiscriminativeBitsRepresentation.executeWithCorrectMaskAndDiscriminativeBitsRepresentation(operation);
	});
}

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType, typename Operation> inline auto extractAndAddAndExecuteWithCorrectMaskAndDiscriminativeBitsRepresentation(DiscriminativeBitsRepresentation const & extractionInformation, PartialKeyType compressionMask, DiscriminativeBit const & keyInformation, Operation const & operation) {
	return extractionInformation.extract(compressionMask, [&](auto const &intermediateDiscriminativeBitsRepresentation) {
		return intermediateDiscriminativeBitsRepresentation.insert(keyInformation, [&](auto const &insertedDiscriminativeBitsRepresentation) {
			return insertedDiscriminativeBitsRepresentation.executeWithCorrectMaskAndDiscriminativeBitsRepresentation(operation);
		});
	});
}




}}

#endif
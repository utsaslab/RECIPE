#ifndef __SPIDER__MAPHELPERS__MAP_VALUE_EXTRACTOR__HPP__
#define __SPIDER__MAPHELPERS__MAP_VALUE_EXTRACTOR__HPP__

/** @author robert.binna@uibk.ac.at */

#include <utility>

#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/PairKeyExtractor.hpp>
#include <idx/contenthelpers/PairPointerKeyExtractor.hpp>

#include <idx/contenthelpers/KeyComparator.hpp>

namespace idx { namespace maphelpers {

/**
 * Helper structure for use in conjunction with { @link idx::maphelpers::STLLikeIndex }
 * For the given value type stored in an instance of STLLikeIndex it converts the value to the internal value representation used in the map
 *
 * @tparam ValueType the type of the value used
 * @tparam KeyExtractorType the key extractor used to for the STLLikeIndex
 */
template<typename ValueType, template <typename> typename KeyExtractorType> struct MapValueExtractor {
	ValueType operator()(ValueType const &value) {
		return value;
	}
};

/**
 * A template spezialization which allows the handling of pair like types in conjunction with STLLikeIndex.
 * It returns the pairs second entry as the value. Together with the stored key this can later be used to reconstruct the stored pair
 *
 * @tparam ValueType the type of the value used (must be pair like)
 */
template<typename ValueType> struct MapValueExtractor<ValueType, idx::contenthelpers::PairKeyExtractor> {
	auto operator()(ValueType const &value) {
		return value.second;
	}
};

}}

#endif
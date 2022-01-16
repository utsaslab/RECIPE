#ifndef __IDX__CONTENTHELPERS__PAIR_POINTER_KEY_EXTRACTOR__
#define __IDX__CONTENTHELPERS__PAIR_POINTER_KEY_EXTRACTOR__

/** @author robert.binna@uibk.ac.at */

namespace idx { namespace contenthelpers {

/**
 * Key Extractor for a pointer to a pair like type.
 * A pair like
 *
 * @tparam PointerPairLikeType
 */
template<typename PointerPairLikeType> struct PairPointerKeyExtractor {
	using KeyType = decltype(std::declval<PointerPairLikeType>()->first);

	inline KeyType operator()(PointerPairLikeType const & value) const {
		return value->first;
	}
};

} }

#endif
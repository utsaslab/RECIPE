#ifndef __IDX__CONTENTHELPERS__PAIR_KEY_EXTRACTOR__
#define __IDX__CONTENTHELPERS__PAIR_KEY_EXTRACTOR__

/** @author robert.binna@uibk.ac.at */

namespace idx { namespace contenthelpers {

/**
 * For pair like types the PairKeyExtractor returns the key part of the pair
 *
 * @tparam PairLikeType a pair like type having at least 'first' and 'second' field with the 'first' field pointing to the key part and the 'second' field pointing to the value part
 */
template<typename PairLikeType>
struct PairKeyExtractor {
	using KeyType = decltype(std::declval<PairLikeType>().first);

	inline KeyType operator()(PairLikeType const &value) const {
		return value.first;
	}
};

} }

#endif
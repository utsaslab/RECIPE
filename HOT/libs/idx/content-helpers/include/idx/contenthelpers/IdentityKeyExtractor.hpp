#ifndef __IDX__CONTENTHELPERS__IDENTITY_KEY_EXTRACTOR__
#define __IDX__CONTENTHELPERS__IDENTITY_KEY_EXTRACTOR__

/** @author robert.binna@uibk.ac.at */

namespace idx { namespace contenthelpers {

/**
 * A trivial key Extractor which returns the value itself as the extracted key
 *
 * @tparam ValueType
 */
template<typename ValueType>
struct IdentityKeyExtractor {
	typedef ValueType KeyType;

	inline KeyType operator()(ValueType const &value) const {
		return value;
	}
};

} }

#endif
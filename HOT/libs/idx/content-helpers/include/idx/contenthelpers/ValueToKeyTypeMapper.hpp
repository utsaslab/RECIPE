#ifndef __IDX__CONTENTHELPERS__VALUE_TO_KEY_TYPE_MAPPER__HPP__
#define __IDX__CONTENTHELPERS__VALUE_TO_KEY_TYPE_MAPPER__HPP__

/** @author robert.binna@uibk.ac.at */

#include <utility>

namespace idx { namespace contenthelpers {

/**
 * Helper Template, given a value type and key extractor type it is able to determine the corresponding key type.
 *
 * @tparam ValueType the type of the value
 * @tparam KeyExtractor the key extractor to consider
 */
template<typename ValueType, template <typename> typename KeyExtractor> struct ValueToKeyTypeMapper {
	using KeyType = decltype(std::declval<KeyExtractor<ValueType>>()(std::declval<ValueType>()));
};

}}

#endif

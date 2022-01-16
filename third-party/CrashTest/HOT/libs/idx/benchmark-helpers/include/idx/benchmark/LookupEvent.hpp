#ifndef __IDX__BENCHMARK__LOOKUP_EVENT__HPP__
#define __IDX__BENCHMARK__LOOKUP_EVENT__HPP__

#include <idx/contenthelpers/ValueToKeyTypeMapper.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

namespace idx { namespace benchmark {

template<typename ValueType, template <typename> typename KeyExtractor> struct LookupEvent {
	using KeyType = typename idx::contenthelpers::ValueToKeyTypeMapper<ValueType, KeyExtractor>::KeyType;
	KeyType mKey;
	idx::contenthelpers::OptionalValue<ValueType> mExpectedValue;

};

} }


#endif
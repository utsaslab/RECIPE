#ifndef __IDX__BENCHMARK__SCAN_EVENT__HPP__
#define __IDX__BENCHMARK__SCAN_EVENT__HPP__

#include <idx/contenthelpers/ValueToKeyTypeMapper.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

namespace idx { namespace benchmark {

template<typename ValueType, template <typename> typename KeyExtractor> struct ScanEvent {
	using KeyType = typename idx::contenthelpers::ValueToKeyTypeMapper<ValueType, KeyExtractor>::KeyType ;
	KeyType mLookupKey;
	size_t mNumberValuesToScan;
	idx::contenthelpers::OptionalValue<ValueType> mLastResult;
};

} }

#endif
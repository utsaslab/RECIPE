#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT_TYPE_ID_TO_EVENT_TYPE_MAPPING__HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT_TYPE_ID_TO_EVENT_TYPE_MAPPING__HPP__

#include "idx/benchmark/BenchmarkEventTypeId.hpp"
#include "idx/benchmark/InsertEvent.hpp"
#include "idx/benchmark/LookupEvent.hpp"
#include "idx/benchmark/ScanEvent.hpp"
#include "idx/benchmark/UpdateEvent.hpp"

namespace idx { namespace benchmark {

template<typename ValueType, template <typename> typename KeyExtractor, BenchmarkEventTypeId eventTypeId> struct BenchmarkEventTypeIdToEventType {
};

template<typename ValueType, template <typename> typename KeyExtractor> struct BenchmarkEventTypeIdToEventType<ValueType, KeyExtractor, BenchmarkEventTypeId::UpdateEventTypeId> {
	using EventType = UpdateEvent<ValueType, KeyExtractor>;
};

template<typename ValueType, template <typename> typename KeyExtractor> struct BenchmarkEventTypeIdToEventType<ValueType, KeyExtractor, BenchmarkEventTypeId::LookupEventTypeId> {
	using EventType = LookupEvent<ValueType, KeyExtractor>;
};

template<typename ValueType, template <typename> typename KeyExtractor> struct BenchmarkEventTypeIdToEventType<ValueType, KeyExtractor, BenchmarkEventTypeId::InsertEventTypeId> {
	using EventType = InsertEvent<ValueType, KeyExtractor>;
};

template<typename ValueType, template <typename> typename KeyExtractor> struct BenchmarkEventTypeIdToEventType<ValueType, KeyExtractor, BenchmarkEventTypeId::ScanEventTypeId> {
	using EventType = ScanEvent<ValueType, KeyExtractor>;
};

}}

#endif
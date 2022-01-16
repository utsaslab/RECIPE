#ifndef __IDX__BENCHMARK__BENCHMARK_BASE_EVENT__HPP__
#define __IDX__BENCHMARK__BENCHMARK_BASE_EVENT__HPP__

#include <idx/contenthelpers/ValueToKeyTypeMapper.hpp>

#include "idx/benchmark/BenchmarkEventTypeId.hpp"
#include "idx/benchmark/BenchmarkEventTypeIdToEventTypeMapping.hpp"

namespace idx { namespace benchmark {

struct BenchmarkBaseEvent {
	BenchmarkEventTypeId const mEventTypeId;
	BenchmarkBaseEvent* mNextEvent;

	//disable copy of the base type as it can cause unexpected troubles in combination with casts (base type is copied but super type is not considered)
	BenchmarkBaseEvent(BenchmarkBaseEvent const & other ) = delete;

	BenchmarkBaseEvent(BenchmarkEventTypeId eventTypeId) : mEventTypeId(eventTypeId), mNextEvent(nullptr) {
	}
};

}}

#endif
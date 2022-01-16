#ifndef __IDX__BENCHMARK__EXECUTE_FOR_SPECIFIC_EVENT__HPP__
#define __IDX__BENCHMARK__EXECUTE_FOR_SPECIFIC_EVENT__HPP__

#include "idx/benchmark/BenchmarkBaseEvent.hpp"
#include "idx/benchmark/BenchmarkEvent.hpp"
#include "idx/benchmark/BenchmarkEventTypeId.hpp"
#include "idx/benchmark/BenchmarkEventTypeIdToEventTypeMapping.hpp"

namespace idx { namespace benchmark {

template<typename ValueType, template<typename> typename KeyExtractor>
struct SpecificEventProcessor {

	template<typename Operation>
	auto operator()(BenchmarkBaseEvent *baseEvent, Operation operation) {
		switch (baseEvent->mEventTypeId) {
			case BenchmarkEventTypeId::InsertEventTypeId:
				return operation(getSpecificEvent<BenchmarkEventTypeId::InsertEventTypeId>(baseEvent));
			case BenchmarkEventTypeId::LookupEventTypeId:
				return operation(getSpecificEvent<BenchmarkEventTypeId::LookupEventTypeId>(baseEvent));
			case BenchmarkEventTypeId::ScanEventTypeId:
				return operation(getSpecificEvent<BenchmarkEventTypeId::ScanEventTypeId>(baseEvent));
			default: //BenchmarkEventTypeId::UpdateEventTypeId:
				return operation(getSpecificEvent<BenchmarkEventTypeId::UpdateEventTypeId>(baseEvent));
		}
	}

private:
	template<BenchmarkEventTypeId eventTypeId>
	typename BenchmarkEvent<eventTypeId, ValueType, KeyExtractor>::EventType &
	getSpecificEvent(BenchmarkBaseEvent *baseEvent) {
		return reinterpret_cast<BenchmarkEvent<eventTypeId, ValueType, KeyExtractor> *>(baseEvent)->getData();
	}
};

} }

#endif
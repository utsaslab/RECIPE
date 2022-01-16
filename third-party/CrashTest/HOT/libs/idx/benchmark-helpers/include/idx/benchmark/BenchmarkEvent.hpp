#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT__HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT__HPP__

#include "idx/benchmark/BenchmarkBaseEvent.hpp"
#include "idx/benchmark/BenchmarkEventTypeId.hpp"
#include "idx/benchmark/BenchmarkEventTypeIdToEventTypeMapping.hpp"

namespace idx { namespace benchmark {

template<BenchmarkEventTypeId eventTypeId, typename ValueType, template <typename> typename KeyExtractor> class BenchmarkEvent : public BenchmarkBaseEvent {
public:
	using EventType = typename BenchmarkEventTypeIdToEventType<ValueType, KeyExtractor, eventTypeId>::EventType;
private:
	EventType data;

public:
	template<typename... Args> BenchmarkEvent(Args... args) : BenchmarkBaseEvent(eventTypeId), data(args...) {
	}

	EventType & getData() {
		return data;
	}
};


/*

template<typename EventTypeId> struct BenchmarkEvent {
	typename EventTypeIdToEventType::EventType mEventData;
	EventType mNextEventType;
};

struct BenchmarkEventProcessor(eventInputData) {
	EventTypeId firstEventTypeId;
	void* mRawEventMemory

	totalBenchmarkEventsSize = estimateSize(eventInputData);
	allocate();

	read(eventInputData) {

	}
}

BenchmarkEventIterator {
	void* currentEventMemory;
	EventTypeId currentEventType;
	size_t remainingEvents;

	void run() {
		while(remainingEvents > 0) {
			currentEventMemory += processCurrentEvent();
		}
	}
};*/

}}

#endif
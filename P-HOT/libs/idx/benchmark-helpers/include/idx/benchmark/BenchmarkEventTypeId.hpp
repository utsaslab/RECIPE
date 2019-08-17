#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT_TYPE_ID__HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT_TYPE_ID__HPP__

namespace idx { namespace benchmark {

enum BenchmarkEventTypeId {
	UpdateEventTypeId = 0,
	LookupEventTypeId = 1,
	ScanEventTypeId = 2,
	InsertEventTypeId = 3
};

}}

#endif
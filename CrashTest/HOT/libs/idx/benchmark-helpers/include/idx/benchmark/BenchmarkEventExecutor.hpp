#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT_EXECUTOR__HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT_EXECUTOR__HPP__

#include <memory>

#include "idx/benchmark/BenchmarkBaseEvent.hpp"

#include "idx/benchmark/SpecificEventProcessor.hpp"

#include "idx/benchmark/InsertEvent.hpp"
#include "idx/benchmark/LookupEvent.hpp"
#include "idx/benchmark/ScanEvent.hpp"
#include "idx/benchmark/UpdateEvent.hpp"

namespace idx { namespace benchmark {

template<typename IndexStructure, typename ValueType, template <typename> typename KeyExtractor> class BenchmarkEventExecutor {

	std::shared_ptr<IndexStructure> mIndex;

public:
	using SpecificInsertEvent = InsertEvent<ValueType, KeyExtractor>;
	using SpecificLookupEvent = LookupEvent<ValueType, KeyExtractor>;
	using SpecificScanEvent = ScanEvent<ValueType, KeyExtractor>;
	using SpecificUpdateEvent = UpdateEvent<ValueType, KeyExtractor>;

	BenchmarkEventExecutor(std::shared_ptr<IndexStructure> const & index) : mIndex(index) {
	}

	inline bool operator()(SpecificInsertEvent const & insertEvent) {
		return mIndex->insert(insertEvent.mValueToInsert);
	}

	inline bool operator()(SpecificLookupEvent const & lookupEvent) {
		return mIndex->lookup(lookupEvent.mKey).compliesWith(lookupEvent.mExpectedValue);
	}

	inline bool operator()(SpecificScanEvent const & scanEvent) {
		return mIndex->scan(scanEvent.mLookupKey, scanEvent.mNumberValuesToScan).compliesWith(scanEvent.mLastResult);
	}

	inline bool operator()(SpecificUpdateEvent const & updateEvent) {
		return mIndex->upsert(updateEvent.mNewValue).compliesWith(updateEvent.mPreviousValue);
	}

};

}}

#endif
#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT_LIST_BACKING_BUFFER_SIZE_ESTIMATOR_HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT_LIST_BACKING_BUFFER_SIZE_ESTIMATOR_HPP__

#include <array>

#include "idx/benchmark/ContinuosBuffer.hpp"
#include "idx/benchmark/BenchmarkBaseEvent.hpp"

namespace idx { namespace benchmark {

class BenchmarkEventListBackingBuferSizeEstimator {
	std::array<max_align_t, 4096/sizeof(max_align_t)> backingBuffer;
	size_t totalBackingBufferSizeInBytes = (4096/sizeof(max_align_t)) * sizeof(max_align_t);
	ContinuousBuffer mCountBuffer;
	size_t mTotalBufferSize;

	BenchmarkBaseEvent* head;
	BenchmarkBaseEvent** endPointer;

public:
	BenchmarkEventListBackingBuferSizeEstimator()
		: mCountBuffer(backingBuffer.data(), backingBuffer.size() * sizeof(typename decltype(backingBuffer)::value_type)), mTotalBufferSize(0ul)
	{
	}

	template<typename EventConstructor, typename... Args> void addEvent(EventConstructor const & constructEvent, Args... args) {
		const std::pair<BenchmarkBaseEvent *, ContinuousBuffer> & event = constructEvent(args..., mCountBuffer);
		size_t eventSize = (mCountBuffer.mRemainingBufferSize - event.second.mRemainingBufferSize);
		mTotalBufferSize += eventSize;
		uintptr_t relativeAddress = (reinterpret_cast<char*>(event.second.mRemainingBuffer) - reinterpret_cast<char*>(backingBuffer.data()));
		size_t alignmentOffset = relativeAddress % sizeof(max_align_t);
		mCountBuffer = { reinterpret_cast<char*>(backingBuffer.data()) + alignmentOffset, totalBackingBufferSizeInBytes - alignmentOffset };
	}

	size_t getTotalBufferSize() {
		return mTotalBufferSize;
	}

};

}}

#endif
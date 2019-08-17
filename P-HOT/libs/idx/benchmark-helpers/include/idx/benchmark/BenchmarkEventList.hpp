#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT_LIST__HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT_LIST__HPP__

#include <cstddef>

#include "idx/benchmark/ContinuosBuffer.hpp"
#include "idx/benchmark/BenchmarkBaseEvent.hpp"

namespace idx { namespace benchmark {

class BenchmarkEventList {
	static constexpr size_t MAX_ALIGNMENT_IN_BYTES = sizeof(std::max_align_t);
	size_t mInitialBackingBufferSize;
	size_t mNumberEvents;
	std::unique_ptr<std::max_align_t[]> mBackingBuffer;
	ContinuousBuffer mRemainingBuffer;
	BenchmarkBaseEvent* mHead;
	BenchmarkBaseEvent** mEndPointer;

public:
	BenchmarkEventList(size_t backingBufferSize)
		: mInitialBackingBufferSize(backingBufferSize),
		  mNumberEvents(0ul),
		  mBackingBuffer(new std::max_align_t[getNumberMaxAlignTSizeBlocks(backingBufferSize)]),
		  mRemainingBuffer(mBackingBuffer.get(), backingBufferSize),
		  mHead(nullptr),
		  mEndPointer(&mHead)
	{
	}

	BenchmarkEventList() : mInitialBackingBufferSize(0ul), mBackingBuffer(nullptr), mRemainingBuffer(nullptr, 0ul), mHead(nullptr), mEndPointer(&mHead) {
	}


	BenchmarkEventList(BenchmarkEventList && other) = default;
	BenchmarkEventList & operator=(BenchmarkEventList && other) = default;

	BenchmarkEventList(BenchmarkEventList const & other) = delete;
	BenchmarkEventList & operator=(BenchmarkEventList const & other) = delete;


	size_t getBackingBufferBytesUsed() {
		return mInitialBackingBufferSize - mRemainingBuffer.mRemainingBufferSize;
	}

	size_t getRemainingBackingBytes() {
		return mRemainingBuffer.mRemainingBufferSize;
	}

	static size_t constexpr getNumberMaxAlignTSizeBlocks(size_t backingBufferSize) {
		return (backingBufferSize/MAX_ALIGNMENT_IN_BYTES) + std::min<size_t>(1, backingBufferSize%MAX_ALIGNMENT_IN_BYTES);
	}


	template<typename EventConstructor, typename... Args> BenchmarkBaseEvent* addEvent(EventConstructor constructor, Args... args) {
		const std::pair<BenchmarkBaseEvent *, ContinuousBuffer> & event = constructor(args..., mRemainingBuffer);
		*mEndPointer = event.first;
		mEndPointer = &event.first->mNextEvent;
		mRemainingBuffer = event.second;

		++mNumberEvents;

		return event.first;
	}


	class BenchmarkEventListIterator {
		friend class BenchmarkEventList;
	private:
		BenchmarkBaseEvent* mCurrent;

		BenchmarkEventListIterator() : BenchmarkEventListIterator(nullptr) {
		}

		BenchmarkEventListIterator(BenchmarkBaseEvent* current) : mCurrent(current) {
		}

	public:
		BenchmarkBaseEvent & operator*() {
			return *mCurrent;
		}

		BenchmarkBaseEvent* operator->() {
			return mCurrent;
		}

		BenchmarkEventListIterator & operator++() {
			if(mCurrent != nullptr) {
				mCurrent = mCurrent->mNextEvent;
			}
			return *this;
		}

		bool operator==(BenchmarkEventListIterator const & other) const {
			return mCurrent == other.mCurrent;
		}

		bool operator!=(BenchmarkEventListIterator const & other) const {
			return mCurrent != other.mCurrent;
		}
	};

	BenchmarkEventListIterator begin() const {
		return BenchmarkEventListIterator(mHead);
	}

	BenchmarkEventListIterator end() const {
		return BenchmarkEventListIterator();
	}

	size_t size() const {
		return mNumberEvents;
	}
};

}}

#endif
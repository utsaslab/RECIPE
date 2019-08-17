#ifndef __HOT__ROWEX__THREAD_SPECIFIC_MEMORY_INFORMATION__
#define __HOT__ROWEX__THREAD_SPECIFIC_MEMORY_INFORMATION__

#include <atomic>
#include <vector>

#include "hot/rowex/HOTRowexChildPointer.hpp"

namespace hot { namespace rowex {

constexpr uint32_t NUMBER_EPOCHS = 3;
constexpr uint32_t NOT_IN_EPOCH = NUMBER_EPOCHS;

class ThreadSpecificEpochBasedReclamationInformation {
	std::array<std::vector<HOTRowexChildPointer>, NUMBER_EPOCHS> mFreeLists;
	std::atomic<uint32_t> mLocalEpoch;
	uint32_t mPreviouslyAccessedEpoch;
	bool mThreadWantsToAdvance;

public:
	static std::atomic<size_t> mNumberFrees;

	ThreadSpecificEpochBasedReclamationInformation() : mFreeLists(), mLocalEpoch(NOT_IN_EPOCH), mPreviouslyAccessedEpoch(NOT_IN_EPOCH), mThreadWantsToAdvance(false)  {
	}

	ThreadSpecificEpochBasedReclamationInformation(ThreadSpecificEpochBasedReclamationInformation const & other) = delete;
	ThreadSpecificEpochBasedReclamationInformation(ThreadSpecificEpochBasedReclamationInformation && other) =  delete;

	~ThreadSpecificEpochBasedReclamationInformation() {
		for(uint32_t i = 0; i < 3; ++i) {
			freeForEpoch(i);
		}
	}

	void scheduleForDeletion(HOTRowexChildPointer const & childPointer) {
		assert(mLocalEpoch != NOT_IN_EPOCH);
		std::vector<HOTRowexChildPointer> & currentFreeList = mFreeLists[mLocalEpoch];
		currentFreeList.emplace_back(childPointer);
		mThreadWantsToAdvance = (currentFreeList.size() % 64u) == 0;
	}

	uint32_t getLocalEpoch() const {
		return mLocalEpoch.load(std::memory_order_acquire);
	}

	void enter(uint32_t newEpoch) {
		assert(mLocalEpoch == NOT_IN_EPOCH);
		if(mPreviouslyAccessedEpoch != newEpoch) {
			freeForEpoch(newEpoch);
			mThreadWantsToAdvance = false;
			mPreviouslyAccessedEpoch = newEpoch;
		}
		mLocalEpoch.store(newEpoch, std::memory_order_release);
	}

	void leave()  {
		mLocalEpoch.store(NOT_IN_EPOCH, std::memory_order_release);
	}

	bool doesThreadWantToAdvanceEpoch() {
		return (mThreadWantsToAdvance);
	}

private:
	void freeForEpoch(uint32_t epoch) {
		std::vector<HOTRowexChildPointer> & previousFreeList = mFreeLists[epoch];
		for(HOTRowexChildPointer const & pointer : previousFreeList) {
			pointer.free();
			//mNumberFrees.fetch_add(1);
		}
		previousFreeList.resize(0u);
	}
};

std::atomic<size_t> ThreadSpecificEpochBasedReclamationInformation::mNumberFrees { 0 };

}}

#endif

#ifndef __HOT_EPOCH_BASED_MEMORY_RECLAMATION_STRATEGY__
#define __HOT_EPOCH_BASED_MEMORY_RECLAMATION_STRATEGY__

#include <atomic>
#include <algorithm>
#include <array>
#include <thread>

#include <tbb/enumerable_thread_specific.h>

#include "hot/rowex/ThreadSpecificEpochBasedReclamationInformation.hpp"


namespace hot { namespace rowex {

class EpochBasedMemoryReclamationStrategy {
	static uint32_t NEXT_EPOCH[3];
	static uint32_t PREVIOUS_EPOCH[3];

	std::atomic<uint32_t> mCurrentEpoch;
	tbb::enumerable_thread_specific<ThreadSpecificEpochBasedReclamationInformation, tbb::cache_aligned_allocator<ThreadSpecificEpochBasedReclamationInformation>, tbb::ets_key_per_instance> mThreadSpecificInformations;

private:
	EpochBasedMemoryReclamationStrategy() : mCurrentEpoch(0), mThreadSpecificInformations() {
	}

public:

	static EpochBasedMemoryReclamationStrategy* getInstance() {
		static EpochBasedMemoryReclamationStrategy instance;
		return &instance;
	}

	void enterCriticalSection() {
		ThreadSpecificEpochBasedReclamationInformation & currentMemoryInformation = mThreadSpecificInformations.local();
		uint32_t currentEpoch = mCurrentEpoch.load(std::memory_order_acquire);
		currentMemoryInformation.enter(currentEpoch);
		if(currentMemoryInformation.doesThreadWantToAdvanceEpoch() && canAdvance(currentEpoch)) {
			mCurrentEpoch.compare_exchange_strong(currentEpoch, NEXT_EPOCH[currentEpoch]);
		}
	}

	bool canAdvance(uint32_t currentEpoch) {
		uint32_t previousEpoch = PREVIOUS_EPOCH[currentEpoch];
		return !std::any_of(mThreadSpecificInformations.begin(), mThreadSpecificInformations.end(), [previousEpoch](ThreadSpecificEpochBasedReclamationInformation const & threadInformation) {
			return (threadInformation.getLocalEpoch() == previousEpoch);
		});
	}

	void leaveCriticialSection() {
		ThreadSpecificEpochBasedReclamationInformation & currentMemoryInformation = mThreadSpecificInformations.local();
		currentMemoryInformation.leave();
	}

	void scheduleForDeletion(HOTRowexChildPointer const & childPointer) {
		mThreadSpecificInformations.local().scheduleForDeletion(childPointer);
	}
};

uint32_t EpochBasedMemoryReclamationStrategy::NEXT_EPOCH[3] = { 1, 2, 0 };
uint32_t EpochBasedMemoryReclamationStrategy::PREVIOUS_EPOCH[3] = { 2, 0, 1 };

} }



#endif
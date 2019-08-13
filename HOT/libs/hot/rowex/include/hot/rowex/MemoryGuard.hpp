#ifndef __HOT__ROWEX__MEMORY_GUARD__
#define __HOT__ROWEX__MEMORY_GUARD__

#include "hot/rowex/EpochBasedMemoryReclamationStrategy.hpp"

namespace hot { namespace rowex {

class MemoryGuard {
	EpochBasedMemoryReclamationStrategy* mMemoryReclamation;

public:
	MemoryGuard(EpochBasedMemoryReclamationStrategy* memoryReclamation) : mMemoryReclamation(memoryReclamation) {
		mMemoryReclamation->enterCriticalSection();
	}

	~MemoryGuard() {
		mMemoryReclamation->leaveCriticialSection();
	}

	MemoryGuard(MemoryGuard const & other) = delete;
	MemoryGuard &operator=(MemoryGuard const & other) = delete;
};

}}

#endif // __HOT_MEMORY_GUARD__
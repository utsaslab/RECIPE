#ifndef __HOT__ROWEX__ITERATE_STACK_STATE__
#define __HOT__ROWEX__ITERATE_STACK_STATE__

#include "hot/rowex/HOTRowexIteratorBufferStates.hpp"
#include "hot/rowex/HOTRowexIteratorEndToken.hpp"

#include <cstdint>

namespace hot { namespace rowex {

template<typename StackEntryType> struct HOTRowexIteratorStackState {
	StackEntryType *mRootEntry;
	StackEntryType *mStackEntry;
	int32_t mBufferState;

	HOTRowexIteratorStackState(StackEntryType *rootEntry, int32_t bufferState, StackEntryType *stackEntry)
		: mRootEntry(rootEntry), mStackEntry(stackEntry), mBufferState(bufferState) {
	}

	HOTRowexIteratorStackState(StackEntryType *rootEntry) : mRootEntry(rootEntry), mStackEntry(rootEntry),
													mBufferState(ITERATOR_FILL_BUFFER_STATE_END) {
		mStackEntry->init(&HOTRowexIteratorEndToken::END_TOKEN, &HOTRowexIteratorEndToken::END_TOKEN + 1);
	}

	HOTRowexIteratorStackState(HOTRowexIteratorStackState const &mOther) = default;

	void end() {
		mStackEntry->init(&HOTRowexIteratorEndToken::END_TOKEN, &HOTRowexIteratorEndToken::END_TOKEN + 1);
		mBufferState = ITERATOR_FILL_BUFFER_STATE_END;
	}
};

}}

#endif
#ifndef __HOT__ROWEX__ITERATOR_BUFFER_STATE__
#define __HOT__ROWEX__ITERATOR_BUFFER_STATE__

#include <cstdlib>
#include <idx/contenthelpers/KeyUtilities.hpp>

#include "hot/rowex/HOTRowexIteratorEndToken.hpp"
#include "hot/rowex/HOTRowexChildPointer.hpp"

namespace hot { namespace rowex {

template<typename KeyType> struct HotRowexIteratorBufferState {
	static constexpr size_t MAXIMUM_NUMBER_NUMBER_ENTRIES_IN_BUFFER = 128u;

	using FixedSizedKeyType = decltype(idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(KeyType())));

	HOTRowexChildPointer * mBuffer;

	size_t mCurrentBufferIndex;
	size_t mCurrentBufferSize;

	FixedSizedKeyType mLastAccessedKey;

	HotRowexIteratorBufferState(HOTRowexChildPointer * const & buffer) : mBuffer(buffer), mCurrentBufferIndex(0u), mCurrentBufferSize(0), mLastAccessedKey() {
		buffer[0] = HOTRowexIteratorEndToken::END_TOKEN;
	}

	HotRowexIteratorBufferState(HotRowexIteratorBufferState<KeyType> const & other) = default;
	HotRowexIteratorBufferState<KeyType> & operator=(HotRowexIteratorBufferState<KeyType> const & other) = default;

	void copy(HotRowexIteratorBufferState const & other) {
		mBuffer[0] = HOTRowexIteratorEndToken::END_TOKEN;
		mCurrentBufferIndex = other.mCurrentBufferIndex;
		mCurrentBufferSize = other.mCurrentBufferSize;
		mLastAccessedKey = other.mLastAccessedKey;
		std::memmove(mBuffer, other.mBuffer, sizeof(HOTRowexChildPointer) * other.mCurrentBufferSize);
	}

	bool isFull() {
		return mCurrentBufferSize >= MAXIMUM_NUMBER_NUMBER_ENTRIES_IN_BUFFER;
	}

	bool endOfDataReached() {
		return mCurrentBufferSize < MAXIMUM_NUMBER_NUMBER_ENTRIES_IN_BUFFER;
	}

	bool isEmpty() {
		return mCurrentBufferSize == 0;
	}

	HOTRowexChildPointer const & getCurrent() const {
		return mBuffer[mCurrentBufferIndex];
	}

	void advance() {
		++mCurrentBufferIndex;
	}

	bool canAdvance() {
		return (mCurrentBufferIndex + 1) < mCurrentBufferSize;
	}

	void push_back(HOTRowexChildPointer const & entryToStore) {
		mBuffer[mCurrentBufferSize++] = entryToStore;
	}

	void setLastAccessedKey(KeyType const & lastAccessedKey) {
		mLastAccessedKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(lastAccessedKey));
	}

	const uint8_t* getLastAccessedKey() {
		return idx::contenthelpers::interpretAsByteArray(mLastAccessedKey);
	}
};

}}

#endif
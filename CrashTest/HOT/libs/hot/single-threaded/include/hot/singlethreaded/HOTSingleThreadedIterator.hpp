#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_ITERATOR__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_ITERATOR__

#include <cstdint>
#include <array>

#include "hot/singlethreaded/HOTSingleThreadedChildPointer.hpp"
#include "hot/singlethreaded/HOTSingleThreadedNodeBase.hpp"
#include "idx/contenthelpers/TidConverters.hpp"

namespace hot { namespace singlethreaded {

class HOTSingleThreadedIteratorStackEntry {
	HOTSingleThreadedChildPointer const * mCurrent;
	HOTSingleThreadedChildPointer const * mEnd;

public:
	//leaf uninitialized
	HOTSingleThreadedIteratorStackEntry() {
	}

	HOTSingleThreadedChildPointer const * init(HOTSingleThreadedChildPointer const * current, HOTSingleThreadedChildPointer const * end) {
		mCurrent = current;
		mEnd = end;

		return mCurrent;
	}

	HOTSingleThreadedChildPointer const * getCurrent() const {
		return mCurrent;
	}

	bool isExhausted() {
		return mCurrent == mEnd;
	}

	void advance() {
		if(mCurrent != mEnd) {
			++mCurrent;
		}
	}
};

template<typename ValueType, template <typename> typename KeyExtractor> class HOTSingleThreaded; //Forward Declaration of SIMDCobTrie for usage as friend class

template<typename ValueType> class HOTSingleThreadedIterator {
	template<typename ValueType2, template <typename> typename KeyExtractor> friend class hot::singlethreaded::HOTSingleThreaded;

	static HOTSingleThreadedChildPointer END_TOKEN;

	alignas (std::alignment_of<HOTSingleThreadedIteratorStackEntry>()) char mRawNodeStack[sizeof(HOTSingleThreadedIteratorStackEntry) * 64];
	HOTSingleThreadedIteratorStackEntry* mNodeStack;
	size_t mCurrentDepth = 0;

public:
	HOTSingleThreadedIterator(HOTSingleThreadedChildPointer const * mSubTreeRoot) : HOTSingleThreadedIterator(mSubTreeRoot, mSubTreeRoot + 1) {
		descend();
	}

	HOTSingleThreadedIterator(HOTSingleThreadedIterator const & other) : mNodeStack(reinterpret_cast<HOTSingleThreadedIteratorStackEntry*>(mRawNodeStack)) {
		std::memcpy(this->mRawNodeStack, other.mRawNodeStack, sizeof(HOTSingleThreadedIteratorStackEntry) * (other.mCurrentDepth + 1));
		mCurrentDepth = other.mCurrentDepth;
	}

	HOTSingleThreadedIterator() : mNodeStack(reinterpret_cast<HOTSingleThreadedIteratorStackEntry*>(mRawNodeStack)) {
		mNodeStack[0].init(&END_TOKEN, &END_TOKEN);
	}

public:
	ValueType operator*() const {
		return idx::contenthelpers::tidToValue<ValueType>(mNodeStack[mCurrentDepth].getCurrent()->getTid());
	}

	HOTSingleThreadedIterator<ValueType> & operator++() {
		mNodeStack[mCurrentDepth].advance();
		while((mCurrentDepth > 0) & (mNodeStack[mCurrentDepth].isExhausted())) {
			--mCurrentDepth;
			mNodeStack[mCurrentDepth].advance();
		}
		if(mNodeStack[0].isExhausted()) {
			mNodeStack[0].init(&END_TOKEN, &END_TOKEN);
		} else {
			descend();
		}
		return *this;
	}

	bool operator==(HOTSingleThreadedIterator<ValueType> const & other) const {
		return (*mNodeStack[mCurrentDepth].getCurrent()) == (*other.mNodeStack[other.mCurrentDepth].getCurrent());
	}

	bool operator!=(HOTSingleThreadedIterator<ValueType> const & other) const {
		return (*mNodeStack[mCurrentDepth].getCurrent()) != (*other.mNodeStack[other.mCurrentDepth].getCurrent());
	}

private:
	HOTSingleThreadedIterator(HOTSingleThreadedChildPointer const * currentRoot, HOTSingleThreadedChildPointer const* rootEnd) : mNodeStack(reinterpret_cast<HOTSingleThreadedIteratorStackEntry*>(mRawNodeStack)) {
		mNodeStack[0].init(currentRoot, rootEnd);
	}

	void descend() {
		HOTSingleThreadedChildPointer const* currentSubtreeRoot = mNodeStack[mCurrentDepth].getCurrent();
		while(currentSubtreeRoot->isAValidNode()) {
			HOTSingleThreadedNodeBase* currentSubtreeRootNode = currentSubtreeRoot->getNode();
			currentSubtreeRoot = descend(currentSubtreeRootNode->begin(), currentSubtreeRootNode->end());
		}
	}

	HOTSingleThreadedChildPointer const* descend(HOTSingleThreadedChildPointer const* current, HOTSingleThreadedChildPointer const* end) {
		return mNodeStack[++mCurrentDepth].init(current, end);
	}
};

template<typename KeyHelper> HOTSingleThreadedChildPointer HOTSingleThreadedIterator<KeyHelper>::END_TOKEN {};

}}

#endif
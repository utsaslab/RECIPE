#ifndef __HOT__ROWEX__SYNCHRONIZED_ITERATOR_STACK_ENTRY__
#define __HOT__ROWEX__SYNCHRONIZED_ITERATOR_STACK_ENTRY__

namespace hot { namespace rowex {

class HOTRowexIteratorStackEntry {
	HOTRowexChildPointer const * mCurrentPointerLocation;
	HOTRowexChildPointer mCurrent;
	HOTRowexChildPointer const * mEnd;

public:
	//leave uninitialized
	HOTRowexIteratorStackEntry() {
	}

	HOTRowexIteratorStackEntry * init(HOTRowexChildPointer const * const & currentPointerLocation, HOTRowexChildPointer const * const & end) {
		mCurrentPointerLocation = currentPointerLocation;
		mCurrent = *currentPointerLocation;
		mEnd = end;
		return this;
	}

	HOTRowexIteratorStackEntry * init(HOTRowexChildPointer const * const & currentPointerLocation, HOTRowexChildPointer const & current, HOTRowexChildPointer const * const & end) {
		mCurrentPointerLocation = currentPointerLocation;
		mCurrent = current;
		mEnd = end;
		return this;
	}

	HOTRowexChildPointer const & getCurrent() const {
		return mCurrent;
	}

	HOTRowexChildPointer const * getCurrentPointerLocation() const {
		return mCurrentPointerLocation;
	}

	bool isLastElement() const {
		return (mCurrentPointerLocation + 1) >= mEnd;
	}

	void advance() {
		++mCurrentPointerLocation;
		mCurrent = *mCurrentPointerLocation;
	}
};

}}

#endif
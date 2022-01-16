#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_INSERT_STACK_ENTRY___
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_INSERT_STACK_ENTRY___

#include <hot/commons/SearchResultForInsert.hpp>

#include "HOTSingleThreadedChildPointer.hpp"

namespace hot { namespace singlethreaded {

struct HOTSingleThreadedInsertStackEntry {
	HOTSingleThreadedChildPointer *mChildPointer;
	hot::commons::SearchResultForInsert mSearchResultForInsert;

	inline void initLeaf(HOTSingleThreadedChildPointer * childPointer) {
		mChildPointer = childPointer;
		//important for finding the correct depth!!
		mSearchResultForInsert.mMostSignificantBitIndex = UINT16_MAX;
	}

	//PERFORMANCE this must be uninitialized
	inline HOTSingleThreadedInsertStackEntry() {
	}
};

} }

#endif
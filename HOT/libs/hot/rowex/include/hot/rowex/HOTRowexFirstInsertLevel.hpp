#ifndef __HOT__ROWEX__FIRST_INSERT_LEVEL__
#define __HOT__ROWEX__FIRST_INSERT_LEVEL__

#include <hot/commons/InsertInformation.hpp>

namespace hot { namespace rowex {

template<typename InsertStackEntryType>
struct HOTRowexFirstInsertLevel {
	InsertStackEntryType *mFirstEntry;
	hot::commons::InsertInformation mInsertInformation;
	bool mIsLeafNodePushdown;

	HOTRowexFirstInsertLevel(InsertStackEntryType *firstEntry, hot::commons::InsertInformation const &insertInformation,
					 bool isLeafNodePushdown)
		: mFirstEntry(firstEntry), mInsertInformation(insertInformation), mIsLeafNodePushdown(isLeafNodePushdown) {
	}
};

} }

#endif
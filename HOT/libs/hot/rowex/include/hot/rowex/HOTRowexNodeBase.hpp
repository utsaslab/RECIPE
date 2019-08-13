#ifndef __HOT__ROWEX__SIMD_COB_TRIE_NODE_BASE__
#define __HOT__ROWEX__SIMD_COB_TRIE_NODE_BASE__

#include <atomic>
#include <iostream>
#include <map>
#include <string>

#include <hot/commons/NodeAllocationInformation.hpp>

#include "hot/rowex/HOTRowexNodeBaseInterface.hpp"
#include "hot/rowex/HOTRowexChildPointerInterface.hpp"

constexpr uint32_t ALL_ENTRIES_USED_SIMD_COB_TRIE_INCREMENTAL = UINT32_MAX; //32 1 bits

namespace hot { namespace rowex {

HOTRowexNodeBase::HOTRowexNodeBase(uint16_t const height, hot::commons::NodeAllocationInformation const & nodeAllocationInformation)
	: mFirstChildPointer(reinterpret_cast<HOTRowexChildPointer*>(reinterpret_cast<char*>(this) + nodeAllocationInformation.mPointerOffset)), mUsedEntriesMask(nodeAllocationInformation.mEntriesMask), mHeight(height), mLock(), mIsObsolete(false) {
}

inline size_t HOTRowexNodeBase::getNumberEntries() const {
	return __builtin_popcount(mUsedEntriesMask);
}

inline bool HOTRowexNodeBase::isFull() const {
	return mUsedEntriesMask == ALL_ENTRIES_USED_SIMD_COB_TRIE_INCREMENTAL;
}

inline bool HOTRowexNodeBase::isObsolete() const {
	return mIsObsolete.load(std::memory_order_acquire);
}

inline bool HOTRowexNodeBase::tryLock() {
	bool aquiredLock = false;
	if(!isObsolete()) {
		mLock.lock();
		if(isObsolete()) {
			mLock.unlock();
		} else {
			aquiredLock = true;
		}
	}
	return aquiredLock;
}

inline void HOTRowexNodeBase::markAsObsolete() {
	mIsObsolete.store(true, std::memory_order_release);
}

inline void HOTRowexNodeBase::unlock() {
	mLock.unlock();
}

inline HOTRowexChildPointer const * HOTRowexNodeBase::toResult( uint32_t const resultMask) const {
	return getPointers() + toResultIndex(resultMask);
}

inline HOTRowexChildPointer* HOTRowexNodeBase::toResult( uint32_t const resultMask) {
	size_t const resultIndex = toResultIndex(resultMask);
	return getPointers() + resultIndex;
}

inline unsigned int HOTRowexNodeBase::toResultIndex( uint32_t resultMask ) const {
	assert(resultMask != 0);
	uint32_t const resultMaskForChildsOnly = mUsedEntriesMask & resultMask;
	return hot::commons::getMostSignificantBitIndex(resultMaskForChildsOnly);
}

inline HOTRowexChildPointer * HOTRowexNodeBase::getPointers()  {
	return mFirstChildPointer;
}

inline HOTRowexChildPointer const * HOTRowexNodeBase::getPointers() const {
	return mFirstChildPointer;
}

inline typename HOTRowexNodeBase::iterator HOTRowexNodeBase::begin()
{
	return getPointers();
}

inline typename HOTRowexNodeBase::iterator HOTRowexNodeBase::end()
{
	return getPointers() + getNumberEntries();
}

inline typename HOTRowexNodeBase::const_iterator HOTRowexNodeBase::begin() const
{
	return getPointers();
}

inline typename HOTRowexNodeBase::const_iterator HOTRowexNodeBase::end() const
{
	return getPointers() + getNumberEntries();
}

}}

#endif
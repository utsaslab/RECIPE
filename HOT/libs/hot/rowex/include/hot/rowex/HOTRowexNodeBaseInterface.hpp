#ifndef __HOT__ROWEX__SIMD_COB_TRIE_BASE_NODE_INTERFACE__
#define __HOT__ROWEX__SIMD_COB_TRIE_BASE_NODE_INTERFACE__

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <mutex>

#include <hot/commons/NodeAllocationInformation.hpp>

#include "hot/rowex/HOTRowexChildPointerInterface.hpp"
#include "hot/rowex/SpinLock.hpp"


namespace hot { namespace rowex {

constexpr size_t SIMD_COB_TRIE_NODE_ALIGNMENT = 8;

struct alignas(SIMD_COB_TRIE_NODE_ALIGNMENT) HOTRowexNodeBase {
	using const_iterator = HOTRowexChildPointer const *;
	using iterator = HOTRowexChildPointer *;

	friend class HOTRowexChildPointer;

public:
	/**
	 * points to the first child pointer contained in this node
	 */
	HOTRowexChildPointer* mFirstChildPointer;

	/**
	 * a mask having all bits set, which corresponds to used entries.
	 * Smaller indexes correspond to less significant bits.
	 *
	 * eg.
	 * If entry with index 0 is used the least significant bit is set.
	 * It entry with index 31 is used the most significant bit is set.
	 */
	uint32_t mUsedEntriesMask;

protected:

public:
	/**
	 * the height of this node. The height of a node is defined by the height of its subtree.
	 * A leaf node therefore has height 1, its parent height 2 and so forth.
	 */
	uint16_t const mHeight;
	SpinLock mLock;
	std::atomic<bool> mIsObsolete;

protected:
	inline HOTRowexNodeBase(uint16_t const height, hot::commons::NodeAllocationInformation const & allocationInformation);

	inline void operator delete (void * rawMemory) = delete;

	/**
	 * determines the child pointer corresponding to a result mask.
	 * A result mask has all bits set, which correspond to potential results.
	 * To determine the actual results the resultMask is intersected with the used entries mask and the index
	 * of the most significant bit corresponds to the result index.
	 * Finally the child pointer positioned at this index is returned as the result candidate
	 *
	 * @param resultMask the mask for potential results.
	 * @return the actual result candidate.
	 */
	inline HOTRowexChildPointer const * toResult( uint32_t resultMask) const;

	/**
	 * determines the child pointer corresponding to a result mask.
	 * A result mask has all bits set, which correspond to potential results.
	 * To determine the actual results the resultMask is intersected with the used entries mask and the index
	 * of the most significant bit corresponds to the result index.
	 * Finally the child pointer positioned at this index is returned as the result candidate
	 *
	 * @param resultMask the mask for potential results.
	 * @return the actual result candidate.
	 */
	inline HOTRowexChildPointer* toResult( uint32_t resultMask);

	/**
	 * Determines the index of the actual result candidate.
	 * Therefore the resultMask is first restricted to the actual used entries and then the
	 * index corresponding to the most significant set bit is returned as the index of the actual result candidate
	 *
	 * @param resultMask the mask having bits set for all potential result candidates
	 * @return the index of the actual result candidate
	 */
	inline unsigned int toResultIndex( uint32_t resultMask ) const;


public:
	/**
	 * @return the number of entries stored in this node
	 */
	inline size_t getNumberEntries() const;

	/**
	 * @return whether the number of entries in this node corresponds to the maximum node fanout. For HOTRowex the maximum node fanout is 32.
	 */
	inline bool isFull() const;

	/**
	 * @return whether this node was already replaced by an insert/delete operation and is therefore obsolete.
	 * 	Therefore it will be garbage collected in the future, when no thread holds any reference to this node.
	 */
	inline bool isObsolete() const;

	/**
	 * tries to lock the current node.
	 * A HOTRowexNode can only be locked if it is not obsolete
	 * This operations is blocking
	 *
	 * @return wheter the lock can be aquired. It returns false if the node is in state obsolete
	 */
	inline bool tryLock();

	/**
	 * marks this node as obsolete. This implies that it will eventually be garbage collected.
	 */
	inline void markAsObsolete();


	/**
	 * releases the lock to this node.
	 */
	inline void unlock();

	/**
	 * @return the pointer to the first child pointer stored in this node. All other child pointers are stored sequentially following the first child pointer.
	 */
	inline HOTRowexChildPointer * getPointers();

	/**
	 * @return the pointer to the first child pointer stored in this node. All other child pointers are stored sequentially following the first child pointer.
	 */
	inline HOTRowexChildPointer const * getPointers() const;

	/**
	 * @return an iterator pointing to the first entry stored in this node
	 */
	inline iterator begin();

	/**
	 * @return an iterator pointing to the first element after the last entry stored in this node
	 */
	inline iterator end();

	/**
	 * @return an iterator pointing to the first entry stored in this node
	 */
	inline const_iterator begin() const;

	/**
	 * @return an iterator pointing to the first element after the last entry stored in this node
	 */
	inline const_iterator end() const;
};

}}

#endif

#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_NODE_BASE_INTERFACE__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_NODE_BASE_INTERFACE__

#include <cstdint>
#include <map>
#include <string>

#include <hot/commons/NodeAllocationInformation.hpp>

#include "hot/singlethreaded/HOTSingleThreadedChildPointerInterface.hpp"
#include "hot/singlethreaded/MemoryPool.hpp"

namespace hot { namespace singlethreaded {

constexpr size_t SIMD_COB_TRIE_NODE_ALIGNMENT = 8;
constexpr size_t MAXIMUM_NODE_SIZE_IN_LONGS = 60u;

struct alignas(SIMD_COB_TRIE_NODE_ALIGNMENT) HOTSingleThreadedNodeBase {
	using const_iterator = HOTSingleThreadedChildPointer const *;
	using iterator = HOTSingleThreadedChildPointer *;

	friend class HOTSingleThreadedChildPointer;

public:
	/**
	 * points to the first child pointer contained in this node
	 */
	HOTSingleThreadedChildPointer* mFirstChildPointer;

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

protected:
	inline static MemoryPool<uint64_t, MAXIMUM_NODE_SIZE_IN_LONGS>* getMemoryPool();

	inline HOTSingleThreadedNodeBase(uint16_t const level, hot::commons::NodeAllocationInformation const & allocationInformation);
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
	inline HOTSingleThreadedChildPointer const * toResult( uint32_t resultMask) const;

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
	inline HOTSingleThreadedChildPointer* toResult( uint32_t resultMask);

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
	 * @return the total number of allocations executed on the underlying memory pool
	 */
	static inline size_t getNumberAllocations();

	/**
	 * @return the number of entries stored in this node
	 */
	inline size_t getNumberEntries() const;

	/**
	 * @return whether the number of entries in this node corresponds to the maximum node fanout. For HOTSingleThreaded the maximum node fanout is 32.
	 */
	inline bool isFull() const;

	/**
	 * @return the pointer to the first child pointer stored in this node. All other child pointers are stored sequentially following the first child pointer.
	 */
	inline HOTSingleThreadedChildPointer * getPointers();

	/**
	 * @return the pointer to the first child pointer stored in this node. All other child pointers are stored sequentially following the first child pointer.
	 */
	inline HOTSingleThreadedChildPointer const * getPointers() const;

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

} }

#endif

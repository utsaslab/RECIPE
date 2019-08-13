#ifndef __HOT__ROWEX__CHILD_POINTER_INTERFACE__
#define __HOT__ROWEX__CHILD_POINTER_INTERFACE__

#include <atomic>
#include <set>

#include <hot/commons/NodeType.hpp>

namespace hot { namespace rowex {

struct HOTRowexNodeBase;

class HOTRowexChildPointer {
	std::atomic<intptr_t> mPointer;

	static constexpr std::memory_order read_memory_order = std::memory_order_acquire;
	static constexpr std::memory_order write_memory_order = std::memory_order_release;

public:
	/**
	 * This method calls the passed operation with the actual node type
	 *
	 * Be aware to only call this method on child pointer representing valid nodes.
	 * For all other cases this may result in undefined behaviour.
	 *
	 * @tparam Operation the type of the operation to execute on the node, the return type of the operation determines the return type of the node
	 * @param withPrefetch determines whether the node should be prefetched before invoking the operation
	 * @param operation the type of the operation to execute on the node
	 * @return the result of the operation invoked on the actual node
	 */
	template<typename Operation>
	inline auto executeForSpecificNodeType(bool const withPrefetch, Operation const &operation) const;

	/**
	 * This method calls the passed operation with the actual node type
	 *
	 * Be aware to only call this method on child pointer representing valid nodes.
	 * For all other cases this may result in undefined behaviour.
	 *
	 * @tparam Operation the type of the operation to execute on the node, the return type of the operation determines the return type of the node
	 * @param withPrefetch determines whether the node should be prefetched before invoking the operation
	 * @param operation the type of the operation to execute on the node
	 * @return the result of the operation invoked on the actual node
	 */
	template<typename Operation>
	inline auto executeForSpecificNodeType(bool const withPrefetch, Operation const &operation);

	/**
	 * initializes a default child pointer
	 * a default child pointer is of type node and its value is a nullpointer to a node.
	 */
	inline HOTRowexChildPointer();

	/**
	 * creates a copy of the child pointer
	 * @param rawPointer the child pointer to copy
	 */
	inline HOTRowexChildPointer(HOTRowexChildPointer const & rawPointer);

	/**
	 * For a given node type and the base node pointer constructs a child pointer which is able to deduce the actual nodes type
	 *
	 * Be aware that specifying a non matching node type may result in undefined behaviour
	 *
	 * @param nodeAlgorithmType the actual type of the node to point to
	 * @param node a pointer to the base node
	 */
	inline HOTRowexChildPointer(hot::commons::NodeType  nodeAlgorithmType, HOTRowexNodeBase const *node);

	/**
	 * This initializes a childpointer with the tuple identifier of an entry. How this tuple identifier is formed heavily depends on the actual value's type
	 *
	 * @param leafValue a leaf value represented by its tuple identifier
	 */
	inline HOTRowexChildPointer(intptr_t leafValue);

	inline HOTRowexChildPointer &operator=(const HOTRowexChildPointer &other);

	inline bool operator==(HOTRowexChildPointer const &rhs) const;

	inline bool operator!=(HOTRowexChildPointer const &rhs) const;

	inline void free() const;

	/**
	 * Extracts the node type of this child pointer.
	 * Be aware that this is only defined for actual nodes. In all other cases the result is undefined.
	 *
	 * @return the type of the node pointed to by this child pointer.
	 */
	inline hot::commons::NodeType  getNodeType() const;

	/**
	 * Extracts the raw node pointer from this child pointer.
	 * Be aware that this is only defined for actual nodes. In all other cases the result is undefined.
	 *
	 * @return a pointer to the node itself
	 */
	inline HOTRowexNodeBase *getNode() const;

	/**
	 * Extracts the tuple identifier of this child pointer.
	 * Be aware that this is only defined if this child pointer was initialized as a leaf child pointer.
	 * In all other cases the result is undefined.
	 *
	 * @return the stored tuple identifier
	 */
	inline intptr_t getTid() const;

	/**
	 * @return whether this child pointer instance points to a leaf value or a child node.
	 */
	inline bool isLeaf() const;

	/**
	 * @return whether this child pointer instance points to a leaf value or a child node. In case it is a leaf node 1 is returned otherwise 0.
	 */
	inline intptr_t isLeafInt() const;

	/**
	 *
	 * @return whether this child pointer is no leaf value. It is therefore true for pointers to actual nodes as well as for nullpointers.
	 */
	inline bool isNode() const;

	/**
	 * @return whether this child pointer points to an actual node instance. It is therefore guaranteed that it is no nullptr.
	 */
	inline bool isAValidNode() const;

	/**
	 * @return whether this child pointer is of type node and was initialized with a nullpr.
	 */
	inline bool isEmpty() const;

	/**
	 * @return whether this child pointer represents either a valid node or a leaf value.
	 */
	inline bool isUsed() const;

	inline uint16_t getHeight() const;

	inline HOTRowexChildPointer const * search(uint8_t const * const & keyBytes) const;

	/**
	 * Determines the number of entries in the node represented by this child pointer instance.
	 *
	 * Be aware that this is only defined for childpointers of type node which have been initialized with pointers other than nullptr.
	 *
	 * @return the number of entries in the node in
	 */
	inline unsigned int getNumberEntries() const;

	/**
	 * Collects all discriminative bits used in the binary trie, this pointers instance references to.
	 *
	 * @return the set of discriminative bits used in the underlying binary trie
	 */
	inline std::set<uint16_t> getDiscriminativeBits() const;

	/**
	 * Determines the smallest value rooted in the subtree corresponding to this childpointer.
	 * If this pointer represents a leaf value it is the leaf value itself.
	 * Otherwise it is the smallest value of the subtree rooted at the node's first child pointer.
	 *
	 * Be aware that this function is not defined for childpointers of type node, which have been initialized with a nullptr.
	 *
	 * @return  the smallest value in the subtree corresponding to this childpointer.
	 */
	inline HOTRowexChildPointer getSmallestLeafValueInSubtree() const;

	/**
	 * Determines the largest value rooted in the subtree corresponding to this childpointer.
	 * If this pointer represents a leaf value it is the leaf value itself.
	 * Otherwise it is the largest value of the subtree rooted at the node's last child pointer.
	 *
	 * Be aware that this function is not defined for childpointers of type node, which have been initialized with a nullptr.
	 *
	 * @return  the largest value in the subtree corresponding to this childpointer.
	 */
	inline HOTRowexChildPointer getLargestLeafValueInSubtree() const;

	/**
	 * for its semantics see std::atomic::compare_exchange_strong
	 *
	 * @param expected The expected currentValue
	 * @param newValue The new Value which should be set
	 * @return if the expected currentValue is still the current value. In this case the newValue was successfully set in course of this operation
	 */
	inline bool compareAndSwap(HOTRowexChildPointer const & expected, HOTRowexChildPointer const & newValue);

	/**
	 * deletes all nodes contained in this subtree, including this node itself
	 */
	inline void deleteSubtree();

private:
	inline bool isLeaf(intptr_t currentPointerValue) const;
	inline bool isNode(intptr_t currentPointerValue) const;

	template<hot::commons::NodeType  nodeAlgorithmType>
	static inline auto castToNode(HOTRowexNodeBase const *node);

	template<hot::commons::NodeType  nodeAlgorithmType>
	static inline auto castToNode(HOTRowexNodeBase *node);
};

}}

#endif
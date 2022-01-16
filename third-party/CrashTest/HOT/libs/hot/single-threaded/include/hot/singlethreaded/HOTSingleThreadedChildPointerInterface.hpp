#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_CHILD_POINTER_INTERFACE__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_CHILD_POINTER_INTERFACE__

#include <set>

#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/NodeType.hpp>

namespace hot { namespace singlethreaded {

struct HOTSingleThreadedNodeBase;

class HOTSingleThreadedChildPointer {
	intptr_t mPointer;

public:
	template<hot::commons::NodeType  nodeAlgorithmType>
	static inline auto castToNode(HOTSingleThreadedNodeBase const *node);

	template<hot::commons::NodeType  nodeAlgorithmType>
	static inline auto castToNode(HOTSingleThreadedNodeBase *node);

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
	inline HOTSingleThreadedChildPointer();

	/**
	 * creates a copy of the child pointer
	 * @param rawPointer the child pointer to copy
	 */
	inline HOTSingleThreadedChildPointer(HOTSingleThreadedChildPointer const & rawPointer);

	/**
	 * For a given node type and the base node pointer constructs a child pointer which is able to deduce the actual nodes type
	 *
	 * Be aware that specifying a non matching node type may result in undefined behaviour
	 *
	 * @param nodeAlgorithmType the actual type of the node to point to
	 * @param node a pointer to the base node
	 */
	inline HOTSingleThreadedChildPointer(hot::commons::NodeType  nodeAlgorithmType, HOTSingleThreadedNodeBase const *node);

	/**
	 * This initializes a childpointer with the tuple identifier of an entry. How this tuple identifier is formed heavily depends on the actual value's type
	 *
	 * @param leafValue a leaf value represented by its tuple identifier
	 */
	inline HOTSingleThreadedChildPointer(intptr_t leafValue);

	inline HOTSingleThreadedChildPointer &operator=(const HOTSingleThreadedChildPointer &other);

	inline bool operator==(HOTSingleThreadedChildPointer const &rhs) const;

	inline bool operator!=(HOTSingleThreadedChildPointer const &rhs) const;

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
	inline HOTSingleThreadedNodeBase *getNode() const;

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
	 * @return whether this child pointer points to an actual node instance. It is therefore guaranteed that it is no nullptr.
	 */
	inline bool isAValidNode() const;

	/**
	 *
	 * @return whether this child pointer is no leaf value. It is therefore true for pointers to actual nodes as well as for nullpointers.
	 */
	inline bool isNode() const;

	/**
	 * @return whether this child pointer is of type node and was initialized with a nullpr.
	 */
  	inline bool isUnused() const;

	inline uint16_t getHeight() const;

	template<typename... Args>
	inline auto search(Args... args) const;

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
	inline HOTSingleThreadedChildPointer getSmallestLeafValueInSubtree() const;

	/**
	 * Determines the largest value rooted in the subtree corresponding to this childpointer.
	 * If this pointer represents a leaf value it is the leaf value itself.
	 * Otherwise it is the largest value of the subtree rooted at the node's last child pointer.
	 *
	 * Be aware that this function is not defined for childpointers of type node, which have been initialized with a nullptr.
	 *
	 * @return  the largest value in the subtree corresponding to this childpointer.
	 */
	inline HOTSingleThreadedChildPointer getLargestLeafValueInSubtree() const;

	/**
	 * deletes all nodes contained in this subtree, including this node itself
	 */
	inline void deleteSubtree();
};

} }

#endif
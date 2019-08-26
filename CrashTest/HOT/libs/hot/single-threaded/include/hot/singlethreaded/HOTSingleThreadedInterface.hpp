#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_INTERFACE__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_INTERFACE__

#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <utility>
#include <set>
#include <map>
#include <numeric>
#include <cstring>


#include <hot/commons/Algorithms.hpp>
#include <hot/commons/BiNode.hpp>
#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/TwoEntriesNode.hpp>

#include "hot/singlethreaded/HOTSingleThreadedNode.hpp"

//Helper Data Structures
#include "HOTSingleThreadedInsertStackEntry.hpp"

#include "hot/singlethreaded/HOTSingleThreadedIterator.hpp"
#include "hot/singlethreaded/HOTSingleThreadedDeletionInformation.hpp"

#include "idx/contenthelpers/KeyUtilities.hpp"
#include "idx/contenthelpers/TidConverters.hpp"
#include "idx/contenthelpers/ContentEquals.hpp"
#include "idx/contenthelpers/KeyComparator.hpp"
#include "idx/contenthelpers/OptionalValue.hpp"


namespace hot { namespace singlethreaded {

constexpr uint32_t MAXIMUM_NUMBER_NODE_ENTRIES = 32u;

/**
 * HOTSingleThreaded represents a single threaded height optimized trie.
 *
 * The overall algorithm and the implementation details are described by Binna et al in the paper
 * "HOT: A Height Optimized Trie Index for Main-Memory Database Systems" in the proceedings of Sigmod 2018.
 *
 * @tparam ValueType The type of the value to index. The ValueType must no exceed 8 bytes and may only use the less significant 63 bits. It is therefore perfectly suited to use tuple identifiers as values.
 * @tparam KeyExtractor A Function given the ValueType returns a key, which by using the corresponding functions in idx::contenthelpers can be converted to a big endian ordered byte array.
 */
template<typename ValueType, template <typename> typename KeyExtractor> struct HOTSingleThreaded {
	static_assert(sizeof(ValueType) <= 8, "Only value types which can be stored in a pointer are allowed");
	static KeyExtractor<ValueType> extractKey;
	using KeyType = decltype(extractKey(std::declval<ValueType>()));
	static typename idx::contenthelpers::KeyComparator<KeyType>::type compareKeys;

	using const_iterator = HOTSingleThreadedIterator<ValueType>;
	static const_iterator END_ITERATOR;

	HOTSingleThreadedChildPointer mRoot;

	/**
	 * Creates an empty order preserving index structure based on the HOT algorithm
	 */
	HOTSingleThreaded();
	HOTSingleThreaded(HOTSingleThreaded const & other) = delete;
	HOTSingleThreaded(HOTSingleThreaded && other);

	HOTSingleThreaded& operator=(HOTSingleThreaded const & other) = delete;
	HOTSingleThreaded& operator=(HOTSingleThreaded && other);

	~HOTSingleThreaded();

	inline bool isEmpty() const;
	inline bool isRootANode() const;

	/**
	 * For a given key it looks up the stored value
	 *
	 * @param key the key to lookup
	 * @return the looked up value. The result is valid, if a matching record was found.
	 */
	inline __attribute__((always_inline)) idx::contenthelpers::OptionalValue<ValueType> lookup(KeyType const &key);

	idx::contenthelpers::OptionalValue <ValueType> extractAndMatchLeafValue( HOTSingleThreadedChildPointer const & current, KeyType const &key);

	/**
	 * Scans a given number of values and returns the value at the end of the scan operation
	 *
	 * @param key the key to start the scanning operation at
	 * @param numberValues the number of values to scan in sequential order
	 * @return the record after scanning n values starting at the given key. If not the given number of values can be traversed the resulting value is invalid.
	 */
	inline idx::contenthelpers::OptionalValue<ValueType> scan(KeyType const &key, size_t numberValues) const;

	inline unsigned int searchForInsert(uint8_t const * keyBytes, std::array<HOTSingleThreadedInsertStackEntry, 64> & insertStack);

	inline bool remove(KeyType const & key);

	/**
	 * Inserts the given value into the index. The value is inserted according to its keys value.
	 * In case the index already contains a value for the corresponding key, the value is not inserted.
	 *
	 * @param value the value to insert.
	 * @return true if the value can be inserted, false if the index already contains a value for the corresponding key
	 */
	inline bool insert(ValueType const & value);
	inline bool insertWithInsertStack(std::array<HOTSingleThreadedInsertStackEntry, 64> &insertStack, unsigned int leafDepth,
									  KeyType const &existingKey, uint8_t const *newKeyBytes, ValueType const &newValue);

	/**
	 * Executes an upsert for the given value.
	 * If the index does not contain a value for the value's key, the upsert operation executes an insert.
	 * It the index already contains a value for the value's key, this previously contained value is replaced and returned
	 *
	 * @param newValue the value to upsert.
	 * @return the value of a previously contained value for the same key or an invalid result otherwise
	 */
	inline idx::contenthelpers::OptionalValue<ValueType> upsert(ValueType newValue);

	/**
	 * @return an iterator to the first value according to the key order.
	 */
	inline const_iterator begin() const;

	/**
	 * @return an iterator which is positioned after the last element.
	 */
	inline const_iterator end() const;

	/**
	 * searches an entry for the given key. In case an entry is found, an iterator for this entry is returned.
	 * If no matching entry is found the { @link #end() } iterator is returned.
	 *
	 * @param searchKey the key to search a matching entry for
	 * @return either an iterator pointing to the matching entry or the end iterator
	 */
	inline const_iterator find(KeyType const & searchKey) const;
private:
	inline const_iterator findForNonEmptyTrie(KeyType const & searchKey) const;

public:
	/**
	 * returns an iterator to the first entry which has a key, which is not smaller than the given search key.
	 * This is either an iterator to the matching entry itself or the first value contained in the index which has a key which is larger than the search key.
	 *
	 * @param searchKey the search key to determine the lower bound for
	 * @return either the first entry which has a key, which is not smaller than the given search key or the end iterator if no entry fullfills the lower bound condition
	 */
	inline const_iterator lower_bound(KeyType const & searchKey) const;

	/**
	 * returns an iterator to the first entry which has a key which is larger than the given search key
	 *
	 * @param searchKey the search key to determine the upper bound for
	 * @return either the first entry which has a key larger than the search key or the end iterator if no entry fulfills the uper bound condition
	 */
	inline const_iterator upper_bound(KeyType const & searchKey) const;

	private:
	inline const_iterator lower_or_upper_bound(KeyType const & searchKey, bool is_lower_bound = true) const;

public:
	/**
	 * helper function for debuggin purposes only, which for a given path returns the child pointer stored at this location.
	 * A path to a childpointer consists of a top down ordered list of indexes, where each index determines the position of the pointer to follow.
	 * The first index is applied to the root node, the next to the resulting first level node and so forth.
	 *
	 * @param path the sequence of indexes encoding the path to the corresponding child pointer
	 * @return the matching child pointer of an unused node-type childpointer
	 */
	inline HOTSingleThreadedChildPointer getNodeAtPath(std::initializer_list<unsigned int> path);

	/**
	 * collects a set of statistics for this index instance.
	 * The first entry in the resulting pair contains the total size of the index in bytes.
	 * The second entry contains a map of statistical values containing the following entries:
	 *
	 * - height: the overall height of the tree
	 * - leafNodesOnDepth_<current_depth> how many leaf entries are contained in nodes on the current_depth
	 * - leafNodesOnBinaryDepth_<current_binary_depth> ho many leaf entries would be contained in an equivalent binary patricia trie on depth current_binary_depth
	 * - numberValues the overall number of values stored
	 * - <NODE_TYPE>: the number of nodes of type <NODE_TYPE> possible values for node type are:
	 * 		+ SINGLE_MASK_8_BIT_PARTIAL_KEYS
	 * 		+ SINGLE_MASK_16_BIT_PARTIAL_KEYS
	 * 		+ SINGLE_MASK_32_BIT_PARTIAL_KEYS
	 * 		+ MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS
	 * 		+ MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS
	 * 		+ MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS
	 * 		+ MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS
	 * 		+ MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS
	 * 	- numberAllocations the actual number of allocations which where executd by the underlying memory pool
	 *
	 * @return the collected statistical values
	 */
	std::pair<size_t, std::map<std::string, double>> getStatistics() const;
private:
	inline void collectStatsForSubtree(HOTSingleThreadedChildPointer const & subTreeRoot, std::map<std::string, double> & stats) const;

	/**
	 * root has depth 0
	 * first Level has depth 1...
	 * @param leafNodesPerDepth an output parameter for collecting the number of values aggregated by depth
	 * @param currentDepth the current depth to process
	 */
	inline void getValueDistribution(HOTSingleThreadedChildPointer const & childPointer, size_t depth, std::map<size_t, size_t> & leafNodesPerDepth) const;

	/**
	 * root has depth 0
	 * first Level has depth 1...
	 * @param leafNodesPerDepth an output parameter for collecting the number of values aggregated by depth in a virtual cobtrie
	 * @param currentDepth the current depth to process
	 */
	inline void getBinaryTrieValueDistribution(HOTSingleThreadedChildPointer const & childPointer, size_t binaryTrieDepth, std::map<size_t, size_t> & leafNodesPerDepth) const;
	static bool hasTheSameKey(intptr_t tid, KeyType const &key);

	void removeWithStack(std::array<HOTSingleThreadedInsertStackEntry, 64> const &searchStack, unsigned int currentDepth);
	void removeRecurseUp(std::array<HOTSingleThreadedInsertStackEntry, 64> const &searchStack, unsigned int currentDepth, HOTSingleThreadedDeletionInformation const & deletionInformation, HOTSingleThreadedChildPointer const & replacement);
	template<typename Operation> void removeAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(
		std::array<HOTSingleThreadedInsertStackEntry, 64> const &searchStack, unsigned int currentDepth, HOTSingleThreadedDeletionInformation const & deletionInformation, Operation const & operation
	);

	template<typename Operation> static void removeEntryAndExecuteOperationOnNewNodeBeforeIntegrationIntoTreeStructure(
		HOTSingleThreadedChildPointer* const currentNodePointer, HOTSingleThreadedDeletionInformation const & deletionInformation, Operation const & operation
	);

	HOTSingleThreadedDeletionInformation determineDeletionInformation(
		const std::array<HOTSingleThreadedInsertStackEntry, 64> &searchStack, unsigned int currentDepth);

public:
	/**
	 * @return the overall tree height
	 */
	size_t getHeight() const;
};

inline void insertNewValueIntoNode(
	std::array<HOTSingleThreadedInsertStackEntry, 64> & insertStack, hot::commons::DiscriminativeBit const & significantKeyInformation,
	unsigned int insertDepth, unsigned int leafDepth, HOTSingleThreadedChildPointer const & valueToInsert
);
template<typename NodeType> inline void insertNewValue(
	NodeType const &existingNode, std::array<HOTSingleThreadedInsertStackEntry, 64> &insertStack,
	hot::commons::InsertInformation const &insertInformation, unsigned int insertDepth, HOTSingleThreadedChildPointer const &valueToInsert
);
template<typename NodeType> inline void insertNewValueResultingInNewPartitionRoot(
	NodeType const &existingNode, std::array<HOTSingleThreadedInsertStackEntry, 64> &insertStack, const hot::commons::DiscriminativeBit &keyInformation,
	unsigned int insertDepth, HOTSingleThreadedChildPointer const &valueToInsert
);
inline void integrateBiNodeIntoTree(
	std::array<HOTSingleThreadedInsertStackEntry, 64> & insertStack, unsigned int currentDepth,
	hot::commons::BiNode<HOTSingleThreadedChildPointer> const & splitEntries, bool const newIsRight
);


} }

#endif

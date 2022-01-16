#ifndef __HOT__ROWEX__HOT_ROWEX_INTERFACE__
#define __HOT__ROWEX__HOT_ROWEX_INTERFACE__

#include <idx/contenthelpers/KeyComparator.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/BiNode.hpp>

#include "hot/rowex/HOTRowexChildPointerInterface.hpp"
#include "hot/rowex/EpochBasedMemoryReclamationStrategy.hpp"
#include "hot/rowex/HOTRowexInsertStack.hpp"
#include "hot/rowex/HOTRowexInsertStackEntry.hpp"
#include "hot/rowex/HOTRowexIterator.hpp"

namespace hot { namespace rowex {

/**
 * HOTRowex represents a concurrent height optimized trie using a
 * Read-Optimized Write EXclusion (ROWEX) concurrency protocol.
 *
 * The overall algorithm and the implementation details are described by Binna et al in the paper
 * "HOT: A Height Optimized Trie Index for Main-Memory Database Systems" in the proceedings of Sigmod 2018.
 *
 * @tparam ValueType The type of the value to index. The ValueType must no exceed 8 bytes and may only use the less significant 63 bits. It is therefore perfectly suited to use tuple identifiers as values.
 * @tparam KeyExtractor A Function given the ValueType returns a key, which by using the corresponding functions in idx::contenthelpers can be converted to a big endian ordered byte array.
 */
template<typename ValueType, template <typename> typename KeyExtractor> struct HOTRowex {
	static_assert(sizeof(ValueType) <= 8, "Only value types which can be stored in a pointer are allowed");

	static KeyExtractor<ValueType> extractKey;
	using KeyType = decltype(extractKey(std::declval<ValueType>()));

	using InsertStackType = HOTRowexInsertStack<ValueType, KeyExtractor, HOTRowexInsertStackEntry>;
	using InsertStackEntryType = typename InsertStackType::EntryType;
	using const_iterator = HOTRowexSynchronizedIterator<ValueType, KeyExtractor>;

	static typename idx::contenthelpers::KeyComparator<KeyType>::type compareKeys;

	HOTRowexChildPointer mRoot;

	EpochBasedMemoryReclamationStrategy* const mMemoryReclamation;

	/**
	 * Creates an empty concurrent order preserving index structure based on the HOT algorithm
	 */
	HOTRowex();
	HOTRowex(HOTRowex const & other) = delete;
	HOTRowex(HOTRowex && other);

	HOTRowex& operator=(HOTRowex const & other) = delete;
	HOTRowex& operator=(HOTRowex && other);

	~HOTRowex();

	/**
	 * For a given key it looks up the stored value
	 *
	 * @param key the key to lookup
	 * @return the looked up value. The result is valid, if a matching record was found.
	 */
	inline idx::contenthelpers::OptionalValue<ValueType> lookup(KeyType const &key) const;

	/**
	 * Scans a given number of values and returns the value at the end of the scan operation
	 *
	 * @param key the key to start the scanning operation at
	 * @param numberValues the number of values to scan in sequential order
	 * @return the record after scanning n values starting at the given key. If not the given number of values can be traversed the resulting value is invalid.
	 */
	inline idx::contenthelpers::OptionalValue<ValueType> scan(KeyType const &key, size_t numberValues) const;

	/**
	 * Inserts the given record into the index. The value is inserted according to its keys value.
	 * In case the index already contains a value for the corresponding key, the value is not inserted.
	 *
	 * @param value the value to insert.
	 * @return true if the value can be inserted, false if the index already contains a value for the corresponding key
	 */
	inline bool insert(ValueType const & value);
private:
	inline bool insertGuarded(ValueType const & value);

public:
	/**
	 * Executes an upsert for the given value.
	 * If the index does not contain a value for the value's key, the upsert operation executes an insert.
	 * It the index already contains a value for the value's key, this previously contained value is replaced and returned
	 *
	 * @param newValue the value to upsert.
	 * @return the value of a previously contained value for the same key or an invalid result otherwise
	 */
	inline idx::contenthelpers::OptionalValue<ValueType> upsert(ValueType newValue);

private:
	inline idx::contenthelpers::OptionalValue<bool> insertNewValue(
		InsertStackType & insertStack, hot::commons::DiscriminativeBit const & newBit, ValueType const & value
	);
	inline idx::contenthelpers::OptionalValue<bool> insertForStackRange(
		InsertStackType & insertStack, const HOTRowexFirstInsertLevel<InsertStackEntryType> & insertLevel, unsigned int numberLockedEntries, ValueType const & valueToInsert
	);
	static inline void leafNodePushDown(
		InsertStackEntryType & leafEntry, hot::commons::InsertInformation const & insertInformation, HOTRowexChildPointer const & valueToInsert
	);
	static inline void normalInsert(
		InsertStackEntryType & currentNodeStackEntry, hot::commons::InsertInformation const & insertInformation, HOTRowexChildPointer const & valueToInsert
	);
	static inline hot::commons::BiNode<HOTRowexChildPointer> split(
		InsertStackEntryType &currentInsertStackEntry,  hot::commons::InsertInformation const &insertInformation, HOTRowexChildPointer const &valueToInsert
	);
	static inline hot::commons::BiNode<HOTRowexChildPointer> integrateAndSplit(
		InsertStackEntryType & currentInsertStackEntry, hot::commons::BiNode<HOTRowexChildPointer> const & splitEntries
	);
	static inline void finalParentPullUp(
		InsertStackEntryType & currentNodeStackEntry, hot::commons::BiNode<HOTRowexChildPointer> const & splitEntries
	);

public:
	/**
	 * @return an iterator to the first value according to the key order.
	 */
	inline const_iterator begin() const;

	/**
	 * @return an iterator which is positioned after the last element.
	 */
	inline const_iterator const & end() const;

	/**
	 * searches an entry for the given key. In case an entry is found, an iterator for this entry is returned.
	 * If no matching entry is found the { @link #end() } iterator is returned.
	 *
	 * @param searchKey the key to search a matching entry for
	 * @return either an iterator pointing to the matching entry or the end iterator
	 */
	inline const_iterator find(KeyType const & searchKey) const;

	/**
	 * returns an iterator to the first entry which has a key, which is not smaller than the given search key.
	 * This is either an iterator to the matching entry itself or the first value contained in the index which has a key which is larger than the search key.
	 *
	 * @param searchKey the search key to determine the lower bound for
	 * @return either the first entry which has a key, which is not smaller than the given search key or the end iterator if no entry fulfills the lower bound condition
	 */
	inline const_iterator lower_bound(KeyType const & searchKey) const;

	/**
	 * returns an iterator to the first entry which has a key which is larger than the given search key
	 *
	 * @param searchKey the search key to determine the upper bound for
	 * @return either the first entry which has a key larger than the search key or the end iterator if no entry fulfills the uper bound condition
	 */
	inline const_iterator upper_bound(KeyType const & searchKey) const;

	/**
	 * helper function for debuggin purposes only, which for a given path returns the child pointer stored at this location.
	 * A path to a childpointer consists of a top down ordered list of indexes, where each index determines the position of the pointer to follow.
	 * The first index is applied to the root node, the next to the resulting first level node and so forth.
	 *
	 * @param path the sequence of indexes encoding the path to the corresponding child pointer
	 * @return the matching child pointer of an unused node-type childpointer
	 */
	inline HOTRowexChildPointer getNodeAtPath(std::initializer_list<unsigned int> path);

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
	 *
	 * @return the collected statistical values
	 */
	std::pair<size_t, std::map<std::string, double>> getStatistics() const;
private:
	inline void collectStatsForSubtree(HOTRowexChildPointer const & subTreeRoot, std::map<std::string, double> & stats) const;

	inline void getValueDistribution(HOTRowexChildPointer const & childPointer, size_t depth, std::map<size_t, size_t> & leafNodesPerDepth) const;

	inline void getBinaryTrieValueDistribution(HOTRowexChildPointer const & childPointer, size_t binaryTrieDepth, std::map<size_t, size_t> & leafNodesPerDepth) const;

};

}}

#endif
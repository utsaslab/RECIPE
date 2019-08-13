#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_NODE_INTERFACE__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_NODE_INTERFACE__

#include <iostream>

#include <idx/contenthelpers/OptionalValue.hpp>

#include <hot/commons/BiNodeInformation.hpp>
#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/PartialKeyConversionInformation.hpp>
#include <hot/commons/NodeMergeInformation.hpp>
#include <hot/commons/NodeParametersMapping.hpp>
#include <hot/commons/NodeType.hpp>
#include <hot/commons/NodeAllocationInformation.hpp>
#include <hot/commons/SearchResultForInsert.hpp>
#include <hot/commons/SparsePartialKeys.hpp>

#include "hot/singlethreaded/HOTSingleThreadedDeletionInformation.hpp"
#include "hot/singlethreaded/HOTSingleThreadedNodeBaseInterface.hpp"

namespace hot { namespace singlethreaded {

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> struct alignas(SIMD_COB_TRIE_NODE_ALIGNMENT) HOTSingleThreadedNode;

template<typename DiscriminativeBitsRepresentation, typename PartialKeyTypeTemplateParam> struct alignas(SIMD_COB_TRIE_NODE_ALIGNMENT) HOTSingleThreadedNode : HOTSingleThreadedNodeBase {
	using PartialKeyType = PartialKeyTypeTemplateParam;

	static constexpr hot::commons::NodeType mNodeType { hot::commons::NodeParametersToNodeType<DiscriminativeBitsRepresentation, PartialKeyType>::mNodeType };

	using PartialKeyConversionInformation = hot::commons::PartialKeyConversionInformation<PartialKeyType>;
	using DiscriminativeBitsRepresentationType = DiscriminativeBitsRepresentation;

	DiscriminativeBitsRepresentation mDiscriminativeBitsRepresentation;
	hot::commons::SparsePartialKeys<PartialKeyType> mPartialKeys;

	/**
	 * constructs a new node given its height, the number of entries it will store
	 * and a representation of all discriminative bits necessary to discriminate its entries
	 *
	 * Be aware that solely calling this constructor does not initialize the nodes partial keys nor its child pointers
	 *
	 * @param height the height of this node and therefore of its subtree
	 * @param numberEntries the number of entries to store
	 * @param discriminativeBitsRepresentation the discriminativeBitsRepresentation used for this node
	 */
	HOTSingleThreadedNode(uint16_t const height, uint16_t const numberEntries, DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation);

public:
	/**
	 * allocates memory for a node holding number Entries value
	 *
	 * @param baseSize the base size
	 * @param numberEntries the number of entries this node should contain
	 * @return the allocated memory
	 */
	inline void* operator new (size_t /* baseSize */, uint16_t const numberEntries);

	/**
	 * releases a nodes memory
	 */
	inline void operator delete (void *);

	/**
	 * determines the allocation information necessary to allocate a node of this type with a specifc number of entries
	 * @param numberEntries the number of entries the allocation information should be calculated for
	 * @return the determined allocation information
	 */
	static inline hot::commons::NodeAllocationInformation getNodeAllocationInformation(uint16_t const numberEntries);

	/**
	 * constructs a new node from a given source node by inserting an additional value
	 *
	 * @tparam SourceDiscriminativeBitsRepresentation the discriminative bits representation of the source node
	 * @tparam SourcePartialKeyType the partial key type used to store partial keys in the source node
	 * @param sourceNode the source node
	 * @param numberEntries the number of entries to store in the new node. must be number of entries in the source node + 1
	 * @param discriminativeBitsRepresentation the discriminative bits information required distinguish all of the old values and the new value to insert from each other
	 * @param insertInformation the information required to determine the insert location and the recoding information necessary to recode the partial keys
	 * @param newValue the new value to insert
	 */
	template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode(
		HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
		uint16_t const numberEntries,
		DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
		hot::commons::InsertInformation const & insertInformation,
		HOTSingleThreadedChildPointer const & newValue
	);

	/**
	 * constructs a new node by extracting a given range of entries from an existing node
	 *
	 * @tparam SourceDiscriminativeBitsRepresentation the discriminative bits representation of the source node
	 * @tparam SourcePartialKeyType the partial key type used to store partial keys in the source node
	 * @param the source node
	 * @param numberEntries the number of entries to store in the new node. must be equal to the number of entries in the source range
	 * @param discriminativeBitsRepresentation the discriminative bits information, which is required to distinguish all entries in the source range
	 * @param compressionMask the compression mask which can be used to recode the partial keys from the source representation to the representation for the new node
	 * @param firstIndexInRange the first index in the range of entries to copy
	 * @param numberEntriesInRange  the number of entries in the range to copy
	 */
	template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode(
		HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
		uint16_t const numberEntries,
		DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
		SourcePartialKeyType compressionMask,
		uint32_t firstIndexInRange,
		uint32_t numberEntriesInRange
	);

	/**
	 * constructs a new node by extracting a given range of entries from an existing node and adding an additional entry
	 *
	 * @tparam SourceDiscriminativeBitsRepresentation the discriminative bits representation of the source node
	 * @tparam SourcePartialKeyType the partial key type used to store partial keys in the source node
	 * @param the source node
	 * @param numberEntries the number of entries to store in the new node. must be equal to the number of entries in the source range + 1
	 * @param discriminativeBitsRepresentation the discriminative bits information, which is required to distinguish all entries in the source range and the new value to insert
	 * @param compressionMask the compression mask which can be used to recode the partial keys from the source representation to the representation for the new node. Before actually inserting the compressed partial keys, they have to be recoded to respect a potential new discriminative bit required for the new entry.
	 * @param firstIndexInRange the first index in the range of entries to copy
	 * @param numberEntriesInRange  the number of entries in the range to copy
	 * @param insertInformation the information required to determine the insert location and the recoding information necessary to recode the partial keys
	 * @param newValue the new value to insert
	 */
	template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode(
		HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
		uint16_t const numberEntries,
		DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
		SourcePartialKeyType compressionMask,
		uint32_t firstIndexInRange,
		uint32_t numberEntriesInRange,
		hot::commons::InsertInformation const & insertInformation,
		HOTSingleThreadedChildPointer const & newValue
	);

	/**
	 * Constructs a new node by removing a single entry
	 *
	 * @tparam SourceDiscriminativeBitsRepresentation the discriminative bits representation of the source node
	 * @tparam SourcePartialKeyType the partial key type used to store partial keys in the source node
	 * @param the source node
	 * @param numberEntries the number of entries to store in the new. this must be equal to the number of entries in the source node - 1
	 * @param discriminativeBitsRepresentation the discriminative bits information, which is required to distinguish all entries which remain in the new node
	 * @param deletionInformation the information which entry to delete and how to compress the remaining entry's partial keys
	 */
	template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode(
		HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
		uint16_t const numberEntries,
		DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
		HOTSingleThreadedDeletionInformation const & deletionInformation
	);

	/**
	 * Constructs a new node by simultaneously removing a single entry and adding a new entry
	 *
	 * @tparam SourceDiscriminativeBitsRepresentation the discriminative bits representation of the source node
	 * @tparam SourcePartialKeyType the partial key type used to store partial keys in the source node
	 * @param the source node
	 * @param numberEntries the number of entries to store in the new. this must be equal to the number of entries in the source node
	 * @param discriminativeBitsRepresentation the discriminative bits information, which is required to distinguish all entries which remain in the new node and the additional new entry
	 * @param deletionInformation the information which entry to delete and how to compress the remaining entry's partial keys
	 */
	template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline HOTSingleThreadedNode(
		HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode,
		uint16_t const numberEntries,
		DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
		HOTSingleThreadedDeletionInformation const & deletionInformation,
		hot::commons::DiscriminativeBit const & keyInformation,
		HOTSingleThreadedChildPointer const & newValue
	);

	/**
	 * Constructs a new node by merging two nodes and removing a single entry from one of the source nodes
	 *
	 * @tparam LeftNodeType the type of the left node
	 * @tparam RightNodeType the type of the right node
	 * @param numberEntries the number of entries in the new node, which is equal to the number of entries in the left node + number of entries in the right node - 1
	 * @param discriminativeBitsRepresentation the discriminative bits required to distinguish all entries of the newly created node
	 * @param leftSourceNode the left node
	 * @param leftRecodingMask the recoding mask to recode the partial keys of the left node
	 * @param rightSourceNode the right node
	 * @param rightRecodingMask the recoding mask to recode the partial keys of the left node
	 * @param deletionInformation the information which entry to delete and how to compress the final partial keys
	 * @param entryToDeleteIsInRightSide decides whether the entry to delete is in the right or in the left source node
	 */
	template<typename LeftNodeType, typename RightNodeType> inline HOTSingleThreadedNode(
		uint16_t const numberEntries, DiscriminativeBitsRepresentation const & discriminativeBitsRepresentation,
		LeftNodeType const & leftSourceNode,
		uint32_t leftRecodingMask,
		RightNodeType const & rightSourceNode,
		uint32_t rightRecodingMask,
		HOTSingleThreadedDeletionInformation const & deletionInformation,
		bool entryToDeleteIsInRightSide
	);

	/**
	 * searches the node and returns a potential result candidate by:
	 * 	1. extracting the discriminative bits of the search key bytes thereby creating a dense partial key
	 * 	2. determining all stored sparse partial keys which comply with the dense search partial key
	 * 	3. determine the highest index of a complying partial key
	 * 	4. returns the result candidate at this location
	 *
	 *
	 * @param keyBytes the key bytes of the search key
	 * @return the result candidate
	 */
	inline HOTSingleThreadedChildPointer const * search(uint8_t const * keyBytes) const;

	/**
	 * searches the node and returns a potential result candidate by:
	 * 	1. extracting the discriminative bits of the search key bytes thereby creating a dense partial key
	 * 	2. determining all stored sparse partial keys which comply with the dense search partial key
	 * 	3. determine the highest index of a complying partial key
	 * 	4. returns the result candidate at this location
	 *
	 * In addition to the normal search it determines the most significant bit of the discriminative bits used in this node
	 * and the index of the result candidate. Both informations are required in HOT's insertion algorithm
	 *
	 * @param searchResultOut an outparameter for the additional result information
	 * @param keyBytes the key bytes of the search key
	 * @return the result candidate
	 */
	inline HOTSingleThreadedChildPointer* searchForInsert(hot::commons::SearchResultForInsert & searchResultOut, uint8_t const * keyBytes) const;

	/**
	 * Determines the information required to insert a new entry in the node.
	 * The new entry has the property that the given discriminative bit is required to distinguish the new entry from all other entries contained in the node.
	 * Furthermore the provided entryIndex must be correspond to one of the entries which share a common prefix with the new entry's key up to the discriminative bits position.
	 * The final insertion information describes the affected subtree and contains information to create the new partial key
	 *
	 * @param entryIndex the index of an entry which shares a common prefix with the new entry up to the discriminative bits position
	 * @param discriminativeBit the additional discriminative bit which is required to distinguish the new entry from the currently contained entries
	 * @return the insertion information
	 */
	inline hot::commons::InsertInformation getInsertInformation(uint entryIndex, hot::commons::DiscriminativeBit const & discriminativeBit) const;

	/**
	 * Determines the information necessary to delete the entry with the given index from this node.
	 * The deletion information consists of the compression information to recode the remaining partial keys,
	 * the discriminative bit which was necessary to distinguish the entry to delete from the remaining entries and the
	 * position of the entries in the affected BiNode
	 *
	 * @param indexOfEntryToRemove the index of the entry to remove
	 * @return the deletion information
	 */
	inline HOTSingleThreadedDeletionInformation getDeletionInformation(uint32_t indexOfEntryToRemove) const;


	/**
	 * This method creates a copy of the existing node containing the new value.
	 *
	 * @param insertInformation the insertion information used to determine the insertion position and potential recoding information
	 * @param newValue the new value to insert
	 * @return the pointer to the newly created node
	 */
	inline HOTSingleThreadedChildPointer addEntry(
		hot::commons::InsertInformation const & insertInformation, HOTSingleThreadedChildPointer const & newValue
	) const;

	/**
	 * This method creats a copy of the existing node without the entry described by the deletion information
	 *
	 * @param deletionInformation the information necessary to remove an entry.
	 * @return the newly created node
	 */
	inline HOTSingleThreadedChildPointer removeEntry(HOTSingleThreadedDeletionInformation const & deletionInformation) const;

	/**
	 * This method creats a copy of the existing node with a single entry removed and an additional entry added.
	 *
	 * @param deletionInformation the information necessary to remove a single entry.
	 * @param discriminativeBit the discriminative bit required to distighuish the new entry from the remaining source entries
	 * @param newValue the new value to insert
	 * @return the newly created node
	 */
	inline HOTSingleThreadedChildPointer removeAndAddEntry(HOTSingleThreadedDeletionInformation const & deletionInformation, hot::commons::DiscriminativeBit const & discriminativeBit, HOTSingleThreadedChildPointer const & newValue) const;

	/**
	 * This methods creates a new node from a range of the current node's entries
	 *
	 * @param firstIndexInRange the index of the first entry to copy to the new node
	 * @param numberEntriesInRange the number of entries to copy to the new node
	 * @return the newly created node
	 */
	inline HOTSingleThreadedChildPointer compressEntries(uint32_t firstIndexInRange, uint16_t numberEntriesInRange) const;

	/**
	 * This methods creates a new node from a range of the current node's entries and an additional new entry to insert
	 *
	 * @param firstIndexInRange the index of the first entry to copy to the new node
	 * @param numberEntriesInRange the number of entries to copy to the new node
	 * @param insertInformation the insert information required to determine the insertion position and potential recoding information
	 * @param newValue the new value to insert
	 * @return the newly created node
	 */
	inline HOTSingleThreadedChildPointer compressEntriesAndAddOneEntryIntoNewNode(
		uint32_t firstIndexInRange, uint16_t numberEntriesInRange, hot::commons::InsertInformation const & insertInformation, HOTSingleThreadedChildPointer const & newValue
	) const;

	/**
	 * For a specific entry it determines the index of its least significant used discriminative bit (thereby representing its parent BiNode)
	 *
	 * @param entryIndex the index of the entry to determine the least significant used discriminative bit for
	 * @return the least significant used discriminative bit for a given entry
	 */
	inline uint16_t getLeastSignificantDiscriminativeBitForEntry(unsigned int entryIndex) const;

	/**
	 * This method handles an overflow which would occure when inserting the new value into the current node.
	 * Therefore, it combines an insert with a node split and returns a new BiNode pointing to the newly created nodes
	 *
	 * @param insertInformation the insert information necessary to the determine insert position and recoding information
	 * @param newValue the new value to insert
	 * @return the inserted and split node.
	 */
	inline hot::commons::BiNode<HOTSingleThreadedChildPointer> split(hot::commons::InsertInformation const & insertInformation, HOTSingleThreadedChildPointer const & newValue) const;

	/**
	 * @return a child pointer which combines the nodes address whith its actual node type
	 */
	inline HOTSingleThreadedChildPointer toChildPointer() const;

	/**
	 * Determines the direct neighbour of the given entry or an unused childpointer if no direct neighbour exists.
	 *
	 * A direct neighbour only exists if its BiNode sibling is either a leaf entry of a pointer to a hot node
	 *
	 *     b
	 *   /   \
	 * a      c
	 *
	 * In the example given above a is a direct neighbour of b
	 * In the example given below d is not a direct neighbour of a. Nor is c a direct neighbour as it only represents an internal node.
	 *
	 *     b
	 *   /   \
	 * a      c
	 *      /   \
	 *     d     e
	 *
	 *
	 *
	 * @param entryIndex the index of the entry to get its direct neighbour for
	 * @return
	 */
	idx::contenthelpers::OptionalValue<std::pair<uint16_t, HOTSingleThreadedChildPointer*>> getDirectNeighbourOfEntry(size_t entryIndex) const;

	/**
	 * For a given entry it determines its direct parent BiNode and thereby the position of all entries contained in the subtree rooted at this BiNode.
	 *
	 * @param entryIndex the index of the entry to determine the parent BiNode for
	 * @return a description of the parent bi node consisting of the entries contained in its subtree, the corresponding discriminative bit and a mask for this discriminative bit
	 */
	hot::commons::BiNodeInformation getParentBiNodeInformationOfEntry(size_t entryIndex) const;

private:
	template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline void compressRangeIntoNewNode(
		HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const &sourceNode, uint32_t compressionMask,
		uint32_t firstIndexInSourceRange, uint32_t firstIndexInTarget, uint32_t numberEntriesInRange
	);

	template<typename SourceNodeType> inline void copyAndRemove(
		SourceNodeType const & sourceNode, HOTSingleThreadedDeletionInformation const & deletionInformation, uint32_t sourceRecodingMask, uint32_t targetStartIndex
	);

	template<typename SourceNodeType> inline void copyAndRecode(
		SourceNodeType const & sourceNode, uint32_t recodingMask, uint32_t targetStartIndex
	);

	inline void addHighBitToMasksInRightHalf(uint32_t firstIndexInRightHalf);

	template<typename SourceDiscriminativeBitsRepresentation, typename SourcePartialKeyType> inline PartialKeyConversionInformation getConversionInformation(
		HOTSingleThreadedNode<SourceDiscriminativeBitsRepresentation, SourcePartialKeyType> const & sourceNode, hot::commons::DiscriminativeBit const & significantKeyInformation
	) const;

	template<typename SourcePartialKeyType> inline PartialKeyConversionInformation getConversionInformationForCompressionMask(
		SourcePartialKeyType compressionMask, hot::commons::DiscriminativeBit const & significantKeyInformation
	) const;

	inline PartialKeyConversionInformation getConversionInformation(
		uint32_t sourceMaskBits, hot::commons::DiscriminativeBit const & significantKeyInformation
	) const;

	/**
	 *
	 * @return a mask for all entries which have the most significant bit set for this node. the position of a bit which is set in this mask represents an entry which has the most significant bit set.
	 */
	inline uint32_t getMaskForLargerEntries() const;

public:
	/**
	 * @return the size of this nodes in bytes.
	 * This cannot be determined by sizeof as the actual size of the partial keys array as well as the size of the array of childpointers is not determined at compile time
	 */
	inline size_t getNodeSizeInBytes() const;

	/**
	 * @return the depth each entry would have in a corresponding pointer based binary patricia trie.
	 */
	inline std::array<uint8_t, 32> getEntryDepths() const;

private:
	inline void collectEntryDepth(std::array<uint8_t, 32> & entryDepths, size_t minEntryInRange, size_t maxEntryInRange, size_t currentDepth, uint32_t usedMaskBits) const;

public:
	/**
	 * @return checks whether all invariants are met for this node
	 */
	inline bool isPartitionCorrect() const;

	/**
	 * Gets a mapping from the absolute bit positions of the extraction bits used to the corresponding
	 * bit positions in the entries masks. The corresponding bit position in the entries masks is calculated from right to left.
	 * The absolute bit positions of the extraction bits are calculated in the order of their occurence hence from left to right
	 *
	 * @param maskBitMapping maps from the absoluteBitPosition to its maskPosition
	 */
	std::map<uint16_t, uint16_t> getExtractionMaskToEntriesMasksMapping() const;

	/**
	 * prints all partial keys contained in this node and a mapping to big endian byte order and a mapping to the actual key bits
	 *
	 * @param out the outputstream to print the keys to. It defaults to stdout
	 */
	void printPartialKeysWithMappings(std::ostream &out = std::cout) const;

private:
	// Prevent heap allocation
	void * operator new   (size_t);
	void * operator new[] (size_t);
	void operator delete[] (void*);

};

inline HOTSingleThreadedChildPointer mergeNodesAndRemoveEntryIfPossible(uint32_t rootDiscriminativeBitPosition, HOTSingleThreadedChildPointer const &left, HOTSingleThreadedChildPointer const &right, HOTSingleThreadedDeletionInformation const &deletionInformation, bool entryToDeleteIsInRightSide);
template<typename LeftDiscriminativeBitsRepresentation, typename LeftPartialKeyType, typename RightDiscriminativeBitsRepresentation, typename RightPartialKeyType> inline HOTSingleThreadedChildPointer mergeNodesAndRemoveEntry(
	hot::commons::NodeMergeInformation const & mergeInformation, HOTSingleThreadedNode<LeftDiscriminativeBitsRepresentation, LeftPartialKeyType> const & left, HOTSingleThreadedNode<RightDiscriminativeBitsRepresentation, RightPartialKeyType> const & right, HOTSingleThreadedDeletionInformation const & deletionInformation, bool entryToDeleteIsInRightSide
);


} }

#endif

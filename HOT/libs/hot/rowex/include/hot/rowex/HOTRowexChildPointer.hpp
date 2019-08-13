#ifndef __HOT__ROWEX__CHILD_POINTER__
#define __HOT__ROWEX__CHILD_POINTER__

#include <hot/commons/NodeType.hpp>
#include <hot/commons/NodeParametersMapping.hpp>

#include "hot/rowex/HOTRowexChildPointerInterface.hpp"
#include "hot/rowex/HOTRowexNodeInterface.hpp"

namespace hot { namespace rowex {

constexpr intptr_t NODE_ALGORITHM_TYPE_EXTRACTION_MASK = 0x7u;
constexpr intptr_t POINTER_AND_IS_LEAF_VALUE_MASK = 15u;
constexpr intptr_t POINTER_EXTRACTION_MASK = ~(POINTER_AND_IS_LEAF_VALUE_MASK);

template<hot::commons::NodeType nodeAlgorithmType> inline auto HOTRowexChildPointer::castToNode(HOTRowexNodeBase const * node) {
	using DiscriminativeBitsRepresentationType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyMappingType ;
	using PartialKeyType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyType ;
	return reinterpret_cast<HOTRowexNode<DiscriminativeBitsRepresentationType, PartialKeyType> const *>(node);
}

template<hot::commons::NodeType nodeAlgorithmType> inline auto HOTRowexChildPointer::castToNode(HOTRowexNodeBase * node) {
	using DiscriminativeBitsRepresentationType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyMappingType ;
	using PartialKeyType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyType ;
	return reinterpret_cast<HOTRowexNode<DiscriminativeBitsRepresentationType, PartialKeyType> *>(node);
}

template<typename Operation> inline auto HOTRowexChildPointer::executeForSpecificNodeType(bool const withPrefetch, Operation const & operation) const {
	HOTRowexNodeBase const * node = getNode();

	if(withPrefetch) {
		__builtin_prefetch(node);
		__builtin_prefetch(reinterpret_cast<char const*>(node) + 64);
		__builtin_prefetch(reinterpret_cast<char const*>(node) + 128);
		__builtin_prefetch(reinterpret_cast<char const*>(node) + 192);
	}

	switch(getNodeType()) {
		case hot::commons::NodeType::SINGLE_MASK_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::SINGLE_MASK_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::SINGLE_MASK_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::SINGLE_MASK_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::SINGLE_MASK_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::SINGLE_MASK_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		default: //hot::commons::NodeType::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
	}
}

template<typename Operation> inline auto HOTRowexChildPointer::executeForSpecificNodeType(bool const withPrefetch, Operation const & operation) {
	HOTRowexNodeBase * node = getNode();

	if(withPrefetch) {
		__builtin_prefetch(node);
		__builtin_prefetch(reinterpret_cast<char*>(node) + 64);
		__builtin_prefetch(reinterpret_cast<char*>(node) + 128);
		__builtin_prefetch(reinterpret_cast<char*>(node) + 192);
	}

	switch(getNodeType()) {
		case hot::commons::NodeType::SINGLE_MASK_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::SINGLE_MASK_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::SINGLE_MASK_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::SINGLE_MASK_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::SINGLE_MASK_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::SINGLE_MASK_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		default: //hot::commons::NodeType::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
	}
}

inline HOTRowexChildPointer::HOTRowexChildPointer() : mPointer(reinterpret_cast<intptr_t>(nullptr)) {
}

inline HOTRowexChildPointer::HOTRowexChildPointer(HOTRowexChildPointer const & other) : mPointer(other.mPointer.load(read_memory_order)) {
}


inline HOTRowexChildPointer::HOTRowexChildPointer(hot::commons::NodeType nodeAlgorithmType, HOTRowexNodeBase const *node)
	: mPointer((reinterpret_cast<intptr_t>(node) | static_cast<intptr_t>(nodeAlgorithmType)) << 1) {
}

inline HOTRowexChildPointer::HOTRowexChildPointer(intptr_t leafValue)
	: mPointer((leafValue << 1) | 1) {
}

inline HOTRowexChildPointer & HOTRowexChildPointer::operator=(const HOTRowexChildPointer &other) {
	intptr_t otherRawPointer = other.mPointer.load(read_memory_order);
	mPointer.store(otherRawPointer, write_memory_order);
	// by convention, always return *this
	return *this;
}

inline bool HOTRowexChildPointer::operator==(HOTRowexChildPointer const & other) const {
	return (mPointer.load(read_memory_order) == other.mPointer.load(read_memory_order));
}

inline bool HOTRowexChildPointer::operator!=(HOTRowexChildPointer const & other) const {
	intptr_t current = mPointer.load(read_memory_order);
	intptr_t otherRawPointer = other.mPointer.load(read_memory_order);
	return current != otherRawPointer;
}

inline void HOTRowexChildPointer::free() const {
	executeForSpecificNodeType(false, [&](const auto & node) -> void {
		delete &node;
	});
}

constexpr intptr_t NODE_ALGORITH_TYPE_HELPER_EXTRACTION_MASK = NODE_ALGORITHM_TYPE_EXTRACTION_MASK << 1;
inline hot::commons::NodeType HOTRowexChildPointer::getNodeType() const {
	const unsigned int nodeAlgorithmCode = static_cast<unsigned int>(mPointer.load(read_memory_order) & NODE_ALGORITH_TYPE_HELPER_EXTRACTION_MASK);
	return static_cast<hot::commons::NodeType>(nodeAlgorithmCode >> 1u);
}

inline HOTRowexNodeBase* HOTRowexChildPointer::getNode() const {
	intptr_t const nodePointerValue = (mPointer.load(read_memory_order) >> 1) & POINTER_EXTRACTION_MASK;
	return reinterpret_cast<HOTRowexNodeBase *>(nodePointerValue);
}

inline intptr_t HOTRowexChildPointer::getTid() const {
	// The the value stored in the pseudo-leaf
	//normally this is undefined behaviour lookup for intrinsic working only on x86 cpus replace with instruction for arithmetic shift
	return mPointer.load(read_memory_order) >> 1;
}

inline bool HOTRowexChildPointer::isLeaf() const {
	return isLeaf(mPointer.load(read_memory_order));
}

inline intptr_t HOTRowexChildPointer::isLeafInt() const {
	intptr_t isLeaf = mPointer.load(read_memory_order) & 1;
	return isLeaf;
}

inline bool HOTRowexChildPointer::isNode(intptr_t currentPointerValue) const {
	return !isLeaf(currentPointerValue);
}

inline bool HOTRowexChildPointer::isAValidNode() const {
	intptr_t currentNodeValue = mPointer.load(read_memory_order);
	return isNode(currentNodeValue) & (currentNodeValue != reinterpret_cast<intptr_t>(nullptr));
}

inline bool HOTRowexChildPointer::isEmpty() const {
	constexpr intptr_t EMPTY_CHILD_POINTER = reinterpret_cast<intptr_t>(nullptr) << 1;
	intptr_t currentValue = mPointer.load(read_memory_order);
	return currentValue == EMPTY_CHILD_POINTER;
}

inline bool HOTRowexChildPointer::isUsed() const {
	constexpr intptr_t EMPTY_CHILD_POINTER = reinterpret_cast<intptr_t>(nullptr) << 1;
	intptr_t currentValue = mPointer.load(read_memory_order);
	return currentValue != EMPTY_CHILD_POINTER;
}

inline uint16_t HOTRowexChildPointer::getHeight() const {
	return isLeaf() ? 0 : getNode()->mHeight;
}

inline HOTRowexChildPointer const * HOTRowexChildPointer::search(uint8_t const * const & keyBytes) const {
	return executeForSpecificNodeType(true,	[&](const auto & node) {
		return node.search(keyBytes);
	});
}

inline unsigned int HOTRowexChildPointer::getNumberEntries() const {
	return isLeaf() ? 1 : getNode()->getNumberEntries();
}

inline std::set<uint16_t> HOTRowexChildPointer::getDiscriminativeBits() const {
	return executeForSpecificNodeType(false, [&](const auto & node) {
		return node.mDiscriminativeBitsRepresentation.getDiscriminativeBits();
	});
}

inline HOTRowexChildPointer HOTRowexChildPointer::getSmallestLeafValueInSubtree() const {
	return isLeaf() ? *this : getNode()->getPointers()[0].getSmallestLeafValueInSubtree();
}

inline HOTRowexChildPointer HOTRowexChildPointer::getLargestLeafValueInSubtree() const {
	return isLeaf() ? *this : getNode()->getPointers()[getNode()->getNumberEntries() - 1].getLargestLeafValueInSubtree();
}

bool HOTRowexChildPointer::compareAndSwap(HOTRowexChildPointer const & expected, HOTRowexChildPointer const & newValue) {
	intptr_t expectedContainedValue = expected.mPointer.load(std::memory_order_relaxed);
	return mPointer.compare_exchange_strong(expectedContainedValue, newValue.mPointer.load(std::memory_order_relaxed));
}

inline void HOTRowexChildPointer::deleteSubtree() {
	if(isNode() && getNode() != nullptr) {
		executeForSpecificNodeType(true, [](auto & node) -> void {
			for(HOTRowexChildPointer & childPointer : node) {
				childPointer.deleteSubtree();
			}
			delete &node;
		});
	}
}

inline bool HOTRowexChildPointer::isLeaf(intptr_t currentPointerValue) const {
	return currentPointerValue & 1;
}

inline bool HOTRowexChildPointer::isNode() const {
	return isNode(mPointer.load(read_memory_order));
}

}}

#endif
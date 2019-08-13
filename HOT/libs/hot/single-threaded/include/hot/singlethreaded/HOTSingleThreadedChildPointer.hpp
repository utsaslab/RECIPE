#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_CHILD_POINTER__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_CHILD_POINTER__

#include <hot/commons/NodeParametersMapping.hpp>
#include <hot/commons/NodeType.hpp>

#include "hot/singlethreaded/HOTSingleThreadedChildPointerInterface.hpp"
#include "hot/singlethreaded/HOTSingleThreadedNode.hpp"

namespace hot { namespace singlethreaded {

constexpr intptr_t NODE_ALGORITHM_TYPE_EXTRACTION_MASK = 0x7u;
constexpr intptr_t POINTER_AND_IS_LEAF_VALUE_MASK = 15u;
constexpr intptr_t POINTER_EXTRACTION_MASK = ~(POINTER_AND_IS_LEAF_VALUE_MASK);

template<hot::commons::NodeType  nodeAlgorithmType> inline auto HOTSingleThreadedChildPointer::castToNode(HOTSingleThreadedNodeBase const * node) {
	using DiscriminativeBitsRepresentationType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyMappingType;
	using PartialKeyType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyType;
	return reinterpret_cast<HOTSingleThreadedNode<DiscriminativeBitsRepresentationType, PartialKeyType> const *>(node);
}

template<hot::commons::NodeType  nodeAlgorithmType> inline auto HOTSingleThreadedChildPointer::castToNode(HOTSingleThreadedNodeBase * node) {
	using DiscriminativeBitsRepresentationType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyMappingType;
	using PartialKeyType = typename hot::commons::NodeTypeToNodeParameters<nodeAlgorithmType>::PartialKeyType;
	return reinterpret_cast<HOTSingleThreadedNode<DiscriminativeBitsRepresentationType, PartialKeyType> *>(node);
}

template<typename Operation> inline __attribute__((always_inline))
    auto HOTSingleThreadedChildPointer::executeForSpecificNodeType(bool const withPrefetch, Operation const & operation) const {
	HOTSingleThreadedNodeBase const * node = getNode();

	if(withPrefetch) {
		__builtin_prefetch(node);
		__builtin_prefetch(reinterpret_cast<char const*>(node) + 64);
		__builtin_prefetch(reinterpret_cast<char const*>(node) + 128);
		__builtin_prefetch(reinterpret_cast<char const*>(node) + 192);
	}

	switch(getNodeType()) {
		case hot::commons::NodeType ::SINGLE_MASK_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::SINGLE_MASK_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::SINGLE_MASK_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::SINGLE_MASK_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::SINGLE_MASK_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::SINGLE_MASK_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		default: //hot::commons::NodeType ::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
	}
}

template<typename Operation> inline __attribute__((always_inline)) auto HOTSingleThreadedChildPointer::executeForSpecificNodeType(bool const withPrefetch, Operation const & operation) {
	HOTSingleThreadedNodeBase * node = getNode();

	if(withPrefetch) {
		__builtin_prefetch(node);
		__builtin_prefetch(reinterpret_cast<char*>(node) + 64);
		__builtin_prefetch(reinterpret_cast<char*>(node) + 128);
		__builtin_prefetch(reinterpret_cast<char*>(node) + 192);
	}

	switch(getNodeType()) {
		case hot::commons::NodeType ::SINGLE_MASK_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::SINGLE_MASK_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::SINGLE_MASK_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::SINGLE_MASK_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::SINGLE_MASK_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::SINGLE_MASK_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
		case hot::commons::NodeType ::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType ::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS>(node));
		default: //hot::commons::NodeType ::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return operation(*castToNode<hot::commons::NodeType::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS>(node));
	}
}

inline HOTSingleThreadedChildPointer::HOTSingleThreadedChildPointer() : mPointer(reinterpret_cast<intptr_t>(nullptr)) {
}

inline HOTSingleThreadedChildPointer::HOTSingleThreadedChildPointer(HOTSingleThreadedChildPointer const & other)
	: mPointer(other.mPointer)
{
}

inline HOTSingleThreadedChildPointer::HOTSingleThreadedChildPointer(hot::commons::NodeType  nodeAlgorithmType, HOTSingleThreadedNodeBase const *node)
	: mPointer((reinterpret_cast<intptr_t>(node) | static_cast<intptr_t>(nodeAlgorithmType)) << 1) {
}

inline HOTSingleThreadedChildPointer::HOTSingleThreadedChildPointer(intptr_t leafValue)
	: mPointer((leafValue << 1) | 1) {
}

inline HOTSingleThreadedChildPointer & HOTSingleThreadedChildPointer::operator=(const HOTSingleThreadedChildPointer &other) {
	mPointer = other.mPointer;
	// by convention, always return *this
	return *this;
}

inline bool HOTSingleThreadedChildPointer::operator==(HOTSingleThreadedChildPointer const & other) const {
	return (mPointer == other.mPointer);
}

inline bool HOTSingleThreadedChildPointer::operator!=(HOTSingleThreadedChildPointer const & other) const {
	return (mPointer != other.mPointer);
}

inline void HOTSingleThreadedChildPointer::free() const {
	executeForSpecificNodeType(false, [&](const auto & node) -> void {
		delete &node;
	});
}

constexpr intptr_t NODE_ALGORITH_TYPE_HELPER_EXTRACTION_MASK = NODE_ALGORITHM_TYPE_EXTRACTION_MASK << 1;
inline hot::commons::NodeType  HOTSingleThreadedChildPointer::getNodeType() const {
	const unsigned int nodeAlgorithmCode = static_cast<unsigned int>(mPointer & NODE_ALGORITH_TYPE_HELPER_EXTRACTION_MASK);
	return static_cast<hot::commons::NodeType >(nodeAlgorithmCode >> 1u);
}

inline HOTSingleThreadedNodeBase* HOTSingleThreadedChildPointer::getNode() const {
	intptr_t const nodePointerValue = (mPointer >> 1) & POINTER_EXTRACTION_MASK;
	return reinterpret_cast<HOTSingleThreadedNodeBase *>(nodePointerValue);
}

inline intptr_t HOTSingleThreadedChildPointer::getTid() const {
	// The the value stored in the pseudo-leaf
	//normally this is undefined behaviour lookup for intrinsic working only on x86 cpus replace with instruction for arithmetic shift
	return mPointer >> 1;
}

inline bool HOTSingleThreadedChildPointer::isLeaf() const {
	return mPointer & 1;
}

inline bool HOTSingleThreadedChildPointer::isNode() const {
	return !isLeaf();
}

inline bool HOTSingleThreadedChildPointer::isAValidNode() const {
	return isNode() & (mPointer != reinterpret_cast<intptr_t>(nullptr));
}

inline bool HOTSingleThreadedChildPointer::isUnused() const {
	return (!isLeaf()) & (getNode() == nullptr);
}

inline uint16_t HOTSingleThreadedChildPointer::getHeight() const {
	return isLeaf() ? 0 : getNode()->mHeight;
}

template<typename... Args> inline __attribute__((always_inline)) auto HOTSingleThreadedChildPointer::search(Args... args) const {
	return executeForSpecificNodeType(true,	[&](const auto & node) {
		return node.search(args...);
	});
}

inline unsigned int HOTSingleThreadedChildPointer::getNumberEntries() const {
	return isLeaf() ? 1 : getNode()->getNumberEntries();
}

inline std::set<uint16_t> HOTSingleThreadedChildPointer::getDiscriminativeBits() const {
	return executeForSpecificNodeType(false, [&](const auto & node) {
		return node.mDiscriminativeBitsRepresentation.getDiscriminativeBits();
	});
}

inline HOTSingleThreadedChildPointer HOTSingleThreadedChildPointer::getSmallestLeafValueInSubtree() const {
	return isLeaf() ? *this : getNode()->getPointers()[0].getSmallestLeafValueInSubtree();
}

inline HOTSingleThreadedChildPointer HOTSingleThreadedChildPointer::getLargestLeafValueInSubtree() const {
	return isLeaf() ? *this : getNode()->getPointers()[getNode()->getNumberEntries() - 1].getLargestLeafValueInSubtree();
}

inline void HOTSingleThreadedChildPointer::deleteSubtree() {
	if(isNode() && getNode() != nullptr) {
		executeForSpecificNodeType(true, [](auto & node) -> void {
			for(HOTSingleThreadedChildPointer & childPointer : node) {
				childPointer.deleteSubtree();
			}
			delete &node;
		});
	}
}

} }

#endif

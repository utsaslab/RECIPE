#ifndef __HOT__COMMONS__NODE_PARAMETERS_MAPPING__
#define __HOT__COMMONS__NODE_PARAMETERS_MAPPING__

#include "hot/commons/NodeType.hpp"
#include "hot/commons/MultiMaskPartialKeyMappingInterface.hpp"
#include "hot/commons/SingleMaskPartialKeyMappingInterface.hpp"

namespace hot { namespace commons {

template<typename DiscriminativeBitsRepresentation, typename PartialKeyType> struct NodeParametersToNodeType {
};

template<> struct NodeParametersToNodeType<SingleMaskPartialKeyMapping, uint8_t> {
	static constexpr NodeType  mNodeType { NodeType ::SINGLE_MASK_8_BIT_PARTIAL_KEYS };
};

template<> struct NodeParametersToNodeType<SingleMaskPartialKeyMapping, uint16_t> {
	static constexpr NodeType  mNodeType { NodeType ::SINGLE_MASK_16_BIT_PARTIAL_KEYS };
};

template<> struct NodeParametersToNodeType<SingleMaskPartialKeyMapping, uint32_t> {
	static constexpr NodeType  mNodeType { NodeType ::SINGLE_MASK_32_BIT_PARTIAL_KEYS };
};

template<> struct NodeParametersToNodeType<MultiMaskPartialKeyMapping<1u>, uint8_t> {
	static constexpr NodeType  mNodeType { NodeType ::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS };
};

template<> struct NodeParametersToNodeType<MultiMaskPartialKeyMapping<1u>, uint16_t> {
	static constexpr NodeType  mNodeType { NodeType ::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS };
};

template<> struct NodeParametersToNodeType<MultiMaskPartialKeyMapping<1u>, uint32_t> {
	static constexpr NodeType  mNodeType { NodeType ::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS };
};

template<> struct NodeParametersToNodeType<MultiMaskPartialKeyMapping<2u>, uint16_t> {
	static constexpr NodeType  mNodeType { NodeType ::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS };
};

template<> struct NodeParametersToNodeType<MultiMaskPartialKeyMapping<4u>, uint32_t> {
	static constexpr NodeType  mNodeType { NodeType ::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS };
};

template<NodeType  nodeType> struct NodeTypeToNodeParameters {
};

template<> struct NodeTypeToNodeParameters<NodeType ::SINGLE_MASK_8_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = SingleMaskPartialKeyMapping;
	using PartialKeyType = uint8_t;
};

template<> struct NodeTypeToNodeParameters<NodeType ::SINGLE_MASK_16_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = SingleMaskPartialKeyMapping;
	using PartialKeyType = uint16_t;
};

template<> struct NodeTypeToNodeParameters<NodeType ::SINGLE_MASK_32_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = SingleMaskPartialKeyMapping;
	using PartialKeyType = uint32_t;
};

template<> struct NodeTypeToNodeParameters<NodeType ::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = MultiMaskPartialKeyMapping<1u>;
	using PartialKeyType = uint8_t;
};

template<> struct NodeTypeToNodeParameters<NodeType ::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = MultiMaskPartialKeyMapping<1u>;
	using PartialKeyType = uint16_t;
};

template<> struct NodeTypeToNodeParameters<NodeType ::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = MultiMaskPartialKeyMapping<1u>;
	using PartialKeyType = uint32_t;
};

template<> struct NodeTypeToNodeParameters<NodeType ::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = MultiMaskPartialKeyMapping<2u>;
	using PartialKeyType = uint16_t;
};

template<> struct NodeTypeToNodeParameters<NodeType ::MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS> {
	using PartialKeyMappingType = MultiMaskPartialKeyMapping<4u>;
	using PartialKeyType = uint32_t;
};

}}

#endif
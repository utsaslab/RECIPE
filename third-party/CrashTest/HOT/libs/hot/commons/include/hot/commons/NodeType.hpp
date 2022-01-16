#ifndef __HOT__COMMONS__NODE_TYPE__
#define __HOT__COMMONS__NODE_TYPE__

namespace hot { namespace commons {

enum class NodeType : unsigned int {
	SINGLE_MASK_8_BIT_PARTIAL_KEYS = 0,
	SINGLE_MASK_16_BIT_PARTIAL_KEYS = 1,
	SINGLE_MASK_32_BIT_PARTIAL_KEYS = 2,
	MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS = 3,
	MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS = 4,
	MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS = 5,
	MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS = 6,
	MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS = 7
};

inline NodeType getRandomNodeType(uint numberKeyBits, uint numberRandomBytes) {
	//log2 + shift right ---> numberBytes 0-7 > 0, numberBytes 7 - 16 > 1, numberBytes 16 - 32 > 2
	uint32_t numberMaskBytes = (32 - __builtin_clz((numberKeyBits - 1)/8));

	//log2 + shift right ---> numberBytes 0-7 > 0, numberBytes 7 - 16 > 1, numberBytes 16 - 32 > 2
	uint32_t numberExtractionMasks = (32 - __builtin_clz((numberRandomBytes-1)/8));

	uint moreThanASingleExtractionMask = (numberExtractionMasks > 0);

	//because random masks start at 3 + numberMaskBytes (counted from 0) see commend in first line of this function
	// + in case the numberExtractionMasks is larger than 8 the number of mask bits determine the number of of random bytes
	// Hence adding if the mask size is 16 (4) adding 2 will result in 6 which is for MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS. This mus be correct as the number of random bytes can never be larger than the number of key bits
	// Therefore in the case of mask size 32 (5) adding 2 will result in 7 which will be correct in cases where the number of extraction bytes is below 16 as well as above 16 because in both cases the only matching extraction mask is MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS
	return static_cast<NodeType>(3 + numberMaskBytes + (moreThanASingleExtractionMask * 2));
}

inline std::string nodeAlgorithmToString(NodeType nodeAlgorithmType) {
	switch(nodeAlgorithmType) {
		case NodeType::SINGLE_MASK_8_BIT_PARTIAL_KEYS:
			return { "SINGLE_MASK_8_BIT_PARTIAL_KEYS " };
		case NodeType::SINGLE_MASK_16_BIT_PARTIAL_KEYS:
			return { "SINGLE_MASK_16_BIT_PARTIAL_KEYS " };
		case NodeType::SINGLE_MASK_32_BIT_PARTIAL_KEYS:
			return { "SINGLE_MASK_32_BIT_PARTIAL_KEYS " };
		case NodeType::MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS:
			return { "MULTI_MASK_8_BYTES_AND_8_BIT_PARTIAL_KEYS " };
		case NodeType::MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return { "MULTI_MASK_8_BYTES_AND_16_BIT_PARTIAL_KEYS " };
		case NodeType::MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return { "MULTI_MASK_8_BYTES_AND_32_BIT_PARTIAL_KEYS " };
		case NodeType::MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS:
			return { "MULTI_MASK_16_BYTES_AND_16_BIT_PARTIAL_KEYS " };
		default: // MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS:
			return { "MULTI_MASK_32_BYTES_AND_32_BIT_PARTIAL_KEYS " };
	}
}

}}

#endif
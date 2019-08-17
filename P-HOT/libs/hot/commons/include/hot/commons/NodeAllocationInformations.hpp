#ifndef __HOT__COMMONS__NODE_ALLOCATION_INFORMATIONS__
#define __HOT__COMMONS__NODE_ALLOCATION_INFORMATIONS__

#include <array>

#include "hot/commons/NodeAllocationInformation.hpp"

namespace hot { namespace commons {

constexpr size_t MAXIMUM_NUMBER_NODE_ENTRIES = 32;


template<typename NodeType> class NodeAllocationInformations {
	static std::array<NodeAllocationInformation, MAXIMUM_NUMBER_NODE_ENTRIES> mAllocationInformations;

public:
	static inline NodeAllocationInformation const & getAllocationInformation(size_t numberEntries);
};


template<typename NodeTypename> std::array<NodeAllocationInformation, MAXIMUM_NUMBER_NODE_ENTRIES> NodeAllocationInformations<NodeTypename>::mAllocationInformations {
	NodeTypename::getNodeAllocationInformation(1),
	NodeTypename::getNodeAllocationInformation(2),
	NodeTypename::getNodeAllocationInformation(3),
	NodeTypename::getNodeAllocationInformation(4),
	NodeTypename::getNodeAllocationInformation(5),
	NodeTypename::getNodeAllocationInformation(6),
	NodeTypename::getNodeAllocationInformation(7),
	NodeTypename::getNodeAllocationInformation(8),
	NodeTypename::getNodeAllocationInformation(9),
	NodeTypename::getNodeAllocationInformation(10),
	NodeTypename::getNodeAllocationInformation(11),
	NodeTypename::getNodeAllocationInformation(12),
	NodeTypename::getNodeAllocationInformation(13),
	NodeTypename::getNodeAllocationInformation(14),
	NodeTypename::getNodeAllocationInformation(15),
	NodeTypename::getNodeAllocationInformation(16),
	NodeTypename::getNodeAllocationInformation(17),
	NodeTypename::getNodeAllocationInformation(18),
	NodeTypename::getNodeAllocationInformation(19),
	NodeTypename::getNodeAllocationInformation(20),
	NodeTypename::getNodeAllocationInformation(21),
	NodeTypename::getNodeAllocationInformation(22),
	NodeTypename::getNodeAllocationInformation(23),
	NodeTypename::getNodeAllocationInformation(24),
	NodeTypename::getNodeAllocationInformation(25),
	NodeTypename::getNodeAllocationInformation(26),
	NodeTypename::getNodeAllocationInformation(27),
	NodeTypename::getNodeAllocationInformation(28),
	NodeTypename::getNodeAllocationInformation(29),
	NodeTypename::getNodeAllocationInformation(30),
	NodeTypename::getNodeAllocationInformation(31),
	NodeTypename::getNodeAllocationInformation(32)
};

template<typename NodeTypename> inline NodeAllocationInformation const & NodeAllocationInformations<NodeTypename>::getAllocationInformation(size_t numberEntries) {
	return mAllocationInformations[numberEntries - 1];
}

}}

#endif
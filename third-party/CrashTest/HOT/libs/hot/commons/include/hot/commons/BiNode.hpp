#ifndef __HOT__COMMONS__BI_NODE___
#define __HOT__COMMONS__BI_NODE___

#include <hot/commons/DiscriminativeBit.hpp>

#include <hot/commons/BiNodeInterface.hpp>

namespace hot { namespace commons {

template<typename ChildPointerType> inline BiNode<ChildPointerType>::BiNode(uint16_t const discriminativeBitIndex, uint16_t const height, ChildPointerType const & left, ChildPointerType const & right) :  mDiscriminativeBitIndex(discriminativeBitIndex), mHeight(height), mLeft(left), mRight(right) {
}

template<typename ChildPointerType> inline BiNode<ChildPointerType> BiNode<ChildPointerType>::createFromExistingAndNewEntry(DiscriminativeBit const & discriminativeBit, ChildPointerType const & existingNode, ChildPointerType const & newEntry) {
    uint16_t newHeight = existingNode.getHeight() + 1u;
    return discriminativeBit.mValue
        ? BiNode<ChildPointerType> { discriminativeBit.mAbsoluteBitIndex, newHeight, existingNode, newEntry }
    : BiNode<ChildPointerType> { discriminativeBit.mAbsoluteBitIndex, newHeight, newEntry, existingNode };
}

}}

#endif
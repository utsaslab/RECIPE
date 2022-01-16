#include <assert.h>
#include <algorithm>
#include "N.h"

namespace ART_ROWEX {

    inline bool N48::insert(uint8_t key, N *n, bool flush) {
        if (compactCount == 48) {
            return false;
        }

        while (true) {
            if (children[compactCount].load() == nullptr)
                break;
            else {
                compactCount++;
                if (compactCount == 48)
                    return false;
            }
        }

        children[compactCount].store(n, flush ? std::memory_order_release : std::memory_order_relaxed);
        if (flush) clflush((char *)&children[compactCount], sizeof(N *), false, true);
        childIndex[key].store(compactCount, flush ? std::memory_order_release : std::memory_order_relaxed);
        if (flush) clflush((char *)&childIndex[key], sizeof(uint8_t), false, true);
        compactCount++;
        count++;
        return true;
    }

    template<class NODE>
    void N48::copyTo(NODE *n) const {
        for (unsigned i = 0; i < 256; i++) {
            uint8_t index = childIndex[i].load();
            if (index != emptyMarker) {
                n->insert(i, children[index], false);
            }
        }
    }

    void N48::change(uint8_t key, N *val) {
        uint8_t index = childIndex[key].load();
        assert(index != emptyMarker);
        children[index].store(val, std::memory_order_release);
        clflush((char *)&children[index], sizeof(N *), false, true);
    }

    N *N48::getChild(const uint8_t k) const {
        uint8_t index = childIndex[k].load();
        if (index == emptyMarker) {
            return nullptr;
        } else {
            return children[index].load();
        }
    }

    bool N48::remove(uint8_t k, bool force, bool flush) {
        if (count <= 12 && !force) {
            return false;
        }
        assert(childIndex[k] != emptyMarker);
        children[childIndex[k]].store(nullptr, flush ? std::memory_order_release : std::memory_order_relaxed);
        if (flush) clflush((char *)&children[childIndex[k]], sizeof(N *), false, true);
        childIndex[k].store(emptyMarker, flush ? std::memory_order_release : std::memory_order_relaxed);
        if (flush) clflush((char *)&childIndex[k], sizeof(uint8_t), false, true);
        count--;
        assert(getChild(k) == nullptr);
        return true;
    }

    N *N48::getAnyChild() const {
        N *anyChild = nullptr;
        for (unsigned i = 0; i < 48; i++) {
            N *child = children[i].load();
            if (child != nullptr) {
                if (N::isLeaf(child)) {
                    return child;
                }
                anyChild = child;
            }
        }
        return anyChild;
    }

    void N48::deleteChildren() {
        for (unsigned i = 0; i < 256; i++) {
            if (childIndex[i] != emptyMarker) {
                N::deleteChildren(children[childIndex[i]]);
                N::deleteNode(children[childIndex[i]]);
            }
        }
    }

    void N48::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
        childrenCount = 0;
        for (unsigned i = start; i <= end; i++) {
            uint8_t index = this->childIndex[i].load();
            if (index != emptyMarker) {
                N *child = this->children[index].load();
                if (child != nullptr) {
                    children[childrenCount] = std::make_tuple(i, child);
                    childrenCount++;
                }
            }
        }
    }

    uint32_t N48::getCount() const {
        uint32_t cnt = 0;
        for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
            uint8_t index = childIndex[i].load();
            if (index != emptyMarker)
                cnt++;
        }
        return cnt;
    }
}
#include <assert.h>
#include <algorithm>
#include "N.h"

namespace ART_ROWEX {

    void N256::deleteChildren() {
        for (uint64_t i = 0; i < 256; ++i) {
            if (children[i] != nullptr) {
                N::deleteChildren(children[i]);
                N::deleteNode(children[i]);
            }
        }
    }

    inline bool N256::insert(uint8_t key, N *val, bool flush) {
        if (count.load(std::memory_order_acquire) >= 256) {
            return false;
        }

        if (flush) movnt64((uint64_t *)&children[key], (uint64_t)val, false, true);
        else children[key].store(val, std::memory_order_relaxed);
        count.fetch_add(1, std::memory_order_acq_rel);

        return true;
    }

    template<class NODE>
    void N256::copyTo(NODE *n) const {
        for (int i = 0; i < 256; ++i) {
            N *child = children[i].load();
            if (child != nullptr) {
                n->insert(i, child, false);
            }
        }
    }

    void N256::change(uint8_t key, N *n) {
        movnt64((uint64_t *)&children[key], (uint64_t)n, false, true);
    }

    N *N256::getChild(const uint8_t k) const {
        return children[k].load();
    }

    bool N256::remove(uint8_t k, bool force, bool flush) {
        if (count.load(std::memory_order_acquire) <= 37 && !force) {
            return false;
        }

        if (flush)
            movnt64((uint64_t *)&children[k], (uint64_t)nullptr, false, true);
        else
            children[k].store(nullptr, std::memory_order_relaxed);

        count.fetch_sub(1, std::memory_order_acq_rel);
        return true;
    }

    N *N256::getAnyChild() const {
        N *anyChild = nullptr;
        for (uint64_t i = 0; i < 256; ++i) {
            N *child = children[i].load();
            if (child != nullptr) {
                if (N::isLeaf(child)) {
                    return child;
                } else {
                    anyChild = child;
                }
            }
        }
        return anyChild;
    }

    void N256::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                           uint32_t &childrenCount) const {
        childrenCount = 0;
        for (unsigned i = start; i <= end; i++) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(i, child);
                childrenCount++;
            }
        }
    }

    uint32_t N256::getCount() const {
        uint32_t cnt = 0;
        for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
            N *child = children[i].load();
            if (child != nullptr)
                cnt++;
        }
        return cnt;
    }
}
#include <assert.h>
#include <algorithm>
#include "N.h"

namespace ART_ROWEX {


    void N4::deleteChildren() {
        for (uint32_t i = 0; i < compactCount.load(std::memory_order_acquire); ++i) {
            if (children[i].load() != nullptr) {
                N::deleteChildren(children[i]);
                N::deleteNode(children[i]);
            }
        }
    }

    inline bool N4::insert(uint8_t key, N *n, bool flush) {
        if (compactCount.load(std::memory_order_acquire) == 4) {
            return false;
        }

        uint16_t nextIndex = compactCount.fetch_add(1, std::memory_order_acq_rel);
        count.fetch_add(1, std::memory_order_acq_rel);

        if (flush) {
            keys[nextIndex].store(key, std::memory_order_release);
            clflush((char *)this, sizeof(N4), false, true);
            movnt64((uint64_t *)&children[nextIndex], (uint64_t)n, false, true);
        } else {
            keys[nextIndex].store(key, std::memory_order_relaxed);
            children[nextIndex].store(n, std::memory_order_relaxed);
        }

        return true;
    }

    template<class NODE>
    void N4::copyTo(NODE *n) const {
        for (uint32_t i = 0; i < compactCount.load(std::memory_order_acquire); ++i) {
            N *child = children[i].load();
            if (child != nullptr) {
                n->insert(keys[i].load(), child, false);
            }
        }
    }

    void N4::change(uint8_t key, N *val) {
        for (uint32_t i = 0; i < compactCount.load(std::memory_order_acquire); ++i) {
            N *child = children[i].load();
            if (child != nullptr && keys[i].load() == key) {
                movnt64((uint64_t *)&children[i], (uint64_t) val, false, true);
                return ;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    N *N4::getChild(const uint8_t k) const {
        for (uint32_t i = 0; i < 4; ++i) {
            N *child = children[i].load();
            if (child != nullptr && keys[i].load() == k) {
                return child;
            }
        }
        return nullptr;
    }

    bool N4::remove(uint8_t k, bool force, bool flush) {
        for (uint32_t i = 0; i < compactCount.load(std::memory_order_acquire); ++i) {
            if (children[i] != nullptr && keys[i].load() == k) {
                if (flush) movnt64((uint64_t *)&children[i], (uint64_t)nullptr, false, true);
                else children[i].store(nullptr, std::memory_order_relaxed);
                count.fetch_sub(1, std::memory_order_acq_rel);
                return true;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    N *N4::getAnyChild() const {
        N *anyChild = nullptr;
        for (uint32_t i = 0; i < 4; ++i) {
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

    std::tuple<N *, uint8_t> N4::getSecondChild(const uint8_t key) const {
        for (uint32_t i = 0; i < compactCount.load(std::memory_order_acquire); ++i) {
            N *child = children[i].load();
            if (child != nullptr) {
                uint8_t k = keys[i].load();
                if (k != key){
                    return std::make_tuple(child, k);
                }
            }
        }
        return std::make_tuple(nullptr, 0);
    }

    void N4::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                         uint32_t &childrenCount) const {
        childrenCount = 0;
        for (uint32_t i = 0; i < 4; ++i) {
            uint8_t key = this->keys[i].load();
            if (key >= start && key <= end) {
                N *child = this->children[i].load();
                if (child != nullptr) {
                    children[childrenCount] = std::make_tuple(key, child);
                    childrenCount++;
                }
            }
        }
        std::sort(children, children + childrenCount, [](auto &first, auto &second) {
            return std::get<0>(first) < std::get<0>(second);
        });
    }

    uint32_t N4::getCount() const {
        uint32_t cnt = 0;
        for (uint32_t i = 0; i < compactCount.load(std::memory_order_acquire) && cnt < 3; i++) {
            N *child = children[i].load();
            if (child != nullptr)
                cnt++;
        }
        return cnt;
    }
}
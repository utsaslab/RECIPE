#include <assert.h>
#include <algorithm>
#include "N.h"
#include <emmintrin.h> // x86 SSE intrinsics

namespace ART_ROWEX {

    inline bool N16::insert(uint8_t key, N *n, bool flush) {
        if (compactCount.load(std::memory_order_acquire) == 16) {
            return false;
        }

        uint16_t nextIndex = compactCount.fetch_add(1, std::memory_order_acq_rel);
        count.fetch_add(1, std::memory_order_acq_rel);

        if (flush) {
            keys[nextIndex].store(flipSign(key), std::memory_order_release);
            // this clflush will failure-atomically flush the cache line including counters and entire key entries
            clflush((char *)this, sizeof(uintptr_t), false, true);
            movnt64((uint64_t *)&children[nextIndex], (uint64_t)n, false, true);
        } else {
            keys[nextIndex].store(flipSign(key), std::memory_order_relaxed);
            children[nextIndex].store(n, std::memory_order_relaxed);
        }

        return true;
    }

    template<class NODE>
    void N16::copyTo(NODE *n) const {
        for (unsigned i = 0; i < compactCount.load(std::memory_order_acquire); i++) {
            N *child = children[i].load();
            if (child != nullptr) {
                n->insert(flipSign(keys[i]), child, false);
            }
        }
    }

    void N16::change(uint8_t key, N *val) {
        auto childPos = getChildPos(key);
        assert(childPos != nullptr);
        movnt64((uint64_t *)childPos, (uint64_t)val, false, true);
    }

    std::atomic<N *> *N16::getChildPos(const uint8_t k) {
        __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                                     _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
        unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount.load(std::memory_order_acquire)) - 1);
        while (bitfield) {
            uint8_t pos = ctz(bitfield);

            if (children[pos].load() != nullptr) {
                return &children[pos];
            }
            bitfield = bitfield ^ (1 << pos);
        }
        return nullptr;
    }

    N *N16::getChild(const uint8_t k) const {
        __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                                     _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
        unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
        while (bitfield) {
            uint8_t pos = ctz(bitfield);

            N *child = children[pos].load();
            if (child != nullptr && keys[pos].load() == flipSign(k)) {
                return child;
            }
            bitfield = bitfield ^ (1 << pos);
        }
        return nullptr;
    }

    bool N16::remove(uint8_t k, bool force, bool flush) {
        if (count.load(std::memory_order_acquire) <= 3 && !force) {
            return false;
        }
        auto leafPlace = getChildPos(k);
        assert(leafPlace != nullptr);

        if (flush)
            movnt64((uint64_t *)leafPlace, (uint64_t)nullptr, false, true);
        else
            leafPlace->store(nullptr, std::memory_order_relaxed);

        count.fetch_sub(1, std::memory_order_acq_rel);
        assert(getChild(k) == nullptr);
        return true;
    }

    N *N16::getAnyChild() const {
        N *anyChild = nullptr;
        for (int i = 0; i < 16; ++i) {
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

    void N16::deleteChildren() {
        for (std::size_t i = 0; i < compactCount.load(std::memory_order_acquire); ++i) {
            if (children[i].load() != nullptr) {
                N::deleteChildren(children[i]);
                N::deleteNode(children[i]);
            }
        }
    }

    void N16::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
        childrenCount = 0;
        for (int i = 0; i < compactCount.load(std::memory_order_acquire); ++i) {
            uint8_t key = flipSign(this->keys[i]);
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

    uint32_t N16::getCount() const {
        uint32_t cnt = 0;
        for (uint32_t i = 0; i < compactCount.load(std::memory_order_acquire) && cnt < 3; i++) {
            N *child = children[i].load();
            if (child != nullptr)
                ++cnt;
        }
        return cnt;
    }
}
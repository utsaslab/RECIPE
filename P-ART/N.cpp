#include <assert.h>
#include <algorithm>

#include "N.h"
#include "N4.cpp"
#include "N16.cpp"
#include "N48.cpp"
#include "N256.cpp"

namespace ART_ROWEX {
    static unsigned long write_latency_in_ns = 0;
    static unsigned long cpu_freq_mhz = 2100;
    static unsigned long cache_line_size = 64;

    static inline void cpu_pause()
    {
        __asm__ volatile ("pause" ::: "memory");
    }

    static inline unsigned long read_tsc(void)
    {
        unsigned long var;
        unsigned int hi, lo;

        asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
        var = ((unsigned long long int) hi << 32) | lo;

        return var;
    }

    inline void N::mfence()
    {
        asm volatile("mfence":::"memory");
    }

    inline void N::clflush(char *data, int len, bool front, bool back)
    {
        volatile char *ptr = (char *)((unsigned long)data & ~(cache_line_size - 1));
        if (front)
            mfence();
        for (; ptr < data+len; ptr += cache_line_size){
            unsigned long etsc = read_tsc() +
                (unsigned long)(write_latency_in_ns * cpu_freq_mhz/1000);
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
            while (read_tsc() < etsc) cpu_pause();
        }
        if (back)
            mfence();
    }
#ifdef LOCK_INIT
    void lock_initialization () {
        printf("lock table size = %lu\n", lock_initializer.size());
        for (uint64_t i = 0; i < lock_initializer.size(); i++){
            // check if the node is locked
            if (lock_initializer[i]->isLocked(lock_initializer[i]->getVersion()))
                lock_initializer[i]->writeUnlock();
        }
    }
#endif
    void N::setType(NTypes type) {
        typeVersionLockObsolete.fetch_add(convertTypeToVersion(type));
    }

    uint64_t N::convertTypeToVersion(NTypes type) {
        return (static_cast<uint64_t>(type) << 62);
    }

    NTypes N::getType() const {
        return static_cast<NTypes>(typeVersionLockObsolete.load(std::memory_order_relaxed) >> 62);
    }

    uint32_t N::getLevel() const {
        return level;
    }

    void N::writeLockOrRestart(bool &needRestart) {
        uint64_t version;
        do {
            version = typeVersionLockObsolete.load();
            while (isLocked(version)) {
                _mm_pause();
                version = typeVersionLockObsolete.load();
            }
            if (isObsolete(version)) {
                needRestart = true;
                return;
            }
        } while (!typeVersionLockObsolete.compare_exchange_weak(version, version + 0b10));
    }

    void N::lockVersionOrRestart(uint64_t &version, bool &needRestart) {
        if (isLocked(version) || isObsolete(version)) {
            needRestart = true;
            return;
        }
        if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
            version = version + 0b10;
        } else {
            needRestart = true;
        }
    }

    void N::writeUnlock() {
        typeVersionLockObsolete.fetch_add(0b10);
    }

    N *N::getAnyChild(const N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getAnyChild();
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    void N::change(N *node, uint8_t key, N *val) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->change(key, val);
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename biggerN>
    void N::insertGrow(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, N *val, ThreadInfo &threadInfo, bool &needRestart) {
        if (n->insert(key, val, true)) {
            n->writeUnlock();
            return;
        }
        auto nBig = new biggerN(n->getLevel(), n->getPrefi());
        n->copyTo(nBig);
        nBig->insert(key, val, false);
        clflush((char *)nBig, sizeof(biggerN), true, true);

        parentNode->writeLockOrRestart(needRestart);
        if (needRestart) {
            delete nBig;
            n->writeUnlock();
            return;
        }

        N::change(parentNode, keyParent, nBig);
        parentNode->writeUnlock();

        n->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(n, threadInfo);
    }

    template<typename curN>
    void N::insertCompact(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, N *val, ThreadInfo &threadInfo, bool &needRestart) {
        auto nNew = new curN(n->getLevel(), n->getPrefi());
        n->copyTo(nNew);
        nNew->insert(key, val, false);
        clflush((char *)nNew, sizeof(curN), true, true);

        parentNode->writeLockOrRestart(needRestart);
        if (needRestart) {
            delete nNew;
            n->writeUnlock();
            return;
        }

        N::change(parentNode, keyParent, nNew);
        parentNode->writeUnlock();

        n->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(n, threadInfo);
    }

    void N::insertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key, N *val, ThreadInfo &threadInfo, bool &needRestart) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                if (n->compactCount == 4 && n->count <= 3) {
                    insertCompact<N4>(n, parentNode, keyParent, key, val, threadInfo, needRestart);
                    break;
                }
                insertGrow<N4, N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart);
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                if (n->compactCount == 16 && n->count <= 14) {
                    insertCompact<N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart);
                    break;
                }
                insertGrow<N16, N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart);
                break;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                if (n->compactCount == 48 && n->count != 48) {
                    insertCompact<N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart);
                    break;
                }
                insertGrow<N48, N256>(n, parentNode, keyParent, key, val, threadInfo, needRestart);
                break;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->insert(key, val, true);
                node->writeUnlock();
                break;
            }
        }
    }

    N *N::getChild(const uint8_t k, N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getChild(k);
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                return n->getChild(k);
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                return n->getChild(k);
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                return n->getChild(k);
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    void N::deleteChildren(N *node) {
        if (N::isLeaf(node)) {
            return;
        }
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->deleteChildren();
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename smallerN>
    void N::removeAndShrink(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, ThreadInfo &threadInfo, bool &needRestart) {
        if (n->remove(key, parentNode == nullptr, true)) {
            n->writeUnlock();
            return;
        }

        auto nSmall = new smallerN(n->getLevel(), n->getPrefi());

        parentNode->writeLockOrRestart(needRestart);
        if (needRestart) {
            delete nSmall;
            n->writeUnlock();
            return;
        }

        n->remove(key, true, true);
        n->copyTo(nSmall);
        clflush((char *) nSmall, sizeof(smallerN), true, true);
        N::change(parentNode, keyParent, nSmall);

        parentNode->writeUnlock();
        n->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(n, threadInfo);
    }

    void N::removeAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent, ThreadInfo &threadInfo, bool &needRestart) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->remove(key, false, true);
                n->writeUnlock();
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                removeAndShrink<N16, N4>(n, parentNode, keyParent, key, threadInfo, needRestart);
                break;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                removeAndShrink<N48, N16>(n, parentNode, keyParent, key, threadInfo, needRestart);
                break;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                removeAndShrink<N256, N48>(n, parentNode, keyParent, key, threadInfo, needRestart);
                break;
            }
        }
    }

    bool N::isLocked(uint64_t version) const {
        return ((version & 0b10) == 0b10);
    }

    uint64_t N::getVersion() const {
        return typeVersionLockObsolete.load();
    }

    bool N::isObsolete(uint64_t version) {
        return (version & 1) == 1;
    }

    bool N::checkOrRestart(uint64_t startRead) const {
        return readUnlockOrRestart(startRead);
    }

    bool N::readUnlockOrRestart(uint64_t startRead) const {
        return startRead == typeVersionLockObsolete.load();
    }

    uint32_t N::getCount() const {
        switch (this->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(this);
                return n->getCount();
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(this);
                return n->getCount();
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(this);
                return n->getCount();
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(this);
                return n->getCount();
            }
            default: {
                assert(false);
                __builtin_unreachable();
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    Prefix N::getPrefi() const {
        return prefix.load();
    }

    inline void N::setPrefix(const uint8_t *prefix, uint32_t length, bool flush) {
        if (length > 0) {
            Prefix p;
            memcpy(p.prefix, prefix, std::min(length, maxStoredPrefixLength));
            p.prefixCount = length;
            this->prefix.store(p, std::memory_order_release);
        } else {
            Prefix p;
            p.prefixCount = 0;
            this->prefix.store(p, std::memory_order_release);
        }
        if (flush) clflush((char *)&(this->prefix), sizeof(Prefix), false, true);
    }

    void N::addPrefixBefore(N* node, uint8_t key) {
        Prefix p = this->getPrefi();
        Prefix nodeP = node->getPrefi();
        uint32_t prefixCopyCount = std::min(maxStoredPrefixLength, nodeP.prefixCount + 1);
        memmove(p.prefix + prefixCopyCount, p.prefix, std::min(p.prefixCount, maxStoredPrefixLength - prefixCopyCount));
        memcpy(p.prefix, nodeP.prefix, std::min(prefixCopyCount, nodeP.prefixCount));
        if (nodeP.prefixCount < maxStoredPrefixLength) {
            p.prefix[prefixCopyCount - 1] = key;
        }
        p.prefixCount += nodeP.prefixCount + 1;
        this->prefix.store(p, std::memory_order_release);
        clflush((char *)&this->prefix, sizeof(Prefix), false, true);
    }

    bool N::isLeaf(const N *n) {
        return (reinterpret_cast<uintptr_t>(n) & (1ULL << 0));
    }

    N *N::setLeaf(const Key *k) {
        return reinterpret_cast<N *>(reinterpret_cast<void *>((reinterpret_cast<uintptr_t>(k) | (1ULL << 0))));
    }

    Key *N::getLeaf(const N *n) {
        return reinterpret_cast<Key *>(reinterpret_cast<void *>((reinterpret_cast<uintptr_t>(n) & ~(1ULL << 0))));
    }

    std::tuple<N *, uint8_t> N::getSecondChild(N *node, const uint8_t key) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getSecondChild(key);
            }
            default: {
                assert(false);
                __builtin_unreachable();
            }
        }
    }

    void N::deleteNode(N *node) {
        if (N::isLeaf(node)) {
            return;
        }
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                delete n;
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                delete n;
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                delete n;
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                delete n;
                return;
            }
        }
        delete node;
    }

    Key *N::getAnyChildTid(const N *n) {
        const N *nextNode = n;

        while (true) {
            const N *node = nextNode;
            nextNode = getAnyChild(node);

            assert(nextNode != nullptr);
            if (isLeaf(nextNode)) {
                return getLeaf(nextNode);
            }
        }
    }

    void N::getChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                        uint32_t &childrenCount) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
        }
    }
}

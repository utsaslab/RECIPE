#ifndef __HOT__ROWEX__SYNCHRONIZED_ITERATOR__
#define __HOT__ROWEX__SYNCHRONIZED_ITERATOR__

#include <cstdint>
#include <array>

#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/InsertInformation.hpp>

#include <idx/contenthelpers/TidConverters.hpp>
#include <idx/contenthelpers/KeyUtilities.hpp>
#include <idx/contenthelpers/ContentEquals.hpp>

#include "hot/rowex/HOTRowexChildPointer.hpp"
#include "hot/rowex/HOTRowexNodeBase.hpp"
#include "hot/rowex/HOTRowexIteratorBufferState.hpp"
#include "hot/rowex/HOTRowexIteratorEndToken.hpp"
#include "hot/rowex/HOTRowexIteratorStackState.hpp"
#include "hot/rowex/HOTRowexIteratorStackEntry.hpp"
#include "hot/rowex/EpochBasedMemoryReclamationStrategy.hpp"
#include "hot/rowex/MemoryGuard.hpp"

namespace hot { namespace rowex {

using HOTRowexSynchronizedIteratorStackState = HOTRowexIteratorStackState<HOTRowexIteratorStackEntry>;

template<typename ValueType, template <typename> typename KeyExtractor> class HOTRowexSynchronizedIterator {
	static KeyExtractor<ValueType> extractKey;

	using KeyType = decltype(extractKey(std::declval<ValueType>()));
	using FixedSizedKeyType = decltype(idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(KeyType())));

	static HOTRowexSynchronizedIterator<ValueType, KeyExtractor> const END_ITERATOR;

	char mRawNodeStack[sizeof(HOTRowexIteratorStackEntry) * 64];
	char mRawBuffer[sizeof(HOTRowexChildPointer) * HotRowexIteratorBufferState<KeyType>::MAXIMUM_NUMBER_NUMBER_ENTRIES_IN_BUFFER ];
	HOTRowexChildPointer const * mRootPointerLocation;
	EpochBasedMemoryReclamationStrategy* mMemoryReclamationStrategy;
	HotRowexIteratorBufferState<KeyType> mCurrentBufferState;

public:
	static inline HOTRowexSynchronizedIterator begin(HOTRowexChildPointer const * rootPointerLocation, EpochBasedMemoryReclamationStrategy * const & memoryReclamationStrategy) {
		MemoryGuard guard(memoryReclamationStrategy);
		HOTRowexChildPointer rootPointer = *rootPointerLocation;
		return rootPointer.isUsed() ? HOTRowexSynchronizedIterator(rootPointerLocation, rootPointer, memoryReclamationStrategy, guard) : END_ITERATOR;
	}

	static inline HOTRowexSynchronizedIterator find(HOTRowexChildPointer const * rootPointerLocation, KeyType const & searchKey, EpochBasedMemoryReclamationStrategy * const & memoryReclamationStrategy) {
		MemoryGuard guard(memoryReclamationStrategy);
		HOTRowexChildPointer rootPointer = *rootPointerLocation;
		return rootPointer.isUsed() ? HOTRowexSynchronizedIterator(rootPointerLocation, rootPointer, searchKey, memoryReclamationStrategy, guard) : END_ITERATOR;
	}

	static inline HOTRowexSynchronizedIterator const & end() {
		return END_ITERATOR;
	}

	static inline HOTRowexSynchronizedIterator getBounded(HOTRowexChildPointer const * rootPointer, KeyType const & searchKey, bool isLowerBound, EpochBasedMemoryReclamationStrategy * const & memoryReclamationStrategy) {
		MemoryGuard guard(memoryReclamationStrategy);
		HOTRowexChildPointer const & currentRoot = *rootPointer;

		return (currentRoot.isLeaf()
				&& idx::contenthelpers::contentEquals(searchKey, extractKey(idx::contenthelpers::tidToValue<ValueType>(currentRoot.getTid())))
			   ) || currentRoot.isAValidNode()
			   ? HOTRowexSynchronizedIterator(rootPointer, currentRoot, searchKey, isLowerBound, memoryReclamationStrategy, guard)
			   : END_ITERATOR;
	}

	inline HOTRowexSynchronizedIterator(const HOTRowexSynchronizedIterator<ValueType, KeyExtractor> & other) : mRootPointerLocation(other.mRootPointerLocation), mMemoryReclamationStrategy(other.mMemoryReclamationStrategy), mCurrentBufferState(getBufferRoot()) {
		mCurrentBufferState.copy(other.mCurrentBufferState);
	}

private:

	HOTRowexChildPointer const * getBufferRoot()  const {
		return reinterpret_cast<HOTRowexChildPointer const *>(mRawBuffer);
	}

	HOTRowexChildPointer * getBufferRoot()  {
		return reinterpret_cast<HOTRowexChildPointer *>(mRawBuffer);
	}

	inline static int64_t getIteratorStackDepth(HOTRowexIteratorStackEntry const * rootStackEntry, HOTRowexIteratorStackEntry const * currentStackEntry) {
		return currentStackEntry - rootStackEntry;
	}

	inline HOTRowexSynchronizedIterator(HOTRowexChildPointer const * const & rootPointerLocation, HOTRowexChildPointer const & rootPointer, EpochBasedMemoryReclamationStrategy* memoryReclamationStrategy, MemoryGuard const & currentMemoryGuard) //DESCEND or STORE
		: mRootPointerLocation(rootPointerLocation), mMemoryReclamationStrategy(memoryReclamationStrategy), mCurrentBufferState(
			fillBuffer(
				HotRowexIteratorBufferState<KeyType>(getBufferRoot()),
				HOTRowexSynchronizedIteratorStackState({ getStackRoot(), static_cast<int32_t>(ITERATOR_FILL_BUFFER_STATE_DESCEND + rootPointer.isLeafInt()), getStackRoot()->init(rootPointerLocation, rootPointer, rootPointerLocation + 1) }),
				currentMemoryGuard
			)
		)
	{
	}

	HOTRowexIteratorStackEntry* getStackRoot() {
		return reinterpret_cast<HOTRowexIteratorStackEntry*>(mRawNodeStack);
	}

	HOTRowexIteratorStackEntry const * getStackRoot() const {
		return reinterpret_cast<HOTRowexIteratorStackEntry const *>(mRawNodeStack);
	}

	inline HOTRowexSynchronizedIterator(HOTRowexChildPointer const * rootPointerLocation, HOTRowexChildPointer const & rootPointer, KeyType const & searchKey, EpochBasedMemoryReclamationStrategy* memoryReclamationStrategy, MemoryGuard const & currentMemoryGuard)
		: mRootPointerLocation(rootPointerLocation), mMemoryReclamationStrategy(memoryReclamationStrategy), mCurrentBufferState(initializeBufferForKey(getBufferRoot(), getStackRoot(), rootPointerLocation, rootPointer, searchKey, currentMemoryGuard)) {
	}

	inline HOTRowexSynchronizedIterator(HOTRowexChildPointer const * rootPointerLocation, HOTRowexChildPointer const & rootPointer, KeyType const & searchKey, bool isLowerBound, EpochBasedMemoryReclamationStrategy* memoryReclamationStrategy, MemoryGuard const & currentMemoryGuard)
		: mRootPointerLocation(rootPointerLocation), mMemoryReclamationStrategy(memoryReclamationStrategy), mCurrentBufferState(fillBufferForBound(getBufferRoot(), getStackRoot(), rootPointerLocation, rootPointer, searchKey, isLowerBound, currentMemoryGuard)) {
	}

	inline HOTRowexSynchronizedIterator() : mRootPointerLocation(&HOTRowexIteratorEndToken::END_TOKEN), mCurrentBufferState(getBufferRoot()) {
	}

public:

	inline ValueType operator*() const {
		return idx::contenthelpers::tidToValue<ValueType>(mCurrentBufferState.getCurrent().getTid());
	}

	inline HOTRowexSynchronizedIterator<ValueType, KeyExtractor> & operator++() {
		if(mCurrentBufferState.canAdvance()) {
			mCurrentBufferState.advance();
		} else {
			MemoryGuard guard { mMemoryReclamationStrategy };
			const HOTRowexChildPointer currentRoot = *mRootPointerLocation;
			//is Full implies that the end of the data structure was reached
			if(currentRoot.isUsed() & !mCurrentBufferState.endOfDataReached()) {
				mCurrentBufferState = fillBufferForBoundWithByteKey(getBufferRoot(), getStackRoot(), mRootPointerLocation, currentRoot, mCurrentBufferState.getLastAccessedKey(), false, guard);
			} else {
				mCurrentBufferState = { getBufferRoot() };
			}
		}
		return *this;
	}

	inline HOTRowexSynchronizedIterator<ValueType, KeyExtractor>& operator=(HOTRowexSynchronizedIterator<ValueType, KeyExtractor> const & other) {
		mMemoryReclamationStrategy = other.mMemoryReclamationStrategy;
		mRootPointerLocation = other.mRootPointerLocation;
		mCurrentBufferState.copy(other.mCurrentBufferState);
		return *this;
	}

	bool operator==(HOTRowexSynchronizedIterator<ValueType, KeyExtractor> const & other) const {
		return mCurrentBufferState.getCurrent() == other.mCurrentBufferState.getCurrent();
	}

	bool operator!=(HOTRowexSynchronizedIterator<ValueType, KeyExtractor> const & other) const {
		return mCurrentBufferState.getCurrent() != other.mCurrentBufferState.getCurrent();
	}

private:
	static inline HotRowexIteratorBufferState<KeyType> initializeBufferForKey(HOTRowexChildPointer * const & bufferRoot, HOTRowexIteratorStackEntry * const rootStackEntry, HOTRowexChildPointer const * rootPointerLocation, HOTRowexChildPointer const & rootPointer, KeyType const & searchKey, MemoryGuard const & guard) {
		HOTRowexIteratorStackEntry* currentStackEntry = rootStackEntry;
		FixedSizedKeyType const & fixedSizedKey = toFixedSizedKey(searchKey);
		uint8_t const* searchKeyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizedKey);
		currentStackEntry->init(rootPointerLocation, rootPointer, rootPointerLocation + 1);

		while(!currentStackEntry->getCurrent().isLeaf()) {
			currentStackEntry = currentStackEntry->getCurrent().executeForSpecificNodeType(true, [&](auto & node) -> HOTRowexIteratorStackEntry* {
				return (currentStackEntry + 1)->init(node.search(searchKeyBytes), node.end());
			});
		}

		assert(getIteratorStackDepth(rootStackEntry, currentStackEntry) >= 0);

		KeyType const & foundKey = getKeyForStackEntry(currentStackEntry);
		return (idx::contenthelpers::contentEquals(foundKey, searchKey))
			   ? fillBuffer({ bufferRoot } , {rootStackEntry, ITERATOR_FILL_BUFFER_STATE_STORE, currentStackEntry }, guard)
			   : HotRowexIteratorBufferState<KeyType>(bufferRoot);
	}

	static inline int32_t ascendOrAdvance(HOTRowexIteratorStackEntry const * const currentStackEntry) {
		return (currentStackEntry->isLastElement()) ? ITERATOR_FILL_BUFFER_STATE_ASCEND : ITERATOR_FILL_BUFFER_STATE_ADVANCE;
	}

	static inline int32_t descendOrStore(HOTRowexIteratorStackEntry const * const currentStackEntry) {
		//this distinguishes between STORE and DESCEND which are successive states
		return currentStackEntry->getCurrent().isLeafInt();
	}

	static inline HotRowexIteratorBufferState<KeyType> fillBufferForBound(HOTRowexChildPointer * const & bufferRoot, HOTRowexIteratorStackEntry * currentStackRoot, HOTRowexChildPointer const * rootPointerLocation, HOTRowexChildPointer const & rootPointer, KeyType const & searchKey, bool isLowerBound, MemoryGuard const &guard) {
		FixedSizedKeyType const & fixedSizedKeyType = toFixedSizedKey(searchKey);
		return fillBufferForBoundWithByteKey(bufferRoot, currentStackRoot, rootPointerLocation, rootPointer, idx::contenthelpers::interpretAsByteArray(fixedSizedKeyType), isLowerBound, guard);
	}

	static inline HotRowexIteratorBufferState<KeyType> fillBufferForBoundWithByteKey(HOTRowexChildPointer * const & bufferRoot, HOTRowexIteratorStackEntry * currentStackRoot, HOTRowexChildPointer const * rootPointerLocation, HOTRowexChildPointer const & rootPointer, uint8_t const* searchKeyBytes, bool isLowerBound, MemoryGuard const &guard) {
		HOTRowexIteratorStackEntry * currentStackEntry = currentStackRoot;
		currentStackEntry->init(rootPointerLocation, rootPointer, rootPointerLocation + 1);

		std::array<uint16_t, 64> mMostSignificantBitIndexes;
		uint16_t* mMostSignificantBitIndex = mMostSignificantBitIndexes.data();

		while(!currentStackEntry->getCurrent().isLeaf()) {
			HOTRowexIteratorStackEntry * nextStackEntry = currentStackEntry + 1;
			currentStackEntry->getCurrent().executeForSpecificNodeType(true, [&](auto & node) -> void {
				*mMostSignificantBitIndex = node.mDiscriminativeBitsRepresentation.mMostSignificantDiscriminativeBitIndex;
				nextStackEntry->init(node.search(searchKeyBytes), node.end());
			});
			++mMostSignificantBitIndex;
			currentStackEntry = nextStackEntry;
		}

		FixedSizedKeyType const & existingFixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(getKeyForStackEntry(currentStackEntry)));
		uint8_t const* existingKeyBytes = idx::contenthelpers::interpretAsByteArray(existingFixedSizeKey);

		idx::contenthelpers::OptionalValue<hot::commons::DiscriminativeBit> mismatchingBit = hot::commons::getMismatchingBit(existingKeyBytes, searchKeyBytes, idx::contenthelpers::getMaxKeyLength<KeyType>());

		if(mismatchingBit.mIsValid) {
			HOTRowexChildPointer const * childPointerLocation = currentStackEntry->getCurrentPointerLocation();
			--mMostSignificantBitIndex;
			--currentStackEntry;
			unsigned int entryIndex = childPointerLocation - currentStackEntry->getCurrent().getNode()->getPointers();
			while (getIteratorStackDepth(currentStackRoot, currentStackEntry) > 0 && mismatchingBit.mValue.mAbsoluteBitIndex < *mMostSignificantBitIndex) {
				HOTRowexIteratorStackEntry* parentStackEntry = currentStackEntry - 1;
				entryIndex = currentStackEntry->getCurrentPointerLocation() - parentStackEntry->getCurrent().getNode()->getPointers();
				currentStackEntry = parentStackEntry;
				--mMostSignificantBitIndex;
			}
			return fillBuffer({ bufferRoot }, currentStackEntry->getCurrent().executeForSpecificNodeType(false, [&](auto const &existingNode) -> HOTRowexSynchronizedIteratorStackState {
				hot::commons::InsertInformation const & insertInformation = existingNode.getInsertInformation(entryIndex, mismatchingBit.mValue);

				unsigned int nextEntryIndex = insertInformation.mKeyInformation.mValue
											  ? (insertInformation.getFirstIndexInAffectedSubtree() + insertInformation.getNumberEntriesInAffectedSubtree())
											  : insertInformation.getFirstIndexInAffectedSubtree();
				HOTRowexChildPointer const * nextEntryLocation = existingNode.getPointers() + nextEntryIndex;

				if(nextEntryLocation >= existingNode.end()) {
					return HOTRowexSynchronizedIteratorStackState(currentStackRoot, HOTRowexSynchronizedIterator::ascendOrAdvance(currentStackEntry), currentStackEntry);
				} else {
					(++currentStackEntry)->init(nextEntryLocation, existingNode.end());
					return HOTRowexSynchronizedIteratorStackState(currentStackRoot, HOTRowexSynchronizedIterator::descendOrStore(currentStackEntry), currentStackEntry );
				}
			}), guard);
		} else {
			//in case of isLowerBound == true the next ACTION is STORE otherwise the next ACTION is ADVANCE or ASCEND
			return fillBuffer({ bufferRoot }, { currentStackRoot, isLowerBound ? ITERATOR_FILL_BUFFER_STATE_STORE : ascendOrAdvance(currentStackEntry), currentStackEntry }, guard);
		}
	}

	static inline HotRowexIteratorBufferState<KeyType> fillBuffer(HotRowexIteratorBufferState<KeyType> iteratorBufferState, HOTRowexSynchronizedIteratorStackState stackState, MemoryGuard const & /* guard */) {
		while(true) {
			switch(stackState.mBufferState) {
				case ITERATOR_FILL_BUFFER_STATE_DESCEND: {
					HOTRowexNodeBase *childNode = stackState.mStackEntry->getCurrent().getNode();
					(++stackState.mStackEntry)->init(childNode->begin(), childNode->end());
					stackState.mBufferState = descendOrStore(stackState.mStackEntry);
					break;
				}
				case ITERATOR_FILL_BUFFER_STATE_STORE: {
					iteratorBufferState.push_back(stackState.mStackEntry->getCurrent());
					if(!iteratorBufferState.isFull()) {
						stackState.mBufferState = ascendOrAdvance(stackState.mStackEntry);
					} else {
						iteratorBufferState.setLastAccessedKey(getKeyForStackEntry(stackState.mStackEntry));
						return iteratorBufferState;
					}
					break;
				}
				case ITERATOR_FILL_BUFFER_STATE_ADVANCE: {
					stackState.mStackEntry->advance();
					stackState.mBufferState = descendOrStore(stackState.mStackEntry);
					break;
				}
				case ITERATOR_FILL_BUFFER_STATE_ASCEND: {
					bool elementsRemaining = ((--stackState.mStackEntry) >= stackState.mRootEntry);
					//abort if stack is consumed
					stackState.mBufferState = elementsRemaining ? ascendOrAdvance(stackState.mStackEntry) : ITERATOR_FILL_BUFFER_STATE_END;
					break;
				}
				default: { //END
					return iteratorBufferState;
				}
			}
		}
	}

	static inline FixedSizedKeyType toFixedSizedKey(KeyType const & key) {
		return idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(key));
	}

	static inline KeyType getKeyForStackEntry(HOTRowexIteratorStackEntry const * iteratorStackEntry) {
		return getKeyForChildPointer(iteratorStackEntry->getCurrent());
	}

	static inline KeyType getKeyForChildPointer(HOTRowexChildPointer const & childPointer) {
		ValueType const & leafValue = idx::contenthelpers::tidToValue<ValueType>(childPointer.getTid());
		return extractKey(leafValue);
	}
};

template<typename ValueType, template <typename> typename KeyExtractor> HOTRowexSynchronizedIterator<ValueType, KeyExtractor> const HOTRowexSynchronizedIterator<ValueType, KeyExtractor>::END_ITERATOR {};
template<typename ValueType, template <typename> typename KeyExtractor> KeyExtractor<ValueType> HOTRowexSynchronizedIterator<ValueType, KeyExtractor>::extractKey;

}}

#endif
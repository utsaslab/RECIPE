#ifndef __HOT__COMMONS__BIT_MASK_32__
#define __HOT__COMMONS__BIT_MASK_32__

#include <cassert>
#include <cstdint>
#include <iterator>

namespace hot { namespace commons {

constexpr uint32_t ALL_SLOTS_USED_MASK32 = 0xFFFFFFFF;
constexpr uint32_t ALL_SLOTS_EMPTY_MASK32 = 0x00000000;

constexpr uint8_t NO_SLOT_AVAILABLE32 = 0xFF;
constexpr uint8_t TOTAL_NUMBER_SLOTS = 32;

class BitMask32Iterator;

class BitMask32 {

private:
	uint32_t mUsedSlots;

public:
	BitMask32();
	BitMask32(uint32_t slots);
	BitMask32(int slots);

	static BitMask32 create(std::initializer_list<uint8_t> slotsToReserve);

	inline uint8_t getAndAllocateSlot();

	inline void reserveSlot(uint_fast8_t slotIndex);

	inline void removeSlot(const uint_fast8_t slotIndex);

	inline void removeEntries(const uint32_t maskForEntriesToRemove);

	inline void removeEntries(const BitMask32 maskForEntriesToRemove);

	inline uint32_t const & getMask() const;

	inline void setMask(uint32_t newMask);

	inline bool isSlotInUse(const uint_fast8_t slotIndex) const;

	inline void toggleSlot(const uint_fast8_t slotIndex, bool newValue);

	inline bool isEmpty() const;

	inline bool isFull() const;

	inline int getFirstUsedSlot() const;

	inline int consumeFirst();

	inline int getNumberUsedSlots() const;

	inline BitMask32 &operator|=(BitMask32 const &other);

	inline BitMask32 &addAll(BitMask32 const &other);

	inline BitMask32 &operator&=(BitMask32 const &other);

	inline BitMask32 operator|(BitMask32 const &other) const;

	inline BitMask32 operator&(BitMask32 const &other) const;

	inline bool operator==(BitMask32 const &other) const;

	inline bool operator!=(BitMask32 const &other) const;

	BitMask32Iterator begin() const;

	BitMask32Iterator end() const;
};

class BitMask32Iterator : public std::iterator<std::input_iterator_tag, int> {

private:
	BitMask32 mCopy;
	int mCurrent;

	void advance();

public:
	BitMask32Iterator();
	BitMask32Iterator(BitMask32 const &original);

	int operator*() const;
	int* operator->() const;

	//prefix operator
	BitMask32Iterator& operator++();
	BitMask32Iterator operator++(int);
	inline bool operator==(BitMask32Iterator const &rhs) const;
	inline bool operator!=(BitMask32Iterator const &rhs) const;

};

}}

namespace hot { namespace commons {

inline BitMask32::BitMask32() : mUsedSlots(0) {
}

inline BitMask32::BitMask32(uint32_t slots) : mUsedSlots(slots) {
}

inline BitMask32::BitMask32(int slots) : mUsedSlots(slots) {
}

inline BitMask32 BitMask32::create(std::initializer_list<uint8_t> slotsToReserve) {
	BitMask32 slots;
	for (uint8_t slot : slotsToReserve) {
		slots.reserveSlot(slot);
	}
	return slots;
}

inline uint8_t BitMask32::getAndAllocateSlot() {
	if (mUsedSlots == ALL_SLOTS_USED_MASK32) {
		return NO_SLOT_AVAILABLE32;
	}

	const uint8_t slotIndex = __builtin_ffs(~mUsedSlots) - 1;
	mUsedSlots |= (1 << slotIndex);
	return slotIndex;
}

inline void BitMask32::reserveSlot(uint_fast8_t slotIndex) {
	mUsedSlots |= (1 << slotIndex);
}

inline void BitMask32::removeSlot(const uint_fast8_t slotIndex) {
	mUsedSlots &= ~(1 << slotIndex);
}

inline void BitMask32::removeEntries(const uint32_t maskForEntriesToRemove) {
	mUsedSlots &= ~maskForEntriesToRemove;
}

inline void BitMask32::removeEntries(const BitMask32 maskForEntriesToRemove) {
	removeEntries(maskForEntriesToRemove.getMask());
}

inline uint32_t const & BitMask32::getMask() const {
	return mUsedSlots;
}

inline void BitMask32::setMask(uint32_t newMask) {
	mUsedSlots = newMask;
}

inline bool BitMask32::isSlotInUse(const uint_fast8_t slotIndex) const {
	return mUsedSlots & (1 << slotIndex);
}

inline void BitMask32::toggleSlot(const uint_fast8_t slotIndex, bool newValue) {
	uint32_t deleteMask = (~(1 << slotIndex));
	uint32_t updateMask = (newValue << slotIndex);
	mUsedSlots = (mUsedSlots & deleteMask) | updateMask;
}

inline bool BitMask32::isEmpty() const {
	return mUsedSlots == ALL_SLOTS_EMPTY_MASK32;
}

inline bool BitMask32::isFull() const {
	return mUsedSlots == ALL_SLOTS_USED_MASK32;
}

inline int BitMask32::getFirstUsedSlot() const {
	return __builtin_ctz(mUsedSlots);
}

inline int BitMask32::consumeFirst() {
	const int firstSlot = __builtin_ctz(mUsedSlots);
	removeSlot(firstSlot);
	return firstSlot;
}

inline int BitMask32::getNumberUsedSlots() const {
	return __builtin_popcount(mUsedSlots);
}

inline BitMask32 & BitMask32::operator|=(BitMask32 const &other) {
	return addAll(other);
}

inline BitMask32 & BitMask32::addAll(BitMask32 const &other) {
	mUsedSlots |= other.mUsedSlots;
	return *this;
}

inline BitMask32 & BitMask32::operator&=(BitMask32 const &other) {
	mUsedSlots &= other.mUsedSlots;
	return *this;
}

inline BitMask32 BitMask32::operator|(BitMask32 const &other) const {
	return {static_cast<uint32_t>(mUsedSlots | other.mUsedSlots)};
}

inline BitMask32 BitMask32::operator&(BitMask32 const &other) const {
	return {static_cast<uint32_t>(mUsedSlots & other.mUsedSlots)};
}

inline bool BitMask32::operator==(BitMask32 const &other) const {
	return mUsedSlots == other.mUsedSlots;
}

inline bool BitMask32::operator!=(BitMask32 const &other) const {
	return mUsedSlots != other.mUsedSlots;
}

inline BitMask32Iterator BitMask32::begin() const {
	return {*this};
}

const BitMask32Iterator DEFAULT_SLOTS_END_ITERATOR { };

inline BitMask32Iterator BitMask32::end() const { //Move to static end
	return DEFAULT_SLOTS_END_ITERATOR;
}

inline void BitMask32Iterator::advance() {
	mCopy.consumeFirst();
	mCurrent = mCopy.getFirstUsedSlot();
}

inline BitMask32Iterator::BitMask32Iterator() : mCopy {}, mCurrent {} {
};

inline BitMask32Iterator::BitMask32Iterator(BitMask32 const & original) : mCopy { original }, mCurrent { original.getFirstUsedSlot() }{
}

inline int BitMask32Iterator::operator*() const {
	return mCurrent;
}

inline int * BitMask32Iterator::operator->() const {
	return const_cast<int*>(&mCurrent);
}


//prefix operator
inline BitMask32Iterator & BitMask32Iterator::operator++() {
	assert(!mCopy.isEmpty());
	advance();
	return *this;
}

inline BitMask32Iterator BitMask32Iterator::operator++ ( int ) {
	BitMask32Iterator original { mCopy };
	advance();
	return original;
}

inline bool BitMask32Iterator::operator==(BitMask32Iterator const & rhs) const {
	return mCopy == rhs.mCopy;
}

inline bool BitMask32Iterator::operator!=(BitMask32Iterator const & rhs) const {
	return mCopy != rhs.mCopy;
}

} }

#endif
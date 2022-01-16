#ifndef __IDX__CONTENTHELPERS__KEY_UTILITIES__HPP__
#define __IDX__CONTENTHELPERS__KEY_UTILITIES__HPP__

/** @author robert.binna@uibk.ac.at */

#include <array>
#include <cstdint>
#include <cstring>

namespace idx { namespace contenthelpers {

/**
 * returns a big endian representation of the given key. If the key is already in big endian representation the key itself will be returned
 *
 * @tparam KeyType the type of the key to convert
 * @param key the key to convert
 * @return the big endian key representation
 */
template<typename KeyType> inline auto toBigEndianByteOrder(KeyType const & key) {
	return key;
};

template<> __attribute__((always_inline)) inline auto toBigEndianByteOrder<uint64_t>(uint64_t const & key) {
	return __bswap_64(key);
};

template<> inline auto toBigEndianByteOrder<uint32_t>(uint32_t const & key) {
	return __bswap_32(key);
};

template<> inline auto toBigEndianByteOrder<uint16_t>(uint16_t const & key) {
	return __bswap_16(key);
};

/**
 * @tparam KeyType the type of the key to determine the maximum key length
 * @return the maximum length of a key of a given type
 */
template<typename KeyType> constexpr inline __attribute__((always_inline)) size_t getMaxKeyLength() {
	return sizeof(KeyType);
}

constexpr size_t MAX_STRING_KEY_LENGTH = 255;
template<> constexpr inline size_t getMaxKeyLength<char const *>() {
	return MAX_STRING_KEY_LENGTH;
}

/**
 * determines the key length in bytes for a given key
 *
 * @tparam KeyType the type of the key
 * @param key the key to get its length in bytes for
 * @return the key length in bytes
 */
template<typename KeyType> inline size_t getKeyLength(KeyType const & key) {
	return getMaxKeyLength<KeyType>();
}

template<> inline size_t getKeyLength<char const *>(char const * const & key) {
	return std::min<size_t>(strlen(key) + 1u, MAX_STRING_KEY_LENGTH);
}

template<typename KeyType> inline __attribute__((always_inline)) auto toFixSizedKey(KeyType const & key) {
	return key;
}

/**
 * return a fixed size key. A fixed sized key is a deterministic length representation for a key.
 * For instance for cstrings it is a 256 byte long key representation
 *
 * @tparam KeyType the type of the key
 * @param key the key to convert to fixed size
 * @return the fixed sized key
 */
template<> inline auto toFixSizedKey(char const * const & key) {
	std::array<uint8_t, getMaxKeyLength<char const *>()> fixedSizeKey;
	strncpy(reinterpret_cast<char*>(fixedSizeKey.data()), key, getMaxKeyLength<char const *>());
	return fixedSizeKey;
}

/**
 * Gets a pointer to the key bytes of the given key.
 * Be aware that this pointer is only valid as long as keyType is valid!!
 *
 * @tparam KeyType the type of the key
 * @param key the key to get the byte wise representation for
 * @return the byte wise representation of the key
 */
template<typename KeyType> __attribute__((always_inline)) inline uint8_t const * interpretAsByteArray(KeyType const & key) {
	return reinterpret_cast<uint8_t const *>(&key);
}

template<> inline uint8_t const* interpretAsByteArray<const char*>(const char * const & cStringKey) {
	return reinterpret_cast<uint8_t const *>(cStringKey);
}

}}

#endif
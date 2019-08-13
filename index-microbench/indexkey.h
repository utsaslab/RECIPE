
#ifndef _INDEX_KEY_H
#define _INDEX_KEY_H

#include <cstring>
#include <string>

template <std::size_t keySize>
class GenericKey {
public:
  char data[keySize];
public:
  inline void setFromString(std::string key) {
    memset(data, 0, keySize);
    if(key.size() >= keySize) {
      memcpy(data, key.c_str(), keySize - 1);
      data[keySize - 1] = '\0';
    } else {
      strcpy(data, key.c_str());
    }
    
    return;
  }

  // Constructor - Fills it with 0x00
  // This is for the skiplist to initialize an empty node
  GenericKey(int) { memset(data, 0x00, keySize); }
  GenericKey() { memset(data, 0x00, keySize); }
  // Copy constructor
  GenericKey(const GenericKey &other) { memcpy(data, other.data, keySize); }
  inline GenericKey &operator=(const GenericKey &other) {  
    memcpy(data, other.data, keySize);
    return *this;
  }

  inline bool operator<(const GenericKey<keySize> &other) { return strcmp(data, other.data) < 0; }
  inline bool operator>(const GenericKey<keySize> &other) { return strcmp(data, other.data) > 0; }
  inline bool operator==(const GenericKey<keySize> &other) { return strcmp(data, other.data) == 0; }
  // Derived operators
  inline bool operator!=(const GenericKey<keySize> &other) { return !(*this == other); }
  inline bool operator<=(const GenericKey<keySize> &other) { return !(*this > other); }
  inline bool operator>=(const GenericKey<keySize> &other) { return !(*this < other); }
};

template <std::size_t keySize>
class GenericComparator {
public:
  GenericComparator() {}

  inline bool operator()(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
    int diff = strcmp(lhs.data, rhs.data);
    return diff < 0;
  }
};

template <std::size_t keySize>
class GenericEqualityChecker {
public:
  GenericEqualityChecker() {}

  inline bool operator()(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
    int diff = strcmp(lhs.data, rhs.data);
    return diff == 0;
  }
};

template <std::size_t keySize>
class GenericHasher {
public:
  GenericHasher() {}

  inline size_t operator()(const GenericKey<keySize> &lhs) const {
    (void)lhs;
    return 0UL;
  }
};

#endif

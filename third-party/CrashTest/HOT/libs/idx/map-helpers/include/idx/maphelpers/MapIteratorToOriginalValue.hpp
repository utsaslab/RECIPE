#ifndef __SPIDER__MAPHELPERS__MAP_ITERATOR_TO_ORIGINAL_VALUE__HPP__
#define __SPIDER__MAPHELPERS__MAP_ITERATOR_TO_ORIGINAL_VALUE__HPP__

/** @author robert.binna@uibk.ac.at */

#include <utility>

#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/PairKeyExtractor.hpp>
#include <idx/contenthelpers/PairPointerKeyExtractor.hpp>
#include <idx/contenthelpers/KeyComparator.hpp>

#include "idx/maphelpers/MapValueExtractor.hpp"


namespace idx { namespace maphelpers {

/**
 * Helper structure to extract the original value from the maps iterator
 *
 * @tparam MapType the typ of the container
 * @tparam ValueType the type of the value stored in the container
 * @tparam KeyExtractorType the key extractor used to extract the corresponding key for given values
 */
template<template<typename...> typename MapType, typename ValueType,
	template<typename> typename KeyExtractorType>
struct MapIteratorToOriginalValue {
	static KeyExtractorType<ValueType> toKey;
	static MapValueExtractor<ValueType, KeyExtractorType> toValue;

	using MapKeyType = decltype(toKey(std::declval<ValueType>()));
	using MapValueType = decltype(toValue(std::declval<ValueType>()));
	using IteratorType = typename MapType<MapKeyType, MapValueType, typename idx::contenthelpers::KeyComparator<MapKeyType>::type>::const_iterator;

	MapValueType operator()(IteratorType const & iterator) {
		return iterator->second;
	}
};

template<template<typename...> typename MapType, typename ValueType> struct MapIteratorToOriginalValue<MapType, ValueType, idx::contenthelpers::IdentityKeyExtractor> {
	static MapValueExtractor<ValueType, idx::contenthelpers::IdentityKeyExtractor> toValue;
	using MapValueType = decltype(toValue(std::declval<ValueType>()));

	using IteratorType = typename MapType<ValueType, MapValueType, typename idx::contenthelpers::KeyComparator<ValueType>::type>::const_iterator;

	ValueType operator()(IteratorType const & iterator) {
		return iterator->second;
	}
};

/**
 * A template spezialization in case the stored value represents a pair and therefore the iterator itself is able the represent the stored pair
 *
 * @tparam MapType the container type
 * @tparam ValueType the value type must be pair like, meaning that it has fields first and second
 */
template<template<typename...> typename MapType, typename ValueType> struct MapIteratorToOriginalValue<MapType, ValueType, idx::contenthelpers::PairKeyExtractor> {
	static idx::contenthelpers::PairKeyExtractor<ValueType> toKey;
	static MapValueExtractor<ValueType, idx::contenthelpers::PairKeyExtractor> toValue;

	using MapKeyType = decltype(toKey(std::declval<ValueType>()));
	using MapValueType = decltype(toValue(std::declval<ValueType>()));
	using IteratorType = typename MapType<MapKeyType, MapValueType, typename idx::contenthelpers::KeyComparator<MapKeyType>::type>::const_iterator;

	auto operator()(IteratorType const & iterator) {
		return *iterator;
	}
};

}}

#endif
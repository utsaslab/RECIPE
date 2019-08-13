#ifndef __SPIDER__MAPHELPERS__STL_LIKE_INDEX__HPP__
#define __SPIDER__MAPHELPERS__STL_LIKE_INDEX__HPP__

/** @author robert.binna@uibk.ac.at */

#include <utility>

#include <idx/contenthelpers/ContentEquals.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#include "idx/maphelpers/MapValueExtractor.hpp"
#include "idx/maphelpers/MapIteratorToOriginalValue.hpp"

namespace idx { namespace maphelpers {

/**
 * Helper Structure, which can be used to update the value of an iterator
 * In case updating an iterator for a specific container does not match the default stl semantics a template specialization can be profided
 *
 * @tparam MapType
 */
template<template<typename...> typename MapType> struct IteratorUpdater {
	template<typename IteratorType, typename ValueType> void operator()(IteratorType & iterator, ValueType const & value) {
		iterator->second = value;
	}
};

/**
 * A wrapper for stl::map like index structures which can be used in the context of the benchmark-helpers framework
 *
 * @tparam MapType the  template of the underlying map data type
 * @tparam ValueType the type of the value to store
 * @tparam KeyExtractorType the key extractor used to extract the key for the given value
 */
template<template<typename...> typename MapType, typename ValueType,
	template<typename> typename KeyExtractorType>
struct STLLikeIndex {
	static IteratorUpdater<MapType> updateIterator;
	static KeyExtractorType<ValueType> toKey;
	static MapValueExtractor<ValueType, KeyExtractorType> toValue;
	static MapIteratorToOriginalValue<MapType, ValueType, KeyExtractorType> toOriginalValue;

	using MapKeyType = decltype(toKey(std::declval<ValueType>()));
	using MapValueType = decltype(toValue(std::declval<ValueType>()));
	using Map = MapType<MapKeyType, MapValueType, typename idx::contenthelpers::KeyComparator<MapKeyType>::type>;

	Map map;

	inline bool insert(ValueType value) {
		auto result = map.insert({toKey(value), toValue(value)});
		return result.second;
	}

	inline idx::contenthelpers::OptionalValue<ValueType> lookup(MapKeyType const &key) const {
		return iteratorToOptional(map.find(key));
	}
	
	inline idx::contenthelpers::OptionalValue<ValueType> upsert(ValueType newValue) {
		MapValueType const & newMapEntryValue = toValue(newValue);

		std::pair<typename Map::iterator,bool> insertResult = map.insert({toKey(newValue), newMapEntryValue });

		if(insertResult.second) {
			return {}; //no previous value existed
		} else {
			const contenthelpers::OptionalValue <ValueType> &originalValue = iteratorToOptional(insertResult.first);
			updateIterator(insertResult.first, newMapEntryValue);
			//insertResult.first->second = newMapEntryValue;
			assert(map[toKey(newValue)] == newMapEntryValue);
			return originalValue;
		}
	}

	inline idx::contenthelpers::OptionalValue<ValueType> scan(MapKeyType const &key, size_t numberValues) const {
		typename Map::const_iterator iterator = map.lower_bound(key);
		for(size_t i = 0u; i < numberValues && iterator != map.end(); ++i) {
			++iterator;
		}
		return iteratorToOptional(iterator);
	}

	inline idx::contenthelpers::OptionalValue<ValueType> iteratorToOptional(typename Map::const_iterator const & iterator) const {
		bool found = iterator != map.end();
		return found ? idx::contenthelpers::OptionalValue<ValueType>(true, toOriginalValue(iterator)) : idx::contenthelpers::OptionalValue<ValueType>();
	}
};

template<template<typename...> typename MapStructureTemplate, typename ValueType,
	template<typename> typename KeyExtractorType>
KeyExtractorType<ValueType> STLLikeIndex<MapStructureTemplate, ValueType, KeyExtractorType>::toKey;

template<template<typename...> typename MapStructureTemplate, typename ValueType,
	template<typename> typename KeyExtractorType>
IteratorUpdater<MapStructureTemplate> STLLikeIndex<MapStructureTemplate, ValueType, KeyExtractorType>::updateIterator;

template<template<typename...> typename MapStructureTemplate, typename ValueType,
	template<typename> typename KeyExtractorType>
MapValueExtractor<ValueType, KeyExtractorType> STLLikeIndex<MapStructureTemplate, ValueType, KeyExtractorType>::toValue;

template<template<typename...> typename MapStructureTemplate, typename ValueType,
	template<typename> typename KeyExtractorType>
MapIteratorToOriginalValue<MapStructureTemplate, ValueType, KeyExtractorType>STLLikeIndex<MapStructureTemplate, ValueType, KeyExtractorType>::toOriginalValue;

} }

#endif
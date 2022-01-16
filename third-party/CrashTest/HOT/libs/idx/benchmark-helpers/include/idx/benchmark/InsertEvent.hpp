#ifndef __IDX__BENCHMARK__INSERT_EVENT__HPP__
#define __IDX__BENCHMARK__INSERT_EVENT__HPP__

#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/PairKeyExtractor.hpp>

namespace idx { namespace benchmark {

template<typename ValueType, template <typename> typename KeyExtractor> struct InsertEvent {
	ValueType mValueToInsert;

	//InsertEvent static read(std::istream const & input);
};

/*template<typename ValueType, template <typename> typename KeyExtractor> InsertEvent<ValueType, KeyExtractor> InsertEvent<ValueType, KeyExtractor>::read(std::istream const & input) {
	ValueType value;
	input >> value;
	return InsertEvent(value);
}

template<typename PairKeyType, typename PairValueType, template <typename> typename KeyExtractor> InsertEvent<std::pair<PairKeyType, PairValueType>, KeyExtractor> InsertEvent<std::pair<PairKeyType, PairValueType>, KeyExtractor>::read(std::istream const & input) {
	std::pair<PairKeyType, PairValueType> value;

	input >> value.first;
	input >> value.second;

	return InsertEvent(value);
}*/

} }

#endif
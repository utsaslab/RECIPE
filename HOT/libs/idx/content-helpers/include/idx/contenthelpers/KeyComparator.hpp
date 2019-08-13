#ifndef __IDX__CONTENTHELPERS__KEY_COMPARATOR__HPP__
#define __IDX__CONTENTHELPERS__KEY_COMPARATOR__HPP__

/** @author robert.binna@uibk.ac.at */

#include "idx/contenthelpers/CStringComparator.hpp"

namespace idx { namespace contenthelpers {

/**
 * Helper template which allows to derive a comparator function for a given KeyType
 *
 * @tparam the type of the key to compare
 */
template<typename V> struct KeyComparator {
	using type = std::less<V>;
};

template<> struct KeyComparator<const char*> {
	using type = idx::contenthelpers::CStringComparator;
};

} }

#endif
#ifndef __HOT__ROWEX__ITERATOR_END_TOKEN__
#define __HOT__ROWEX__ITERATOR_END_TOKEN__

#include "HOTRowexChildPointer.hpp"

namespace hot { namespace rowex {

template<int dummy>
struct HOTRowexIteratorEndTokenWorkaround {
	static HOTRowexChildPointer END_TOKEN;
};
template<int dummy> HOTRowexChildPointer HOTRowexIteratorEndTokenWorkaround<dummy>::END_TOKEN = {};

using HOTRowexIteratorEndToken = HOTRowexIteratorEndTokenWorkaround<0>;

}}

#endif


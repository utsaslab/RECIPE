#ifndef __HOT__ROWEX__FILL_BUFFER_STATES__
#define __HOT__ROWEX__FILL_BUFFER_STATES__

namespace hot { namespace rowex {
	constexpr int32_t ITERATOR_FILL_BUFFER_STATE_DESCEND = 0;
	constexpr int32_t ITERATOR_FILL_BUFFER_STATE_STORE = 1;
	constexpr int32_t ITERATOR_FILL_BUFFER_STATE_ADVANCE = 2;
	constexpr int32_t ITERATOR_FILL_BUFFER_STATE_ASCEND = 3;
	constexpr int32_t ITERATOR_FILL_BUFFER_STATE_END = 4;
}}

#endif
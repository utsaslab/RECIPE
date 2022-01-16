#ifndef __IDX__CONTENTHELPERS__OPTIONAL_VALUE__HPP__
#define __IDX__CONTENTHELPERS__OPTIONAL_VALUE__HPP__

/** @author robert.binna@uibk.ac.at */

namespace idx { namespace contenthelpers {

/**
 * Helper class to represent values, which might not be set, or be in an undefined state
 *
 * @tparam ValueType the type of the value to store
 * @author robert.binna@uibk.ac.at
 */
template<typename ValueType> struct OptionalValue{
	bool mIsValid;
	ValueType mValue;

	/**
	 * constructs an optional value which has no value set and is therefore not valid
	 */
	inline OptionalValue() : mIsValid(false) {
	}

	/**
	 * constructs an optional value. Depending on the is valid the store parameter migh be undefined
	 *
	 * @param isValid only if isValid is true is the value defined and valid
	 * @param value the value to encapsulate
	 */
	inline OptionalValue(bool const & isValid, ValueType const & value) : mIsValid(isValid), mValue(value) {
	}

	/**
	 * Test whether two optional values comply with each other.
	 * Two optional values comply in case both are invalid or both are valid and share the same value.
	 *
	 * @param expected the optional value to test for compliance with the current value.
	 * @return whether both values comply
	 */
	inline bool compliesWith(OptionalValue const & expected) const {
		return (this->mIsValid == expected.mIsValid) & (!this->mIsValid || (mValue == expected.mValue));
	}
};

}}

#endif
#ifndef __HOT__COMMONS__GENERIC_MASK_CONVERSION_INFORMATION___
#define __HOT__COMMONS__GENERIC_MASK_CONVERSION_INFORMATION___

namespace hot { namespace commons {

template<typename PartialKeyType> struct PartialKeyConversionInformation {
	PartialKeyType const mAdditionalMask;
	PartialKeyType const mConversionMask;

	PartialKeyConversionInformation(PartialKeyType const additionalMask, PartialKeyType const conversionMask) : mAdditionalMask(additionalMask), mConversionMask(conversionMask) {
	}
};

} }

#endif
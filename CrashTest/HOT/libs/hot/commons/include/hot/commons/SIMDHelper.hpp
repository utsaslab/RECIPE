#ifndef __HOT__COMMONS__SIMD_HELPER_HPP___
#define __HOT__COMMONS__SIMD_HELPER_HPP___

#include <array>
#include <cstdint>
#include <immintrin.h>

namespace hot { namespace commons {

template<uint numberBits> struct SIMDRegisterTypeMapper {
};

template<> struct SIMDRegisterTypeMapper<64u> {
	using SIMDRegisterType = __m64;
};

template<> struct SIMDRegisterTypeMapper<128u> {
	using SIMDRegisterType = __m128i;
};

template<> struct SIMDRegisterTypeMapper<256u> {
	using SIMDRegisterType = __m256i;
};

template<uint numberBits>
struct SIMDHelper {
	using SIMDRegisterType = typename SIMDRegisterTypeMapper<numberBits>::SIMDRegisterType;

	/**
	 * Create mask from the most significant bit of each 8-bit element of the input register
	 * @param inputRegister the variable to extract the most significant bits of
	 * @return the created mask
	 */
	static inline uint32_t moveMask8(SIMDRegisterType inputRegister);

	/**
	 * compares packed 8-bit integers in a and b
	 *
	 * @param a first SIMD register
	 * @param b second SIMD register
	 * @return for each equal packaged 8-bit integer the value 0xFF is set in the output register
	 */
	static inline SIMDRegisterType cmpeq_epi8(SIMDRegisterType a, SIMDRegisterType b);

	/**
	 * Broadcast the 8-bit integer to the simd register
	 * @param byte the 8-bit integer to broadcase
	 * @return the created SIMD-register
	 */
	static inline SIMDRegisterType set1_epi8(uint8_t byte);

	/**
	 * Creates a SIMD-register with all bits beeing set to 0
	 * @return the newly created SIMD-register
	 */
	static inline SIMDRegisterType zero();

	/**
	 * calculates the bitwise binary and of two SIMD registers
	 *
	 * @param a the first operand
	 * @param b the second operand
	 * @return the resulting binary and of the provided parameters
	 */
	static inline SIMDRegisterType binaryAnd(SIMDRegisterType a, SIMDRegisterType b);

	/**
	 * calculates the bitwise binary or of two SIMD registers
	 *
	 * @param a the first operand
	 * @param b the second operand
	 * @return the resulting binary or of the provided parameters
	 */
	static inline SIMDRegisterType binaryOr(SIMDRegisterType a, SIMDRegisterType b);

	/**
	 * calculates the bitwise binary and not of two SIMD registers
	 *
	 * @param a the first operand
	 * @param b the second operand
	 * @return the resulting binary and not of the provided parameters
	 */
	static inline SIMDRegisterType binaryAndNot(SIMDRegisterType a, SIMDRegisterType b);

	/**
	 * shifts all entries in the simd register left by 8 bits
	 * @param a the parameter to shift
	 * @return the shifted SIMD-register
	 */
	static inline SIMDRegisterType shiftLeftOneByte(SIMDRegisterType a);

	/**
	 * loads the data of an equivally sized std array into a SIMD-register
	 * @param array the array to load
	 * @return the loaded SIMD-register
	 */
	static inline SIMDRegisterType toRegister(std::array<uint64_t, numberBits/64u> const & array);

	/**
	 * loads the data stored at the given address into a SIMD-register
	 * @param data the data to load
	 * @return the loaded SIMD-register
	 */
	static inline SIMDRegisterType toRegister(void const * data);

	/**
	 * stores the data of the given SIMD register in the provided std array
	 *
	 * @param data the register to store
	 * @param array the target array
	 */
	static inline void store(SIMDRegisterType data, std::array<uint64_t, numberBits/64u> & array);

	/**
	 * stores the data of the given SIMD register at the specified target location
	 *
	 * @param data the register to store
	 * @param location the target address
	 */
	static inline void store(SIMDRegisterType data, void* location);
};

template<>
struct SIMDHelper<64u> {
	using SIMDRegisterType = typename SIMDRegisterTypeMapper<64>::SIMDRegisterType;

	static inline  __attribute__((always_inline)) uint32_t moveMask8(SIMDRegisterType inputRegister) {
		return _mm_movemask_pi8(inputRegister);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType cmpeq_epi8(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_cmpeq_pi8(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType set1_epi8(uint8_t byte) {
		return _mm_set1_pi8(byte);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType zero() {
		return _mm_setzero_si64();
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryAnd(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_and_si64(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryOr(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_or_si64(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryAndNot(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_andnot_si64(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType shiftLeftOneByte(SIMDRegisterType a) {
		return _mm_slli_si64(a, 8);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType toRegister(std::array<uint64_t, 1u> const & array) {
		return _mm_cvtsi64_m64(*array.data());
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType toRegister(void const * data) {
		return _mm_cvtsi64_m64(*reinterpret_cast<uint64_t const *>(data));
	}


	static inline  __attribute__((always_inline)) void store(SIMDRegisterType data, std::array<uint64_t, 1u> & array) {
		array[0] = _mm_cvtm64_si64(data);
	}

	static inline  __attribute__((always_inline)) void store(SIMDRegisterType data, void* location) {
		*reinterpret_cast<uint64_t*>(location) = _mm_cvtm64_si64(data);
	};


};

template<>
struct SIMDHelper<128u> {
	using SIMDRegisterType = typename SIMDRegisterTypeMapper<128>::SIMDRegisterType;

	static inline  __attribute__((always_inline)) uint32_t moveMask8(SIMDRegisterType inputRegister) {
		return _mm_movemask_epi8(inputRegister);
	}

	static inline  __attribute__((always_inline))SIMDRegisterType cmpeq_epi8(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_cmpeq_epi8(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType convertWithZeroExtend(SIMDHelper<64u>::SIMDRegisterType sourceRegister) {
		return _mm_cvtsi64_si128(_mm_cvtm64_si64(sourceRegister));
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType set1_epi8(uint8_t byte) {
		return _mm_set1_epi8(byte);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType zero() {
		return _mm_setzero_si128();
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryAnd(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_and_si128(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryOr(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_or_si128(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryAndNot(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm_andnot_si128(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType toRegister(std::array<uint64_t, 2u> const &array) {
		return _mm_lddqu_si128(reinterpret_cast<SIMDRegisterType const *>(array.data()));
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType toRegister(void const *rawData) {
		return _mm_lddqu_si128(reinterpret_cast<SIMDRegisterType const *>(rawData));
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType shiftLeftOneByte(SIMDRegisterType a) {
		return _mm_bslli_si128(a, 1);
	}

	static inline  __attribute__((always_inline)) void store(SIMDRegisterType data, std::array<uint64_t, 2u> &array) {
		_mm_storeu_si128(reinterpret_cast<SIMDRegisterType *>(array.data()), data);
	}

	static inline  __attribute__((always_inline)) void store(SIMDRegisterType data, void *location) {
		_mm_storeu_si128(reinterpret_cast<SIMDRegisterType *>(location), data);
	}
};

template<>
struct SIMDHelper<256u> {
	using SIMDRegisterType = typename SIMDRegisterTypeMapper<256>::SIMDRegisterType;

	static inline  __attribute__((always_inline)) uint32_t moveMask8(SIMDRegisterType inputRegister) {
		return _mm256_movemask_epi8(inputRegister);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType cmpeq_epi8(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm256_cmpeq_epi8(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType convertWithZeroExtend(SIMDHelper<128u>::SIMDRegisterType sourceRegister) {
		return _mm256_insertf128_si256(zero(), sourceRegister, 0);
	}


	static inline  __attribute__((always_inline)) SIMDRegisterType set1_epi8(uint8_t byte) {
		return _mm256_set1_epi8(byte);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType set1(uint8_t byte) {
		return _mm256_set1_epi8(byte);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType set1(uint16_t unsignedShort) {
		return _mm256_set1_epi16(unsignedShort);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType set1(uint32_t unsigneInt) {
		return _mm256_set1_epi32(unsigneInt);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType zero() {
		return _mm256_setzero_si256();
	}

	/**
	*
	* @return an 256 bit simd register with all bits set
	*/
	static inline  __attribute__((always_inline)) SIMDRegisterType maxValue() {
		SIMDRegisterType zeroRegister = zero();
		return cmpeq_epi8(zeroRegister, zeroRegister);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryAnd(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm256_and_si256(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryOr(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm256_or_si256(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType binaryAndNot(SIMDRegisterType a, SIMDRegisterType b) {
		return _mm256_andnot_si256(a, b);
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType toRegister(std::array<uint64_t, 4u> const & array) {
		return _mm256_loadu_si256(reinterpret_cast<SIMDRegisterType const*>(array.data()));
	}

	static inline  __attribute__((always_inline)) SIMDRegisterType toRegister(void const * rawData) {
		return _mm256_loadu_si256(reinterpret_cast<SIMDRegisterType const*>(rawData));
	}

	/**
	 * vgl http://stackoverflow.com/questions/25248766/emulating-shifts-on-32-bytes-with-avx
	 *
	 * @param a
	 * @param bytesToShift
	 * @return
	 */
	static inline  __attribute__((always_inline)) SIMDRegisterType shiftLeftOneByte(SIMDRegisterType a) {
		return _mm256_alignr_epi8(a, _mm256_permute2x128_si256(a, a, _MM_SHUFFLE(0, 0, 2, 0)), 16 - 1);
	}

	static inline  __attribute__((always_inline)) void store(SIMDRegisterType data, std::array<uint64_t, 4u> & array) {
		_mm256_storeu_si256(reinterpret_cast<SIMDRegisterType*>(array.data()), data);
	}

	static inline  __attribute__((always_inline)) void store(SIMDRegisterType data, void* location) {
		_mm256_storeu_si256(reinterpret_cast<SIMDRegisterType*>(location), data);
	}

};

}}

#endif
//
// Created by Robert Binna on 09.04.15.
//
#ifndef __IDX__UTILS__RANDOMRANGEGENERATOR__
#define __IDX__UTILS__RANDOMRANGEGENERATOR__

#include <cstdint>
#include <functional>
#include <random>
#include <iostream>

namespace idx { namespace utils {

template<class IntType> class RandomRangeGenerator {

public:
	RandomRangeGenerator(IntType low, IntType high) :
		rng(), nextRandomNumber(std::bind(std::uniform_int_distribution<IntType>{low, high}, rng))
	{
		mSeed = std::random_device{}();
		rng.seed({ static_cast<uint64_t>(mSeed) });
	}

	RandomRangeGenerator(IntType seed, IntType low, IntType high) :
		rng(), nextRandomNumber(std::bind(std::uniform_int_distribution<IntType>{low, high}, rng)), mSeed(seed)
	{
		rng.seed({ static_cast<uint64_t>(mSeed) });
	}

	IntType operator()() const {
		IntType result = nextRandomNumber();
		return result;
	}

	IntType getSeed() {
		return mSeed;
	}

private:
	std::mt19937_64 rng;
	std::function <uint64_t(void)> nextRandomNumber;
	IntType mSeed;
};

} }

#endif //__SPIDER__SHR_TREE2__RANDOMRANGEGENERATOR__

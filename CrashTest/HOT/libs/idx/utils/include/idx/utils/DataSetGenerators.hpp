#ifndef __IDX__UTILS__DATA_SET_GENERATORS__
#define __IDX__UTILS__DATA_SET_GENERATORS__

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <string>
#include <set>
#include <vector>

#include "idx/utils/RandomRangeGenerator.hpp"
#include "idx/utils/CommandParser.hpp"
#include "idx/utils/8ByteDatFileIO.hpp"

namespace idx { namespace utils {


std::vector<uint64_t> createDenseDataSet(size_t size) {
	std::vector<uint64_t> values(size);
	for(size_t i = 0; i < size; ++i) {
		values[i] = i;
	}
	return std::move(values);
}

std::vector<uint64_t> createPseudoRandomDataSet(size_t size) {
	std::vector<uint64_t> values(size);
	/*for(size_t i = 0; i < size/10; ++i) {
		int(j = 0; j < 10; ++j) {
			values[i] = (static_cast<uint64_t>(std::rand()) << 32) | i;;
		}
	}*/

	for(size_t i = 0; i < size; ++i) {
		values[i] = (static_cast<uint64_t>(std::rand()) << 32) | i;;
	}
	return std::move(values);
}

std::vector<uint64_t> createRandomDataSet(size_t size, bool isVerbose=false) {
	RandomRangeGenerator<uint64_t> rnd { 0, INT64_MAX };
	std::set<uint64_t> uniqueRandomValues;
	std::vector<uint64_t> results(size);

	if(isVerbose) {
		std::cout << "Using seed:" << rnd.getSeed() << std::endl;
	}

	while(uniqueRandomValues.size() < size) {
		uniqueRandomValues.insert(rnd());
	}

	return std::vector<uint64_t>(uniqueRandomValues.begin(), uniqueRandomValues.end());
};

std::vector<uint64_t> creatDataSet(CommandParser const & params, std::string const & typeParamName) {
	std::string dataSetType = params.expect<std::string>(typeParamName);
	std::vector<uint64_t> values;

	if(dataSetType == "file") {
		std::string inputFile = params.expectExistingFile("input");
		values = idx::utils::readDatFile(inputFile, params.get("size", 0));
	} else {
		size_t size = params.expect<size_t>("size");

		if(dataSetType == "dense") {
			values = idx::utils::createDenseDataSet(size);
		} else if(dataSetType == "pseudorandom") {
			values = idx::utils::createPseudoRandomDataSet(size);
		} else if(dataSetType == "random") {
			values = idx::utils::createRandomDataSet(size, params.isVerbose());
		}
	}

	return std::move(values);
}

} }

#endif
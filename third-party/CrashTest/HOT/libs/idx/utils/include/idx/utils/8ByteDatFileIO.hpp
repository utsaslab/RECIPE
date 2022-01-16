#ifndef __IDX__UTILS__8ByteDatFileIO__
#define __IDX__UTILS__8ByteDatFileIO__

#include <algorithm>
#include <iostream>
#include <cstdint>
#include <fstream>
#include <string>
#include <set>
#include <vector>

namespace idx { namespace utils {

inline std::vector<uint64_t> readDatFile(std::string const &fileName, size_t numberValues = 0) {
	size_t recordsToRead;
	std::ifstream input(fileName);
	input >> recordsToRead;
	if (numberValues > 0) {
		recordsToRead = std::min(recordsToRead, numberValues);
	}
	std::vector<uint64_t> values;
	values.reserve(recordsToRead);
	std::set<uint64_t> duplicateChecker;
	int numberDuplicates = 0;
	for (size_t i = 0; i < recordsToRead; ++i) {
		uint64_t currentValue = 0;
		input >> currentValue;
		if(duplicateChecker.find(currentValue) != duplicateChecker.end()) {
			++numberDuplicates;
		} else {
			duplicateChecker.insert(currentValue);
			values.push_back(currentValue);
		}
	}
	input.close();

	return std::move(values);
}

inline void readDatFile(std::string const &fileName, uint64_t* values, size_t numberValues) {
	size_t recordsToRead;
	std::ifstream input{fileName};
	input >> recordsToRead;
	if (numberValues < recordsToRead) {
		std::cout << "Should read " << numberValues << " values but " << fileName << " only contains " << recordsToRead << " values."<< std::endl;
		exit(1);
	}
	std::cout << "Reading " << numberValues << " values " << std::endl;
	for (size_t i = 0; i < numberValues; ++i) {
		input >> values[i];
	}
	input.close();
}


inline void writeDataFile(std::ostream & output, std::vector<uint64_t> const &values) {
	output << values.size() << std::endl;
	for(uint64_t value : values) {
		output << value << std::endl;
	}
}

//maybe switch to exceptions, exit(1) is not realy nice
inline void writeDatFile(std::string const &fileName, std::vector<uint64_t> const &values) {
	std::ofstream output{fileName};
	if (!output.is_open()) {
		output << "Could not write data file to " << fileName << std::endl;
		exit(1);
	}
	writeDataFile(output, values);
	output.close();
}


} }

#endif

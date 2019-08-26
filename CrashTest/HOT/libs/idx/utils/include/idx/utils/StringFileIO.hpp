#ifndef __IDX__UTILS__STRING_FILE_IO__HPP__
#define __IDX__UTILS__STRING_FILE_IO__HPP__

#include <cstring>
#include <string>
#include <vector>
#include <fstream>


namespace idx { namespace utils {

std::vector<std::pair<char *, size_t>> readStrings(std::string const &fileName,  size_t numberEntries = SIZE_MAX) {
	std::vector <std::pair<char *, size_t>> resultVector;
	std::string line;
	std::ifstream input(fileName);
	while (std::getline(input, line) && (numberEntries == SIZE_MAX || resultVector.size() < numberEntries)) {
		if (line.length() < 127) {
			char *lineBuffer = new char[line.length() + 2];
			lineBuffer = lineBuffer + (((uintptr_t) lineBuffer) % 2);
			strcpy(lineBuffer, line.c_str());
			resultVector.push_back({lineBuffer, line.length()});
		}
	}
	input.close();
	return resultVector;
}

} }

#endif
#ifndef __IDX__BENCHMARK__BENCHMARK_CONFIGURATION__
#define __IDX__BENCHMARK__BENCHMARK_CONFIGURATION__

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <cstdlib>
#include <idx/utils/CommandParser.hpp>
#include <idx/utils/DataSetModifiers.hpp>

namespace idx { namespace benchmark {

struct ConstCharStarComparator
{
	bool operator()(std::pair<char*, size_t> s1, std::pair<char*, size_t> s2) const
	{
		return strcmp(s1.first, s2.first) < 0;
	}
};

struct ConstCharStarReverseComparator
{
	bool operator()(std::pair<char*, size_t> s1, std::pair<char*, size_t> s2) const
	{
		return strcmp(s1.first, s2.first) > 0;
	}
};

struct StringBenchmarkConfiguration {

public:
	std::shared_ptr<std::vector<std::pair<char*, size_t>>> mInsertStrings;
	std::shared_ptr<std::vector<std::pair<char*, size_t>>> mLookupStrings;
	idx::utils::CommandParser mCommandParser;
	std::map<std::string, std::string> mRawArguments;
	bool mInsertOnly;
	bool mWriteDotFile;
	std::string mDotFileLocation;
	size_t mNumberThreads;

	StringBenchmarkConfiguration(idx::utils::CommandParser const & params)
			: mInsertStrings(new std::vector<std::pair<char*, size_t>>)
			, mLookupStrings(new std::vector<std::pair<char*, size_t>>)
			, mCommandParser(params)
			, mRawArguments(params.getRawArguments())
			, mInsertOnly(params.get<bool>("insertOnly", false))
			, mWriteDotFile(params.has("writeDotRepresentation"))
			, mDotFileLocation(mWriteDotFile ? params.expect<std::string>("writeDotRepresentation") : "")
			, mNumberThreads(params.get<size_t>("threads", 1))
	{
		createInsertStrings(params);
		createLookupStrings(params);
	}

	StringBenchmarkConfiguration(StringBenchmarkConfiguration const & configuration) = default;

	bool isInsertOnly() {
		return mInsertOnly;
	}

	bool shouldWriteDotRepresentation() {
		return mWriteDotFile;
	}

	std::string getDotFileLocation() {
		return mDotFileLocation;
	}

	std::vector<std::pair<char*, size_t>> & getInsertStrings() {
		return *mInsertStrings;
	}

	std::vector<std::pair<char*, size_t>> & getLookupStrings() {
		return *mLookupStrings;
	}

	std::vector<std::pair<char*, size_t>> & getSortedLookupValues() {
		ConstCharStarComparator comparator;
		std::sort(mLookupStrings->begin(), mLookupStrings->end(), comparator);
		return *mLookupStrings;
	}

	void writeYAML(std::ostream & output, const size_t depth) const {
		std::string l0(depth * 2 , ' ');

		for(std::pair<std::string, std::string> argument : mRawArguments) {
			output << l0 << argument.first << ": " << argument.second << std::endl;
		}
	}

private:
	void readStrings(std::string const & fileName, std::vector<std::pair<char*, size_t>> & resultVector, size_t numberEntries = SIZE_MAX) {
		std::string line;
		std::ifstream input(fileName);
		while(std::getline(input, line) && (numberEntries == SIZE_MAX || resultVector.size() < numberEntries)) {
			if(line.length() < 127) {
				char *lineBuffer = new char[line.length() + 2];
				lineBuffer = lineBuffer + (((uintptr_t) lineBuffer)%2);
				strcpy(lineBuffer, line.c_str());
				resultVector.push_back({ lineBuffer, line.length() });
			}
		}
		input.close();
	}

	void modifyStringVector(std::string modifierType, std::vector<std::pair<char*, size_t>> & resultVector) {
		if(modifierType == "sequential") {
			std::sort(resultVector.begin(), resultVector.end(), ConstCharStarComparator());
		} else if(modifierType == "reverse") {
			std::sort(resultVector.begin(), resultVector.end(), ConstCharStarReverseComparator());
		} else if(modifierType == "random") {
			std::srand(1);
			std::srand(0);
			std::random_shuffle(resultVector.begin(), resultVector.end());
		}
	}

	void createInsertStrings(idx::utils::CommandParser const & parser) {
		std::string inputFileName = parser.expectExistingFile("inputFile");
		readStrings(inputFileName, *mInsertStrings, parser.get("size", -1));
		if(parser.has("insertModifier")) {
			modifyStringVector(parser.expectOneOf<std::string>("insertModifier", { "sequential", "reverse", "random"}), *mInsertStrings);
		}
	}

	void createLookupStrings(idx::utils::CommandParser const & parser) {
		if(parser.has("lookupFile")) {
			std::string lookupFileName = parser.expectExistingFile("lookupFile");
			readStrings(lookupFileName, *mLookupStrings, parser.get("size", -1));
		} else {
			*mLookupStrings = { mInsertStrings->begin(), mInsertStrings->end() };
		};
		if(parser.has("lookupModifier")) {
			modifyStringVector(parser.expectOneOf<std::string>("lookupModifier", { "sequential", "reverse", "random"}), *mLookupStrings);
		}

	}

};


} }

#endif
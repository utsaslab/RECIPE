#ifndef __IDX__BENCHMARK__BENCHMARK_CONFIGURATION__
#define __IDX__BENCHMARK__BENCHMARK_CONFIGURATION__

#include <algorithm>
#include <cstdint>
#include <memory>
#include <cstdlib>
#include <idx/utils/DataSetGenerators.hpp>
#include <idx/utils/CommandParser.hpp>
#include <idx/utils/DataSetModifiers.hpp>
#include <idx/utils/8ByteDatFileIO.hpp>

namespace idx { namespace benchmark {

struct BenchmarkConfiguration {

public:
	std::shared_ptr<std::vector<uint64_t>> mInsertKeys;
	std::shared_ptr<std::vector<uint64_t>> mLookupKeys;
	idx::utils::CommandParser mCommandParser;
	std::map<std::string, std::string> mRawArguments;
	bool mInsertOnly;
	size_t mNumberThreads;

	BenchmarkConfiguration(idx::utils::CommandParser const & params)
			: mInsertKeys(new std::vector<uint64_t>)
			, mLookupKeys(new std::vector<uint64_t>)
			, mCommandParser(params)
			, mRawArguments(params.getRawArguments())
			, mInsertOnly(params.get<bool>("insertOnly", false))
			, mNumberThreads(params.get<size_t>("threads", 1))
	{
		*mInsertKeys = idx::utils::creatDataSet(params, "insert");
		applyInputModifier(params);
		createLookupKeys(params);
	}

	BenchmarkConfiguration(BenchmarkConfiguration const & configuration) = default;

	bool isInsertOnly() {
		return mInsertOnly;
	}

	std::vector<uint64_t> & getInsertValues() {
		return *mInsertKeys;
	}

	std::vector<uint64_t> & getLookupValues() {
		return *mLookupKeys;
	}

	void writeYAML(std::ostream & output, const size_t depth) const {
		std::string l0(depth * 2 , ' ');

		for(std::pair<std::string, std::string> argument : mRawArguments) {
			output << l0 << argument.first << ": " << argument.second << std::endl;
		}
	}

private:
	void applyInputModifier(idx::utils::CommandParser const & params) {
		if(params.has("insertModifier")) {
			std::string insertModifier = params.expect<std::string>("insertModifier");
			if(params.isVerbose()) {
				std::cout << "insertModifier" << insertModifier << std::endl;
			}
			if(insertModifier == "sequential") {
				idx::utils::sortData(*mInsertKeys);
			} else if(insertModifier == "reverse") {
				idx::utils::reverseData(*mInsertKeys);
			} else if(insertModifier == "random") {
				idx::utils::randomizeData(*mInsertKeys);
			}
		} else {
			if(params.isVerbose()) {
				std::cout << "no insert modifier specified" << std::endl;
			}
		}
	}

	void createLookupKeys(idx::utils::CommandParser const & parser) {
		if(parser.has("lookup")) {
			std::string lookupType = parser.expect<std::string>("lookup");
			if(lookupType == "sequential") {
				*mLookupKeys = std::vector<uint64_t>(mInsertKeys->begin(), mInsertKeys->end());
				idx::utils::sortData(*mLookupKeys);
			} else if(lookupType == "reverse") {
				*mLookupKeys = std::vector<uint64_t>(mInsertKeys->rbegin(), mInsertKeys->rend());
			} else if(lookupType == "random") {
				*mLookupKeys = std::vector<uint64_t>(mInsertKeys->begin(), mInsertKeys->end());
				idx::utils::randomizeData(*mLookupKeys);
			} else if(lookupType == "file") {
				std::string lookupFile = parser.expectExistingFile("lookupFile");
				*mLookupKeys = idx::utils::readDatFile(lookupFile);
			}
		} else {
			mLookupKeys = mInsertKeys;
		}
	}

};


} }

#endif
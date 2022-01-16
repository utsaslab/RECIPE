#ifndef __IDX__BENCHMARK__DAT_FILE_BENCHMARK_CONFIGURATION__
#define __IDX__BENCHMARK__DAT_FILE_BENCHMARK_CONFIGURATION__

#include <algorithm>
#include <cstdint>
#include <memory>
#include <cstring>
#include <cstdlib>
#include <idx/utils/CommandParser.hpp>
#include <idx/utils/DataSetModifiers.hpp>

#include "idx/benchmark/DatFile.hpp"

namespace idx { namespace benchmark {

struct DatFileBenchmarkConfiguration {

public:
	std::shared_ptr<DatFile> mInsertTuples;
	std::shared_ptr<DatFile> mLookupTuples;
	std::map<std::string, std::string> mRawArguments;

	bool mInsertOnly;
	bool mWriteDotFile;
	std::string mDotFileLocation;

	DatFileBenchmarkConfiguration(idx::utils::CommandParser const & params)
		: mInsertTuples()
		, mLookupTuples()
		, mRawArguments(params.getRawArguments())
		, mInsertOnly(params.get<bool>("insertOnly", false))
		, mWriteDotFile(params.has("writeDotRepresentation"))
		, mDotFileLocation(mWriteDotFile ? params.expect<std::string>("writeDotRepresentation") : "")
	{
		loadInsertTuples(params);
		loadLookupStrings(params);
	}

	DatFileBenchmarkConfiguration(DatFileBenchmarkConfiguration const & configuration) : mInsertTuples{ configuration.mInsertTuples}, mLookupTuples{ configuration.mLookupTuples} {
	}

	bool isInsertOnly() {
		return mInsertOnly;
	}

	bool shouldWriteDotRepresentation() {
		return mWriteDotFile;
	}

	std::string getDotFileLocation() {
		return mDotFileLocation;
	}

	DatFile const & getInsertTuples() {
		return *mInsertTuples;
	}

	DatFile const & getLookupTuples() {
		return *mLookupTuples;
	}

	DatFile const & getSortedLookupValues() {
		mLookupTuples->sort();
		return *mLookupTuples;
	}

	void writeYAML(std::ostream & output, const size_t depth) const {
		std::string l0(depth * 2 , ' ');

		for(std::pair<std::string, std::string> argument : mRawArguments) {
			output << l0 << argument.first << ": " << argument.second << std::endl;
		}
	}

private:
	void modifyDatFile(std::string const & modifierType, DatFile & datFile) {
		if(modifierType == "sequential") {
			datFile.sort();
		} else if(modifierType == "reverse") {
			datFile.reverseSort();
		} else if(modifierType == "random") {
			datFile.randomize();
		}
	}

	void loadInsertTuples(idx::utils::CommandParser const & parser) {
		std::string inputFileName = parser.expectExistingFile("inputFile");
		mInsertTuples = idx::benchmark::DatFile::read(inputFileName, parser.get("size", -1));

		if(parser.has("insertModifier")) {
			modifyDatFile(parser.expectOneOf<std::string>("insertModifier", { "sequential", "reverse", "random"}), *mInsertTuples);
		}
	}

	void loadLookupStrings(idx::utils::CommandParser const & parser) {
		if(parser.has("lookupFile")) {
			std::string lookupFileName = parser.expectExistingFile("lookupFile");
			mLookupTuples = idx::benchmark::DatFile::read(lookupFileName, parser.get("size", -1));
		} else if(!parser.has("lookupModifier")){
			mLookupTuples = mInsertTuples;
		} else {
			mLookupTuples = std::shared_ptr<idx::benchmark::DatFile>(new idx::benchmark::DatFile(*mInsertTuples));
		};
		if(parser.has("lookupModifier")) {
			modifyDatFile(parser.expectOneOf<std::string>("lookupModifier", { "sequential", "reverse", "random"}), *mLookupTuples);
		}
	}

};


} }

#endif
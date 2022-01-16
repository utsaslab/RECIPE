#ifndef __IDX__BENCHMARK__DAT_FILE_BENCHMARK__
#define __IDX__BENCHMARK__DAT_FILE_BENCHMARK__

#include <algorithm>
#include <chrono>
#include <iostream>
#include <fstream>
#include <cstdint>

#include "idx/benchmark/DatFile.hpp"
#include "idx/benchmark/DatFileBenchmarkCommandlineHelper.hpp"
#include "idx/benchmark/DatFileBenchmarkConfiguration.hpp"
#include "idx/benchmark/BenchmarkResult.hpp"
#include "idx/utils/GitSHA1.hpp"

namespace idx { namespace benchmark {

template<class Benchmarkable> class DatFileBenchmark {
	std::string mBinary;
	Benchmarkable mBenchmarkable;
	DatFileBenchmarkConfiguration mBenchmarkConfiguration;
	BenchmarkResult mBenchmarkResults;
	std::string mDataStructureName;


public:
	DatFileBenchmark(int argc, char** argv, std::string const & dataStructureName) :
		mBinary(argv[0]),
		mBenchmarkable {},
		mBenchmarkConfiguration{ DatFileBenchmarkCommandlineHelper(argc, argv, dataStructureName).parseArguments() },
		mBenchmarkResults {},
		mDataStructureName { dataStructureName }
	{
	}

	template<typename Function>
	void benchmarkOperation(std::string const & operationName, DatFile const & tuples, int repeats, Function functionToBenchmark) {
		OperationResult result;

		uint64_t numberKeys = tuples.getNumberTuples();
		auto operationStart = std::chrono::system_clock::now().time_since_epoch();
		bool worked = true;
		for(int i=0;  i < repeats; ++i) {
			worked = worked && functionToBenchmark(tuples);
		}
		auto operationEnd = std::chrono::system_clock::now().time_since_epoch();

		result.mNumberOps = repeats * numberKeys;
		result.mDurationInNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(operationEnd).count() - std::chrono::duration_cast<std::chrono::nanoseconds>(operationStart).count();
		result.mNumberKeys = numberKeys;
		result.mExecutionState = worked;
		mBenchmarkResults.add(operationName, result);
	}

	int run() {
		DatFile const & insertTuples = mBenchmarkConfiguration.getInsertTuples();

		benchmarkOperation("insert", insertTuples, 1, [&] (DatFile const & tuples) {
			return mBenchmarkable.insert(tuples);
		});
		mBenchmarkResults.setIndexStatistics(mBenchmarkable.getMemoryConsumption());

		/*for(int i=0; i < mBenchmarkConfiguration.getLookupTuples().getNumberTuples(); ++i) {
			std::cout << "< " << mBenchmarkConfiguration.getLookupTuples().getCell(i, 0) << "> "
					  << "< " << mBenchmarkConfiguration.getLookupTuples().getCell(i, 1) << "> "
					  << "< " << mBenchmarkConfiguration.getLookupTuples().getCell(i, 2) << "> "
					  << std::endl;
		}*/

		if(!mBenchmarkConfiguration.isInsertOnly()) {
			DatFile const & lookupKeys = mBenchmarkConfiguration.getLookupTuples();
			uint64_t repeats = 100000000 / (lookupKeys.getNumberTuples());
			if (repeats < 1) {
				repeats = 1;
			}
			benchmarkOperation("lookup", lookupKeys, repeats, [&](DatFile const & tuples) {
				return mBenchmarkable.search(tuples);
			});
		}
		std::cout << "Lookup bytes:: "  << mBenchmarkConfiguration.getLookupTuples().getTupleSizeInBytes() << std::endl;

		DatFile const &tuplesToIterate = mBenchmarkConfiguration.getSortedLookupValues();
		uint64_t repeats = 100000000 / (tuplesToIterate.getNumberTuples());
		if (repeats < 1) {
			repeats = 1;
		}
		benchmarkOperation("iterate", tuplesToIterate, repeats, [&] (DatFile const & tuples) {
			return mBenchmarkable.iterate(tuples);
		});

		writeYAML(std::cout);


		if(mBenchmarkConfiguration.shouldWriteDotRepresentation()) {
			std::ofstream output(mBenchmarkConfiguration.getDotFileLocation());
			mBenchmarkable.writeDotRepresentation(output);
		}

		return 0;
	};

	void writeYAML(std::ostream & output, const size_t depth=0) const {
		std::string binaryBaseName = mBinary;
		size_t lastSlash = binaryBaseName.find_last_of("/");
		if(lastSlash != std::string::npos) {
			binaryBaseName = binaryBaseName.substr(lastSlash + 1);
		}
		std::string l0(depth * 2, ' ');
		output << l0 << "gitCommitNumber: " << idx::utils::g_GIT_SHA1 << std::endl;
		output << l0 << "dataStructure: " << mDataStructureName <<  std::endl;
		output << l0 << "binary: " << binaryBaseName <<  std::endl;
		output << l0 << "configuration:" << std::endl;
		mBenchmarkConfiguration.writeYAML(output, depth + 1);
		output << l0 << "result:" << std::endl;
		mBenchmarkResults.writeYAML(output, depth + 1);
	}

private:


};

}}

#endif
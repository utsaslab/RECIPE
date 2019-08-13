#ifndef __IDX__BENCHMARK__STRING_BENCHMARK__
#define __IDX__BENCHMARK__STRING_BENCHMARK__

#include <algorithm>
#include <chrono>
#include <iostream>
#include <fstream>
#include <cstdint>
#include <future>
#include <thread>
#include <vector>

#include <boost/hana.hpp>

#include <tbb/task_arena.h>
#include <tbb/task_group.h>
#include <tbb/parallel_for.h>

#include "idx/benchmark/StringBenchmarkCommandlineHelper.hpp"
#include "idx/benchmark/StringBenchmarkConfiguration.hpp"
#include "idx/benchmark/BenchmarkResult.hpp"
#include "idx/utils/GitSHA1.hpp"


#ifdef USE_COUNTERS
#include "PerfEvent.hpp"
#endif

//BUG on ./apps/simd-cob-strings-optimized-incremental-app/simd-cob-strings-optimized-incremental-app -inputFile=/data/rbinna/adobe-hack/emails-validated-random-only-30-characters.txt -lookupModifier=random -size=10000000

constexpr auto hasThreadInfo = boost::hana::is_valid([](auto&& x) -> decltype(x.getThreadInformation()) { });

namespace idx { namespace benchmark {

constexpr auto hasPostInsertOperation = boost::hana::is_valid([](auto&& x) -> decltype(x.postInsertOperation()) { });
constexpr auto requiresAdditionalConfigurationOptions = boost::hana::is_valid([](auto t) -> decltype(
(void)decltype(t)::type::getAdditionalConfigurationOptions
) { });
constexpr auto hasIterateFunctionality = [](auto && x) {
	return boost::hana::is_valid([](auto &&x) -> decltype(x.iterate(std::vector<std::pair<char*, size_t>>())) {})(x) ||
		   boost::hana::is_valid([](auto &&x) -> decltype(x.iterate(x.getThreadInformation(), std::vector<std::pair<char*, size_t>>())) {})(x) ||
		   boost::hana::is_valid([](auto &&x) -> decltype(x.iterate(std::declval<decltype(x.getThreadInformation()) &>(), std::vector<std::pair<char*, size_t>>())) {})(x);
};

constexpr auto hasRemoveFunctionality = [](auto && x) {
	return boost::hana::is_valid([](auto &&x) -> decltype(x.remove(std::pair<char*, size_t>())) {})(x) ||
		   boost::hana::is_valid([](auto &&x) -> decltype(x.remove(x.getThreadInformation(), std::pair<char*, size_t>())) {})(x) ||
		   boost::hana::is_valid([](auto &&x) -> decltype(x.remove(std::declval<decltype(x.getThreadInformation()) &>(), std::pair<char*, size_t>())) {})(x);
};

/**
 * A benchmark for index structures storing zero terminated c-strings
 *
 * To benchmark an index structure it has to provide the following interface:
 *   - bool insert(std::pair<char*, size_t> const & key)  (returns true, if the key was not previously contained)
 *   - bool search(std::pair<char*, size_t> const & lookupKey) (returns true, if the key was actually contained)
 *   - getStatistics (returns statistical information about the index benchmarked index structure. Memory consumption is mandatory, anything else is index structure specific and optional)
 *
 * The following methods are optional:
 *   - bool remove(std::pair<char*, size_t> const & key)  (returns true, if the key to remove was previously contained)
 *   - bool iterate(std::vector<std::pair<char*, size_t>> const & iterateKeys) scans all entries and returns true, if the actually contained entries match the expected keys
 *
 * For concurrent index structures the class to benchmark must provide a threadinfo getThreadInformation which is passed as the first argument to each method.
 * If the actual index does not require a special thread info object, for the sake of simplicity please provide a dummy wrapper parameter.
 *
 * It creates a commandline interface with the following behaviour:
 *
 * Usage: -inputFile=<insertFile> [-insertModifier=<modifierType>] -size=<size> [-lookupModifier=<lookupType>] [-lookupFile=<lookupFileName>] [-help] [-verbose=<true/false>]
 *	description: inserts <size> strings into the benchmarked index structure
 *		After that lookup is executed with either the inserted strings or a new set of string. Both the insertion as well as the lookup order can be modified.
 *		The lookup is executed n times the size of the lookup data set, where n is the smallest natural number which results in at least 100 million lookup operations
 *
 *	parameters:
 *		<insertFile>: The absolute filename of the strings to lookup. Each line of the file contains a single key. The first line contains the total number of keys contained in the file.
 *		<modifierType>: is one of sequential/random/reverse and modifies the input data before it is inserted.
 *		<lookupModifier>: is either a modifier ("sequential"/"random" or "reverse") on the lookupFile or if not provided a modifier on the input
 *			In case no lookup modifer is specified the same data which was used for insert is used for the lookup.
 *		<insertFileName>: It specifies the name of the file containing the input data.
 *		<lookupFileName>: It specifies the name of the file containing the lookup data.
 *
 *	-verbose: specifies whether verbose debug output should be printed or not.
 *	-insertOnly: specifies whether only the insert operation should be executed.
 *	-threads: specifies the number of threads used for insertion as well as lookup operations.
 *	-help: prints this usage message and terminates the application.
 *
 *
 * @tparam Benchmarkable the class to benchmark
 */
template<class Benchmarkable> class StringBenchmark {
	std::string mBinary;
	StringBenchmarkConfiguration mBenchmarkConfiguration;
	Benchmarkable mBenchmarkable;
	BenchmarkResult mBenchmarkResults;
	std::string mDataStructureName;


public:
	template<typename MyBenchmarkable, bool hasAdditionalConfigurationOptions> struct BenchmarkInstantiator {
		static MyBenchmarkable getInstance(StringBenchmarkConfiguration const & /* configuration */) {
			return MyBenchmarkable {};
		}
	};

	template<typename MyBenchmarkable> struct BenchmarkInstantiator<MyBenchmarkable, true> {
		static MyBenchmarkable getInstance(StringBenchmarkConfiguration const & configuration) {
			return MyBenchmarkable { configuration };
		}
	};

	template<typename MyBenchmarkable, bool hasAdditionalConfigurationOptions> struct BenchmarkAdditionalConfigurationExtractor {
		static std::map<std::string, std::string> getAdditionalConfigurationOptions() {
			return std::map<std::string, std::string> {};
		}
	};

	template<typename MyBenchmarkable> struct BenchmarkAdditionalConfigurationExtractor<MyBenchmarkable, true> {
		static std::map<std::string, std::string> getAdditionalConfigurationOptions() {
			return MyBenchmarkable::getAdditionalConfigurationOptions();
		}
	};

	static StringBenchmarkConfiguration getConfiguration(int argc, char** argv, std::string const & dataStructureName) {
		constexpr bool requiresAdditionalConfiguration = requiresAdditionalConfigurationOptions(boost::hana::type_c<Benchmarkable>);

		std::map<std::string, std::string> additionalArguments = BenchmarkAdditionalConfigurationExtractor<
			Benchmarkable, requiresAdditionalConfiguration
		>::getAdditionalConfigurationOptions();

		return StringBenchmarkCommandlineHelper(argc, argv, dataStructureName, additionalArguments).parseArguments();
	}

	static Benchmarkable getBenchmarkableInstance(StringBenchmarkConfiguration const & configuration) {
		constexpr bool requiresAdditionalConfiguration = requiresAdditionalConfigurationOptions(boost::hana::type_c<Benchmarkable>);
		return BenchmarkInstantiator<Benchmarkable, requiresAdditionalConfiguration>::getInstance(configuration);
	}

	StringBenchmark(int argc, char** argv, std::string const & dataStructureName) :
		mBinary(argv[0]),
		mBenchmarkConfiguration{ getConfiguration(argc, argv, dataStructureName) },
		mBenchmarkable { getBenchmarkableInstance(mBenchmarkConfiguration) },
		mBenchmarkResults {},
		mDataStructureName { dataStructureName }
	{
	}

	template<typename Function>
	void benchmarkOperation(std::string const &operationName, std::vector<std::pair<char *, size_t>> const &keys,
														int totalNumberOps, Function functionToBenchmark) {
		OperationResult result;

		uint64_t numberKeys = keys.size();

		auto operationStart = std::chrono::system_clock::now().time_since_epoch();

		bool worked;
#ifdef USE_COUNTERS
		PerfEvent e;
		e.startCounters();
		worked = functionToBenchmark(keys);
		e.stopCounters();
		std::map<std::string, double> profileResults = e.getRawResults(totalNumberOps);
#else
		worked = functionToBenchmark(keys);
#endif
		auto operationEnd = std::chrono::system_clock::now().time_since_epoch();

#ifdef USE_COUNTERS
		result.mCounterValues.insert(profileResults.begin(), profileResults.end());
#endif


		result.mNumberOps = totalNumberOps;
		result.mDurationInNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(operationEnd).count() - std::chrono::duration_cast<std::chrono::nanoseconds>(operationStart).count();
		result.mNumberKeys = numberKeys;
		result.mExecutionState = worked;
		mBenchmarkResults.add(operationName, result);
	}

	int run() {
		std::vector<std::pair<char*, size_t>> & insertKeys = mBenchmarkConfiguration.getInsertStrings();
		tbb::task_arena multithreadedArena(mBenchmarkConfiguration.mNumberThreads, 0);

		benchmarkOperation("insert", insertKeys, insertKeys.size(),
											 [&](std::vector<std::pair<char *, size_t>> const &keys) {
													 bool isMultiThreaded = boost::hana::if_(hasThreadInfo(mBenchmarkable),
																																	 [](auto & /*benchmarkable*/) { return true; },
																																	 [](auto & /*benchmarkable*/) { return false; }
													 )(mBenchmarkable);

													 size_t totalNumberValuesToInsert = keys.size();
													 std::atomic<bool> allThreadSucceeded{true};

													 if (isMultiThreaded) {
														 tbb::task_group insertGroup;
														 multithreadedArena.execute([&] {
																 insertGroup.run([&] { // run in task group
																		 tbb::parallel_for(tbb::blocked_range<size_t>(0, totalNumberValuesToInsert, 10000),
																											 [&](const tbb::blocked_range<size_t> &range) {
																													 bool allInserted = insertRange(range, keys);
																													 if (!allInserted) {
																														 allThreadSucceeded.store(false, std::memory_order_release);
																													 }
																											 });
																 });
														 });
														 insertGroup.wait();
													 } else {
														 allThreadSucceeded.store(
															 insertRange(
																 tbb::blocked_range<size_t>(0, totalNumberValuesToInsert, totalNumberValuesToInsert),
																 keys),
															 std::memory_order_release
														 );
													 }

													 return allThreadSucceeded.load(std::memory_order_acquire) &
																	boost::hana::if_(hasPostInsertOperation(mBenchmarkable),
																									 [](
																										 auto &benchmarkable) { return benchmarkable.postInsertOperation(); },
																									 [](auto & /*benchmarkable*/) { return true; }
																	)(mBenchmarkable);
											 });
		mBenchmarkResults.setIndexStatistics(mBenchmarkable.getStatistics());

		if(!mBenchmarkConfiguration.isInsertOnly()) {
			std::vector<std::pair<char*, size_t>> const &lookupKeys = mBenchmarkConfiguration.getLookupStrings();
			size_t totalNumberLookups = 100'000'000;
			size_t totalNumberKeys = lookupKeys.size();

			std::atomic<bool> allThreadSucceeded { true };

			benchmarkOperation("lookup", lookupKeys, totalNumberLookups,
												 [&](std::vector<std::pair<char *, size_t>> const &keys) {
														 tbb::task_group lookupGroup;
														 multithreadedArena.execute([&] {
																 lookupGroup.run([&] { // run in task group
																		 tbb::parallel_for(tbb::blocked_range<size_t>(0, totalNumberLookups, 10000),
																											 [&](const tbb::blocked_range<size_t> &range) {
																													 bool allLookedUp = boost::hana::if_(
																														 hasThreadInfo(mBenchmarkable),
																														 [&](auto &benchmarkable) {
																																 bool allLookedUp = true;
																																 size_t index = range.begin();
																																 decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
																																 for (size_t i = index; i < range.end(); ++i) {
																																	 index = index < totalNumberKeys ? index : index %
																																																						 totalNumberKeys;
																																	 __builtin_prefetch((keys)[index + 1].first, 0, 0);
																																	 __builtin_prefetch((keys)[index + 1].first + 64, 0,
																																											0);
																																	 allLookedUp &= benchmarkable.search(threadInfo,
																																																			 lookupKeys[index]);
																																	 ++index;
																																 }
																																 return allLookedUp;
																														 },
																														 [&](auto &benchmarkable) {
																																 bool allLookedUp = true;
																																 size_t index = range.begin();
																																 for (size_t i = index; i < range.end(); ++i) {
																																	 index = index < totalNumberKeys ? index : index %
																																																						 totalNumberKeys;
																																	 __builtin_prefetch((keys)[index + 1].first, 0, 0);
																																	 __builtin_prefetch((keys)[index + 1].first + 64, 0,
																																											0);
																																	 allLookedUp &= benchmarkable.search(
																																		 lookupKeys[index]);
																																	 ++index;
																																 }
																																 return allLookedUp;
																														 }
																													 )(mBenchmarkable);


																													 if (!allLookedUp) {
																														 allThreadSucceeded.store(false, std::memory_order_release);
																													 }
																											 });
																 });
														 });

														 lookupGroup.wait();

														 return allThreadSucceeded.load(std::memory_order_acquire);
												 });
		}

		boost::hana::if_(hasIterateFunctionality(mBenchmarkable),
			[&,this](auto && benchmarkable) -> void{
				std::vector<std::pair<char*, size_t>> const &iteratorKeys = mBenchmarkConfiguration.getSortedLookupValues();
				size_t numberThreads = mBenchmarkConfiguration.mNumberThreads;
				uint64_t repeatsPerThread = std::max<uint64_t>(100'000'000u / (insertKeys.size() * numberThreads), 1);
				size_t totalNumberScanOps = repeatsPerThread * numberThreads * insertKeys.size();

					benchmarkOperation("iterate", iteratorKeys, totalNumberScanOps,
														 [&](std::vector<std::pair<char *, size_t>> const &keys) {
																 std::atomic<bool> allThreadSucceeded{true};

																 tbb::task_group scanGroup;
																 multithreadedArena.execute([&] {
																		 scanGroup.run([&] { // run in task group
																				 tbb::parallel_for(
																					 tbb::blocked_range<size_t>(0, repeatsPerThread * numberThreads, 1),
																					 [&](const tbb::blocked_range<size_t> &range) {
																							 bool scannedSuccessfully = boost::hana::if_(
																								 hasThreadInfo(benchmarkable),
																								 [&](auto &benchmarkable) {
																										 bool allScannedSuccesfully = true;
																										 decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
																										 for (size_t i = range.begin(); i < range.end(); ++i) {
																											 allScannedSuccesfully &= benchmarkable.iterate(threadInfo, keys);
																										 }
																										 return allScannedSuccesfully;
																								 },
																								 [&](auto &benchmarkable) {
																										 bool allScannedSuccesfully = true;
																										 for (size_t i = range.begin(); i < range.end(); ++i) {
																											 allScannedSuccesfully &= benchmarkable.iterate(keys);
																										 }
																										 return allScannedSuccesfully;
																								 }
																							 )(benchmarkable);

																							 if (!scannedSuccessfully) {
																								 allThreadSucceeded.store(false, std::memory_order_release);
																							 }
																					 });
																		 });
																 });
																 scanGroup.wait();

																 return allThreadSucceeded.load(std::memory_order_acquire);
														 });
			},
			[&](auto && benchmarkable) -> void{ }
		)(mBenchmarkable);

		boost::hana::if_(hasRemoveFunctionality(mBenchmarkable),
			[&,this](auto && benchmarkable) -> void {
					benchmarkOperation("delete", insertKeys, insertKeys.size(),
														 [&](std::vector<std::pair<char *, size_t>> const &keys) {
																 bool isMultiThreaded = boost::hana::if_(hasThreadInfo(benchmarkable),
																																				 [](auto & /* benchmarkable */) { return true; },
																																				 [](auto & /* benchmarkable */) { return false; }
																 )(benchmarkable);

																 size_t totalNumberValuesToDelete = keys.size();
																 std::atomic<bool> allThreadSucceeded{true};

																 if (isMultiThreaded) {
																	 tbb::task_group insertGroup;
																	 multithreadedArena.execute([&] {
																			 insertGroup.run([&] { // run in task group
																					 tbb::parallel_for(
																						 tbb::blocked_range<size_t>(0, totalNumberValuesToDelete, 10000),
																						 [&](const tbb::blocked_range<size_t> &range) {
																								 bool allInserted = deleteRange(range, keys);
																								 if (!allInserted) {
																									 allThreadSucceeded.store(false,
																																						std::memory_order_release);
																								 }
																						 });
																			 });
																	 });
																	 insertGroup.wait();
																 } else {
																	 allThreadSucceeded.store(
																		 deleteRange(tbb::blocked_range<size_t>(0, totalNumberValuesToDelete,
																																						totalNumberValuesToDelete),
                                                 keys),
																		 std::memory_order_release);
																 }

																 return allThreadSucceeded.load(std::memory_order_acquire);
														 });
			},
			[&](auto && /* benchmarkable */) -> void{ }
		)(mBenchmarkable);

		writeYAML(std::cout);


		if(mBenchmarkConfiguration.shouldWriteDotRepresentation()) {
			std::ofstream output(mBenchmarkConfiguration.getDotFileLocation());
			mBenchmarkable.writeDotRepresentation(output);
		}

		return 0;
	};

	bool insertRange(const tbb::blocked_range<size_t>& range, std::vector<std::pair<char*, size_t>> const & insertKeys) {
		return boost::hana::if_(hasThreadInfo(mBenchmarkable),
			[&](auto & benchmarkable) {
				bool allInserted = true;
				decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
				for(size_t i = range.begin(); i < range.end(); ++i) {
					__builtin_prefetch((insertKeys)[i + 1].first, 0, 0);
					__builtin_prefetch((insertKeys)[i + 1].first + 64, 0, 0);
					allInserted = allInserted & benchmarkable.insert(threadInfo, insertKeys[i]);
				}
				return allInserted;
			},
			[&](auto & benchmarkable) {
				bool allInserted = true;
				for(size_t i = range.begin(); i < range.end(); ++i) {
					__builtin_prefetch((insertKeys)[i + 1].first, 0, 0);
					__builtin_prefetch((insertKeys)[i + 1].first + 64, 0, 0);
					allInserted = allInserted & benchmarkable.insert(insertKeys[i]);
				}
				return allInserted;
			}
		)(mBenchmarkable);
	}

	bool deleteRange(const tbb::blocked_range<size_t>& range, std::vector<std::pair<char*, size_t>> const & deleteKeys) {
		return boost::hana::if_(hasThreadInfo(mBenchmarkable),
			[&](auto & benchmarkable) {
				bool allDeleted = true;
				decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
				for(size_t i = range.begin(); i < range.end(); ++i) {
					__builtin_prefetch((deleteKeys)[i + 1].first, 0, 0);
					__builtin_prefetch((deleteKeys)[i + 1].first + 64, 0, 0);
					allDeleted = allDeleted & benchmarkable.remove(threadInfo, deleteKeys[i]);
				}
				return allDeleted;
			},
			[&](auto & benchmarkable) {
				bool allDeleted = true;
				for(size_t i = range.begin(); i < range.end(); ++i) {
					__builtin_prefetch((deleteKeys)[i + 1].first, 0, 0);
					__builtin_prefetch((deleteKeys)[i + 1].first + 64, 0, 0);
					allDeleted = allDeleted & benchmarkable.remove(deleteKeys[i]);
				}
				return allDeleted;
			}
		)(mBenchmarkable);
	}

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
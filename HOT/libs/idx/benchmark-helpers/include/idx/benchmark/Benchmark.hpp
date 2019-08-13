#ifndef __IDX__BENCHMARK__BENCHMARK__
#define __IDX__BENCHMARK__BENCHMARK__

#include <algorithm>
#include <chrono>
#include <iostream>
#include <cstdint>
#include <thread>
#include <future>

#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>
#include <tbb/task_group.h>
#include <tbb/task_arena.h>

#include <boost/hana.hpp>


#ifdef USE_COUNTERS
#include "PerfEvent.hpp"
#endif

#include "idx/benchmark/BenchmarkCommandlineHelper.hpp"
#include "idx/benchmark/BenchmarkConfiguration.hpp"
#include "idx/benchmark/BenchmarkResult.hpp"
#include "idx/benchmark/NoThreadInfo.hpp"
#include "idx/utils/GitSHA1.hpp"

namespace idx { namespace benchmark {

constexpr auto hasPostInsertOperation = boost::hana::is_valid([](auto&& x) -> decltype(x.postInsertOperation()) { });
constexpr auto requiresAdditionalConfigurationOptions = boost::hana::is_valid([](auto t) -> decltype(
	(void)decltype(t)::type::getAdditionalConfigurationOptions
) { });



constexpr auto hasIterateAllFunctionality = [](auto && x) {
	return boost::hana::is_valid([](auto &&x) -> decltype(x.iterateAll(std::vector<uint64_t>())) {})(x) ||
		boost::hana::is_valid([](auto &&x) -> decltype(x.iterateAll(x.getThreadInformation(), std::vector<uint64_t>())) {})(x) ||
		boost::hana::is_valid([](auto &&x) -> decltype(x.iterateAll(std::declval<decltype(x.getThreadInformation()) &>(), std::vector<uint64_t>())) {})(x);
};

constexpr auto hasDeletionFunctionality = [](auto && x) {
	return boost::hana::is_valid([](auto &&x) -> decltype(x.remove(uint64_t())) {})(x) ||
		   boost::hana::is_valid([](auto &&x) -> decltype(x.remove(x.getThreadInformation(), uint64_t())) {})(x) ||
		   boost::hana::is_valid([](auto &&x) -> decltype(x.remove(std::declval<decltype(x.getThreadInformation()) &>(), uint64_t())) {})(x);
};

constexpr auto hasThreadInfo = boost::hana::is_valid([](auto&& x) -> decltype(x.getThreadInformation()) { });

template<typename Benchmarkable> auto getThreadInfo(Benchmarkable & benchmarkable) {
	return boost::hana::if_(hasThreadInfo(benchmarkable),
		   [](auto & benchmarkable) { return benchmarkable.getThreadInformation(); },
		   [](auto & benchmarkable) { return NoThreadInfo {}; }
	)(benchmarkable);
}

/**
 * A benchmark for index structures storing 64-bit integer keys.
 *
 * To benchmark an index structure it has to provide the following interface:
 *   - bool insert(uint64_t key) (returns true, if the key was not previously contained)
 *   - bool search([], uint64_t key) (returns true, if the key was actually contained)
 *   - getStatistics (returns statistical information about the index benchmarked index structure. Memory consumption is mandatory, anything else is index structure specific and optional)
 *
 * The following methods are optional:
 *   - bool remove(uint64_t key) (returns true, if the key to remove was previously contained)
 *   - bool iterateAll(std::vector<uint64_t> expectedKeys) scans all entries and returns true, if the actually contained entries match the expected keys
 *
 * For concurrent index structures the class to benchmark must provide a threadinfo getThreadInformation which is passed as the first argument to each method.
 * If the actual index does not require a special thread info object, for the sake of simplicity please provide a dummy wrapper parameter.
 *
 * It creates a commandline interface with the following behaviour:
 *
 * Usage: hot-single-threaded-integer-benchmark -insert=<insertType> [-insertModifier=<modifierType>] [-input=<insertFileName>] -size=<size> [-lookup=<lookupType>] [-lookupFile=<lookupFileName>] [-help] [-verbose=<true/false>] [-insertModifier=<true/false>]
 *	description: hot-single-threaded-integer-benchmark/hot-single-threaded-integer-benchmark
 *		inserts <size> values of a given generator type (<insertType>) into the index structure
 *		After that lookup is executed with provided data. Either a modification of the input type or a separate data file
 *		The lookup is executed n times the size of the lookup data set, where n is the smallest natural number which results in at least 100 million lookup operations
 *
 *	switches:
 *		-insert: specifies the distribution which will be used to insert data
 *		-size: specifies the number of values to insert
 *		-insertModifier: specifies the order to insert the values
 *		-lookupModifier: specifies the order to lookup the values.
 *		-input: specifies the file to read data from in case the insertType is file.
 *			Each line of the file contains a single value.
 *			The first line contains the total number of values in the file.
 *		-help: show the usage dialog.
 *		-insertOnly: specifies whether only the insert operation should be executed.
 *		-threads: specifies the number of threads used for inserts as well as lookups.
 *		-verbose: specifies to show debug messages.
 *
 *	potential parameter values:
 *		<insertType>: is either dense/pseudorandom/random or file. In case of file the -insertFile parameter must be provided.
 *		<modifierType>: is on of sequential/random/reverse and modifies the input data before it is inserted.
 *		<lookupType>: is either a modifier ("sequential"/"random" or "reverse") on the input data which will be used to modify the input data before executing the lookup
 *			or "file" which requires the additional parameter lookupFile specifying a file containing  the lookup data.
 *			In case no lookup type is specified the same data which was used for insert is used for the lookup.
 *		<insertFileName>: Only used in case the <modifierType> "file" is specified. It specifies the name of a dat file containing the input data.
 *		<insertFileName>: Only used in case the <lookupType> "file" is specified. It specifies the name of a dat file containing the lookup data.
 *
 * @tparam Benchmarkable the class to benchmark
 */
template<class Benchmarkable> class Benchmark {
	// Check if a type has a serialize method.
	std::string mBinary;
	BenchmarkConfiguration mBenchmarkConfiguration;
	Benchmarkable mBenchmarkable;
	BenchmarkResult mBenchmarkResults;
	std::string mDataStructureName;


public:
	template<typename MyBenchmarkable, bool hasAdditionalConfigurationOptions> struct BenchmarkInstantiator {
		static MyBenchmarkable getInstance(BenchmarkConfiguration const & /* configuration */) {
			return MyBenchmarkable {};
		}
	};

	template<typename MyBenchmarkable> struct BenchmarkInstantiator<MyBenchmarkable, true> {
		static MyBenchmarkable getInstance(BenchmarkConfiguration const & configuration) {
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

	static BenchmarkConfiguration getConfiguration(int argc, char** argv, std::string const & dataStructureName) {
		constexpr bool requiresAdditionalConfiguration = requiresAdditionalConfigurationOptions(boost::hana::type_c<Benchmarkable>);

		std::map<std::string, std::string> additionalArguments = BenchmarkAdditionalConfigurationExtractor<
			Benchmarkable, requiresAdditionalConfiguration
		>::getAdditionalConfigurationOptions();

		return BenchmarkCommandlineHelper(argc, argv, dataStructureName, additionalArguments).parseArguments();
	}

	static Benchmarkable getBenchmarkableInstance(BenchmarkConfiguration const & configuration) {
		constexpr bool requiresAdditionalConfiguration = requiresAdditionalConfigurationOptions(boost::hana::type_c<Benchmarkable>);
		return BenchmarkInstantiator<Benchmarkable, requiresAdditionalConfiguration>::getInstance(configuration);
	}


	Benchmark(int argc, char** argv, std::string const & dataStructureName) :
		mBinary(argv[0]),
		mBenchmarkConfiguration{ getConfiguration(argc, argv, dataStructureName) },
		mBenchmarkable { getBenchmarkableInstance(mBenchmarkConfiguration) },
		mBenchmarkResults {},
		mDataStructureName { dataStructureName }
	{
	}


	template<typename Function>
	void benchmarkOperation(std::string const &operationName, std::vector<uint64_t> const &keys, size_t executedOperations,
														Function functionToBenchmark) {
		OperationResult result;

		uint64_t numberKeys = keys.size();

		auto operationStart = std::chrono::system_clock::now().time_since_epoch();
		bool worked = true;
#ifdef USE_COUNTERS
		PerfEvent e;
		e.startCounters();
		worked = functionToBenchmark(keys);
		e.stopCounters();
		std::map<std::string, double> profileResults = e.getRawResults(executedOperations);
#else
		worked = functionToBenchmark(keys);
#endif
		auto operationEnd = std::chrono::system_clock::now().time_since_epoch();

#ifdef USE_COUNTERS
		result.mCounterValues.insert(profileResults.begin(), profileResults.end());
#endif

		result.mNumberOps = executedOperations;
		result.mDurationInNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(operationEnd).count() - std::chrono::duration_cast<std::chrono::nanoseconds>(operationStart).count();
		result.mNumberKeys = numberKeys;
		result.mExecutionState = worked;
		mBenchmarkResults.add(operationName, result);
	}

	bool insertRange(const tbb::blocked_range<size_t>& range, std::vector<uint64_t> const & insertKeys) {
		return boost::hana::if_(hasThreadInfo(mBenchmarkable),
			[&](auto & benchmarkable) {
				bool allInserted = true;
				decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
				for(size_t i = range.begin(); i < range.end(); ++i) {
					allInserted &= benchmarkable.insert(threadInfo, insertKeys[i]);
				}
				return allInserted;
			},
			[&](auto & benchmarkable) {
				bool allInserted = true;
				for(size_t i = range.begin(); i < range.end(); ++i) {
					allInserted &= benchmarkable.insert(insertKeys[i]);
				}
				return allInserted;
			}
		)(mBenchmarkable);
	}

	bool deleteEntriesInRange(const tbb::blocked_range<size_t>& range, std::vector<uint64_t> const & keysToDelete) {
		return boost::hana::if_(hasThreadInfo(mBenchmarkable),
								[&](auto & benchmarkable) {
									bool allDeleted = true;
									decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
									for(size_t i = range.begin(); i < range.end(); ++i) {
										allDeleted &= benchmarkable.remove(threadInfo, keysToDelete[i]);
									}
									return allDeleted;
								},
								[&](auto & benchmarkable) {
									bool allDeleted = true;
									for(size_t i = range.begin(); i < range.end(); ++i) {
										/*if(i == 29637030) {
											std::cout << "FIRST ERROR" << std::endl;
										}
										if(i == 39506021)  {
											std::cout << "Before segfault" << std::endl;
										}*/
										allDeleted &= benchmarkable.remove(keysToDelete[i]);
									}
									return allDeleted;
								}
		)(mBenchmarkable);
	}

	int run() {
		std::vector<uint64_t> & insertKeys = mBenchmarkConfiguration.getInsertValues();
		tbb::task_arena multithreadedArena(mBenchmarkConfiguration.mNumberThreads, 0);
		if(mBenchmarkConfiguration.mNumberThreads == 1) {
			multithreadedArena.terminate();
		}

		benchmarkOperation("insert", insertKeys, insertKeys.size(), [&, this](std::vector<uint64_t> const &keys) {
				bool isMultiThreaded = boost::hana::if_(hasThreadInfo(mBenchmarkable),
																								[](auto & /* benchmarkable */) { return true; },
																								[](auto & /* benchmarkable */) { return false; }
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
						insertRange(tbb::blocked_range<size_t>(0, totalNumberValuesToInsert, totalNumberValuesToInsert), keys),
						std::memory_order_release
					);
				}

				return allThreadSucceeded.load(std::memory_order_acquire) &
							 boost::hana::if_(hasPostInsertOperation(mBenchmarkable),
																[](auto &benchmarkable) { return benchmarkable.postInsertOperation(); },
																[](auto & /* benchmarkable */ ) { return true; }
							 )(mBenchmarkable);
				});
		mBenchmarkResults.setIndexStatistics(mBenchmarkable.getStatistics());

		//std::cout << "Insert Finished" << std::endl;
		if(!mBenchmarkConfiguration.isInsertOnly()) {
			std::vector<uint64_t> const &lookupKeys = mBenchmarkConfiguration.getLookupValues();
			size_t totalNumberLookups = 100'000'000;

			benchmarkOperation("lookup", lookupKeys, totalNumberLookups, [&](std::vector<uint64_t> const &keys) {
					size_t totalNumberKeys = lookupKeys.size();

					std::atomic<bool> allThreadSucceeded{true};

					if (mBenchmarkConfiguration.mNumberThreads > 1) {
						tbb::task_group lookupGroup;
						multithreadedArena.execute([&] {
								lookupGroup.run([&] { // run in task group
										tbb::parallel_for(tbb::blocked_range<size_t>(0, totalNumberLookups, 10000),
																			[&](const tbb::blocked_range<size_t> &range) {
																					bool allLookedUp = boost::hana::if_(hasThreadInfo(mBenchmarkable),
																																							[&](auto &benchmarkable) {
																																									bool allLookedUp = true;
																																									size_t index = range.begin();
																																									decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
																																									for (size_t i = index;
																																											 i < range.end(); ++i) {
																																										index =
																																											index < totalNumberKeys ? index :
																																											index % totalNumberKeys;
																																										allLookedUp &= benchmarkable.search(
																																											threadInfo, keys[index]);
																																										++index;
																																									}
																																									return allLookedUp;
																																							},
																																							[&](auto &benchmarkable) {
																																									bool allLookedUp = true;
																																									size_t index = range.begin();
																																									for (size_t i = index;
																																											 i < range.end(); ++i) {
																																										index =
																																											index < totalNumberKeys ? index :
																																											index % totalNumberKeys;
																																										allLookedUp &= benchmarkable.search(
																																											keys[index]);
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
					} else {
						return boost::hana::if_(hasThreadInfo(mBenchmarkable),
																		[&](auto &benchmarkable) {
																				bool allLookedUp = true;
																				decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
																				size_t index = 0;
																				for (size_t i = 0; i < totalNumberLookups; ++i) {
																					index = index < totalNumberKeys ? index : 0;
																					allLookedUp &= benchmarkable.search(threadInfo, lookupKeys[index]);
																					++index;
																				}
																				return allLookedUp;

																		},
																		[&](auto &benchmarkable) {
																				bool allLookedUp = true;
																				size_t index = 0;
																				for (size_t i = 0; i < totalNumberLookups; ++i) {
																					index = index < totalNumberKeys ? index : 0;
																					allLookedUp &= benchmarkable.search(lookupKeys[index]);
																					++index;
																				}
																				return allLookedUp;
																		}
						)(mBenchmarkable);
					}
						});
		}
		if(!mBenchmarkConfiguration.isInsertOnly()) {
			boost::hana::if_(hasIterateAllFunctionality(mBenchmarkable),
				[&,this](auto && benchmarkable) -> void{
					size_t numberThreads = mBenchmarkConfiguration.mNumberThreads;
					std::vector<uint64_t> sortedKeys(insertKeys.begin(), insertKeys.end());
					std::sort(sortedKeys.begin(), sortedKeys.end());
					uint64_t repeatsPerThread = std::max<uint64_t>(100'000'000u / (sortedKeys.size() * numberThreads), 1);
					size_t totalNumberScanOps = repeatsPerThread * numberThreads * sortedKeys.size();

					if(mBenchmarkConfiguration.mNumberThreads > 1) {
						this->benchmarkOperation("iterate-all", sortedKeys, totalNumberScanOps,
																		 [&](std::vector<uint64_t> const &keys) {
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
																															 allScannedSuccesfully &= benchmarkable.iterateAll(
																																 threadInfo, keys);
																														 }
																														 return allScannedSuccesfully;
																												 },
																												 [&](auto &benchmarkable) {
																														 bool allScannedSuccesfully = true;
																														 for (size_t i = range.begin(); i < range.end(); ++i) {
																															 allScannedSuccesfully &= benchmarkable.iterateAll(keys);
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
					} else {
						this->benchmarkOperation("iterate-all", sortedKeys, totalNumberScanOps,
																		 [&](std::vector<uint64_t> const &keys) {
																				 return boost::hana::if_(hasThreadInfo(mBenchmarkable),
																																 [&](auto &benchmarkable) {
																																		 bool allScannedSuccesfully = true;
																																		 decltype(benchmarkable.getThreadInformation()) threadInfo = benchmarkable.getThreadInformation();
																																		 for (size_t i = 0; i < repeatsPerThread; ++i) {
																																			 allScannedSuccesfully &= benchmarkable.iterateAll(
																																				 threadInfo, keys);
																																		 }
																																		 return allScannedSuccesfully;
																																 },
																																 [&](auto &benchmarkable) {
																																		 bool allScannedSuccesfully = true;
																																		 for (size_t i = 0; i < repeatsPerThread; ++i) {
																																			 allScannedSuccesfully &= benchmarkable.iterateAll(
																																				 keys);
																																		 }
																																		 return allScannedSuccesfully;
																																 }
																				 )(benchmarkable);
																		 });
					}
				},
				[&](auto && /* benchmarkable */) -> void{ }
			)(mBenchmarkable);
		}

		boost::hana::if_(hasDeletionFunctionality(mBenchmarkable),
			 [&,this](auto && benchmarkable) -> void {
					 benchmarkOperation("delete", insertKeys, insertKeys.size(), [&, this](std::vector<uint64_t> const &keys) {
							 bool isMultiThreaded = boost::hana::if_(hasThreadInfo(benchmarkable),
																											 [](
																												 auto & /*benchmarkable*/) { return true; },
																											 [](
																												 auto & /*benchmarkable*/) { return false; }
							 )(benchmarkable);


							 size_t totalNumberValuesToDelete = keys.size();
							 std::atomic<bool> allThreadSucceeded{true};

							 if (isMultiThreaded) {
								 tbb::task_group insertGroup;
								 multithreadedArena.execute([&] {
										 insertGroup.run([&] { // run in task group
												 tbb::parallel_for(
													 tbb::blocked_range<size_t>(0, totalNumberValuesToDelete,
																											10000),
													 [&](const tbb::blocked_range<size_t> &range) {
															 bool allInserted = deleteEntriesInRange(range, keys);
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
									 deleteEntriesInRange(
										 tbb::blocked_range<size_t>(0, totalNumberValuesToDelete,
																								totalNumberValuesToDelete), keys),
									 std::memory_order_release
								 );
							 }

							 return allThreadSucceeded.load(std::memory_order_acquire);
					 });
			 },
			[&](auto && /* benchmarkable */) -> void{ }
		)(mBenchmarkable);

		writeYAML(std::cout);

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
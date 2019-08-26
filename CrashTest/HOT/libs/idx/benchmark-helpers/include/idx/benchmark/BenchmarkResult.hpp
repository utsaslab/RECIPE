#ifndef __IDX__BENCHMARK__BENCHMARK_RESULT__
#define __IDX__BENCHMARK__BENCHMARK_RESULT__

#include <cstdint>
#include <map>
#include <string>
#include <ostream>

#include <type_traits>

#define CREATE_MEMBER_DETECTOR(X)                                                   \
template<typename T> class Detect_##X {                                             \
    struct Fallback { int X; };                                                     \
    struct Derived : T, Fallback { };                                               \
                                                                                    \
    template<typename U, U> struct Check;                                           \
                                                                                    \
    typedef char ArrayOfOne[1];                                                     \
    typedef char ArrayOfTwo[2];                                                     \
                                                                                    \
    template<typename U> static ArrayOfOne & func(Check<int Fallback::*, &U::X> *); \
    template<typename U> static ArrayOfTwo & func(...);                             \
  public:                                                                           \
    typedef Detect_##X type;                                                        \
    enum { value = sizeof(func<Derived>(0)) == 2 };                                 \
};

CREATE_MEMBER_DETECTOR(writeYAML);


namespace idx { namespace benchmark {

constexpr double ONE_MILLION = 1000000;
constexpr double ONE_SECOND_IN_NANOS = 1000000000ul;

template<bool theSwitch> struct SwitchingParam {};

template<typename T, typename SwitchToMemberFunction> void writeToYAMLObject(std::map<std::string, T> map, std::ostream & output, const size_t depth, SwitchToMemberFunction theSwitcher)
{
	//Stub which will never be called
}

template<typename T> void writeToYAMLObject(std::map<std::string, T> map, std::ostream & output, const size_t depth, SwitchingParam<false> theSwitcher)
{
	std::string l0(depth * 2, ' ');

	for(std::pair<std::string, T> const & information : map) {
		output << l0 << information.first << ": " << information.second << std::endl;
	}
}


template<typename T> void writeToYAMLObject(std::map<std::string, T> map, std::ostream & output, const size_t depth, SwitchingParam<true> /* theSwitcher */)
{
	std::string l0(depth * 2, ' ');

	for(std::pair<std::string, T> const & information : map) {
		output << l0 << information.first << ": " << std::endl;
		information.second.writeYAML(output, depth + 1);
	}
}

template<typename T, typename SwitchToFundamental> void writeToYAMLGeneric(std::map<std::string, T> map, std::ostream & output, const size_t depth, SwitchToFundamental /* theSwitcher */) {
	constexpr bool hasWriteYAML = Detect_writeYAML<T>::value;

	writeToYAMLObject(map, output, depth, SwitchingParam<hasWriteYAML> {});

}


template<typename T> void writeToYAMLGeneric(std::map<std::string, T> map, std::ostream & output, const size_t depth, SwitchingParam<true> /* theSwitcher */) {
	std::string l0(depth * 2, ' ');

	for(std::pair<std::string, T> const & information : map) {
		output << l0 << information.first << ": " << information.second << std::endl;

	}
}

template<typename T> void writeToYAML(std::map<std::string, T> map, std::ostream & output, const size_t depth=0) {
	writeToYAMLGeneric<T>(map, output, depth, SwitchingParam<std::is_fundamental<T>::value> {});
}


struct OperationResult {
	bool mExecutionState;
	int64_t mDurationInNanos;
	uint64_t mNumberKeys;
	uint64_t mNumberOps;

	OperationResult() {
		mExecutionState = false;
	}

#if defined(USE_PAPI) || defined(USE_COUNTERS)
	std::map<std::string, double> mCounterValues;

	OperationResult(bool executionState, int64_t durationInNanos, uint64_t numberKeys, uint64_t numberOps)
		: mExecutionState(executionState), mDurationInNanos(durationInNanos), mNumberKeys(numberKeys), mNumberOps(numberOps), mCounterValues()
	{
	}

	OperationResult(bool executionState, int64_t durationInNanos, uint64_t numberKeys, uint64_t numberOps, std::map<std::string, double> const & counterValues)
		: mExecutionState(executionState), mDurationInNanos(durationInNanos), mNumberKeys(numberKeys), mNumberOps(numberOps), mCounterValues(counterValues)
	{
	}
#else
	OperationResult(bool executionState, uint64_t durationInNanos, uint64_t numberKeys, uint64_t numberOps)
		: mExecutionState(executionState), mDurationInNanos(durationInNanos), mNumberKeys(numberKeys), mNumberOps(numberOps)
	{
	}
#endif



	double getMillionOpsPerSecond() const {
		double timeInSeconds = mDurationInNanos/ONE_SECOND_IN_NANOS;
		double millionOps = (mNumberOps/ONE_MILLION);
		return millionOps/timeInSeconds;
	}

	void writeYAML(std::ostream & output, const size_t depth) const {
		std::string l0(depth * 2 , ' ');

		output << l0 << "executionState: " << mExecutionState << std::endl;
		output << l0 << "timeInNs: " << mDurationInNanos << std::endl;
		output << l0 << "numberKeys: " << mNumberKeys << std::endl;
		output << l0 << "numberOps: " << mNumberOps << std::endl;

#if defined(USE_PAPI) || defined(USE_COUNTERS)
		for(std::pair<std::string, double> const & counterValue : mCounterValues) {
			output << l0 << counterValue.first << ": " << counterValue.second << std::endl;
		}


#endif

#if defined(USE_PAPI)
		if((mCounterValues.find("numberInstructions") != mCounterValues.end()) && (mCounterValues.find("numberCycles") != mCounterValues.end())) {
			output << l0 << "ipc: " << (mCounterValues.at("numberInstructions")/static_cast<double>(mCounterValues.at("numberCycles"))) << std::endl;
		}
#endif

		output << l0 << "millionOpsPerSecond: " << getMillionOpsPerSecond() << std::endl;
	}
};

/**
 * Helper object to collect statistics about the used index structure.
 * The total amount of memory required by the index structure is mandatory, all other collected statistics are
 * purely optional and dependent on the actual index structure
 */
struct IndexStatistics {
	size_t total;
	std::map<std::string, double> additionalInformation;

	void writeYAML(std::ostream & output, const size_t depth=0) const {
		std::string l0(depth * 2, ' ');

		output << l0 << "total: " << total << std::endl;
		output << l0 << "additional:" << std::endl;
		writeToYAML(additionalInformation, output, depth+1);
	}
};

struct BenchmarkResult {

private:
	IndexStatistics memoryInformations;
	std::map<std::string, OperationResult> results;

public:
	std::map<std::string, OperationResult> & getResults() {
		return results;
	}

	std::map<std::string, OperationResult> getResults() const {
		return results;
	}

	void add(const std::string &operationName, const OperationResult &result) {
		results[operationName] = result;
	}

	void setIndexStatistics(IndexStatistics const &memoryConsumption) {
		memoryInformations = memoryConsumption;
	}

	void writeYAML(std::ostream &output, size_t depth = 0) const {
		std::string l0(depth * 2, ' ');

		output << l0 << "runtime" << ":" << std::endl;
		writeToYAML(results, output, depth + 1);
		output << l0 << "memory" << ":" << std::endl;
		memoryInformations.writeYAML(output, depth + 1);
	}
};
}}

#endif
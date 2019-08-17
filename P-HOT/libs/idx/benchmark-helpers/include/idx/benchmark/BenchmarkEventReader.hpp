#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT_READER__HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT_READER__HPP__

#include <cstdlib>

#include <fstream>
#include <sstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include <idx/contenthelpers/ValueToKeyTypeMapper.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#include <idx/maphelpers/STLLikeIndex.hpp>

#include "idx/benchmark/BenchmarkEventList.hpp"
#include "idx/benchmark/BenchmarkEventListBackingBufferSizeEstimator.hpp"
#include "idx/benchmark/BenchmarkEventReaderException.hpp"
#include "idx/benchmark/BenchmarkEventTypeId.hpp"
#include "idx/benchmark/BenchmarkEventTypeIdToEventTypeMapping.hpp"
#include "idx/benchmark/ContentReader.hpp"
#include "idx/benchmark/SpecificEventProcessor.hpp"

#include "idx/benchmark/InsertEvent.hpp"
#include "idx/benchmark/UpdateEvent.hpp"
#include "idx/benchmark/LookupEvent.hpp"
#include "idx/benchmark/ScanEvent.hpp"
#include "BenchmarkLineReader.hpp"

namespace idx { namespace benchmark {

template<typename ValueType, template <typename> typename KeyExtractor> class BenchmarkEventReader {
	using KeyType = typename idx::contenthelpers::ValueToKeyTypeMapper<ValueType, KeyExtractor>::KeyType;
	BenchmarkLineReader<ValueType, KeyExtractor> readBenchmarkLine;
	SpecificEventProcessor<ValueType, KeyExtractor> processEvent;
private:
	std::string mEventsSourceFile;

public:
	using CurentEventsTemporaryIndexType = idx::maphelpers::STLLikeIndex<std::map, ValueType, KeyExtractor>;

	BenchmarkEventReader(std::string const & eventsSourceFile) : mEventsSourceFile(eventsSourceFile) {
	}

	BenchmarkEventList readEvents() {
		CurentEventsTemporaryIndexType currentEvents;
		return readEvents(currentEvents);
	}

	BenchmarkEventList readEvents(CurentEventsTemporaryIndexType & currentEvents) {
		BenchmarkEventList eventList { estimateSize() };

		processLines([&eventList, &currentEvents, this](std::string const & line, size_t lineNumber) {
			BenchmarkBaseEvent* baseEvent = eventList.addEvent(this->readBenchmarkLine, line, lineNumber);
			this->initializeEvent(baseEvent, currentEvents);
		});
		return eventList;
	}


private:
	size_t estimateSize() {
		BenchmarkEventListBackingBuferSizeEstimator sizeEstimator { };
		processLines([&sizeEstimator, this](std::string const & line, size_t lineNumber) {
			sizeEstimator.addEvent(this->readBenchmarkLine, line, lineNumber);
		});
		return sizeEstimator.getTotalBufferSize();
	}

	template<typename LineProcessor> void processLines(LineProcessor processLine) {
		std::ifstream input { mEventsSourceFile };
		size_t lineNumber = 1ul;
		std::string line;
		while(input.good() && std::getline(input, line)) {
			processLine(line, lineNumber++);
		}
	}

	void initializeEvent(BenchmarkBaseEvent* baseEvent, idx::maphelpers::STLLikeIndex<std::map, ValueType, KeyExtractor> & currentEntries) {
		processEvent(baseEvent,[this, &currentEntries](auto & event) {
			return this->initializeEvent(event, currentEntries);
		});
	}

	inline void initializeEvent(InsertEvent<ValueType, KeyExtractor> & insertEvent, idx::maphelpers::STLLikeIndex<std::map, ValueType, KeyExtractor> & currentEntries) {
		currentEntries.insert(insertEvent.mValueToInsert);
	}

	inline void initializeEvent(LookupEvent<ValueType, KeyExtractor> & lookupEvent, idx::maphelpers::STLLikeIndex<std::map, ValueType, KeyExtractor> & currentEntries) {
		lookupEvent.mExpectedValue = currentEntries.lookup(lookupEvent.mKey);
	}

	inline void initializeEvent(ScanEvent<ValueType, KeyExtractor> & scanEvent, idx::maphelpers::STLLikeIndex<std::map, ValueType, KeyExtractor> & currentEntries) {
		scanEvent.mLastResult = currentEntries.scan(scanEvent.mLookupKey, scanEvent.mNumberValuesToScan);
	}

	inline void initializeEvent(UpdateEvent<ValueType, KeyExtractor> & updateEvent, idx::maphelpers::STLLikeIndex<std::map, ValueType, KeyExtractor> & currentEntries) {
		updateEvent.mPreviousValue = currentEntries.upsert(updateEvent.mNewValue);
	}
};


} }

#endif
#ifndef __IDX__BENCHMARK__BENCHMARK_LINE_READER__HPP__
#define __IDX__BENCHMARK__BENCHMARK_LINE_READER__HPP__

#include <string>
#include <sstream>
#include <utility>

#include "idx/benchmark/BenchmarkBaseEvent.hpp"
#include "idx/benchmark/BenchmarkEventReaderException.hpp"
#include "idx/benchmark/ContentReader.hpp"

#include "idx/benchmark/InsertEvent.hpp"
#include "idx/benchmark/LookupEvent.hpp"
#include "idx/benchmark/ScanEvent.hpp"
#include "idx/benchmark/UpdateEvent.hpp"

#include "BenchmarkEvent.hpp"

#include <unordered_map>

#include <idx/benchmark/ContinuosBuffer.hpp>

namespace idx { namespace benchmark {

static std::unordered_map<std::string, BenchmarkEventTypeId> const BENCHMARK_EVENT_NAME_TYPE_MAPPING = {
	{ "INSERT", BenchmarkEventTypeId::InsertEventTypeId },
	{ "READ", BenchmarkEventTypeId::LookupEventTypeId },
	{ "UPDATE", BenchmarkEventTypeId::UpdateEventTypeId },
	{ "SCAN", BenchmarkEventTypeId::ScanEventTypeId }
};


template<typename ValueType, template <typename> typename KeyExtractor> struct BenchmarkLineReader {


public:
	inline std::pair<BenchmarkBaseEvent*, ContinuousBuffer> operator()(std::string const & line, size_t lineNumber, ContinuousBuffer const & continuousBuffer) const {
		std::string eventTypeString;

		std::istringstream lineStream(line);
		lineStream >> eventTypeString;

		if(!lineStream.good()) {
			throw idx::benchmark::BenchmarkEventReaderException("Input Stream is corrupted", line, lineNumber, __FILE__, __LINE__);
		}

		const std::unordered_map<std::string, BenchmarkEventTypeId>::const_iterator & matchingEventType = BENCHMARK_EVENT_NAME_TYPE_MAPPING.find(eventTypeString);
		if(matchingEventType == BENCHMARK_EVENT_NAME_TYPE_MAPPING.end()) {
			throw idx::benchmark::BenchmarkEventReaderException("Could not find a event of specified type from line", line, lineNumber, __FILE__, __LINE__);
		}

		return readEvent(line, lineNumber, lineStream, matchingEventType->second, continuousBuffer);
	}

private:
	static std::pair<BenchmarkBaseEvent*, ContinuousBuffer> readEvent(std::string const & line, size_t lineNumber, std::istringstream & remainingLine, BenchmarkEventTypeId benchmarkEventTypeId, ContinuousBuffer const & buffer) {
		switch(benchmarkEventTypeId) {
			case InsertEventTypeId:
				return createEvent<InsertEventTypeId>(line, lineNumber, remainingLine, buffer);
			case LookupEventTypeId:
				return createEvent<LookupEventTypeId>(line, lineNumber, remainingLine, buffer);
			case ScanEventTypeId:
				return createEvent<ScanEventTypeId>(line, lineNumber, remainingLine, buffer);
			default:
				return createEvent<UpdateEventTypeId>(line, lineNumber, remainingLine, buffer);
		}
	}

	template<BenchmarkEventTypeId eventTypeId> static std::pair<BenchmarkBaseEvent *, ContinuousBuffer> createEvent(std::string const & line, size_t lineNumber, std::istringstream &remainingLine, ContinuousBuffer const & buffer) {
		using EventContainerType = BenchmarkEvent<eventTypeId, ValueType, KeyExtractor>;
		using SpecificEventType = typename EventContainerType::EventType ;
		std::pair<EventContainerType*, ContinuousBuffer> event = buffer.template allocate<EventContainerType>();
		SpecificEventType & specificEvent = event.first->getData();
		std::pair<BenchmarkBaseEvent *, ContinuousBuffer> result { event.first, readEventParameters(line, lineNumber, remainingLine, specificEvent, event.second) };
		if(!remainingLine.eof()) {
			std::string remainingStreamContent;
			std::getline(remainingLine, remainingStreamContent);
			if(remainingStreamContent.length() > 0) {
				std::stringstream errorMessage;
				errorMessage << "Unconsumed Content \"" << remainingStreamContent << "\"";
				throw idx::benchmark::BenchmarkEventReaderException(errorMessage.str(), line, lineNumber, __FILE__, __LINE__);
			}
		}
		return result;
	};



	static ContinuousBuffer readEventParameters(std::string const & line, size_t lineNumber, std::istringstream &remainingLine, InsertEvent<ValueType, KeyExtractor> & insertEvent, ContinuousBuffer const &buffer) {
		return readContentChecked(line, lineNumber, remainingLine, std::make_pair(&insertEvent.mValueToInsert, buffer));
	}

	static ContinuousBuffer readEventParameters(std::string const & line, size_t lineNumber, std::istringstream &remainingLine, LookupEvent<ValueType, KeyExtractor> & lookupEvent, ContinuousBuffer const &buffer) {
		return readContentChecked(line, lineNumber, remainingLine, std::make_pair(&lookupEvent.mKey, buffer));
	}

	static ContinuousBuffer readEventParameters(std::string const & line, size_t lineNumber, std::istringstream &remainingLine, ScanEvent<ValueType, KeyExtractor> & scanEvent, ContinuousBuffer const &buffer) {
		return readContentChecked(line, lineNumber, remainingLine, std::make_pair(&scanEvent.mNumberValuesToScan, readContent(remainingLine, std::make_pair(&scanEvent.mLookupKey, buffer))));
	}

	static ContinuousBuffer readEventParameters(std::string const & line, size_t lineNumber, std::istringstream &remainingLine, UpdateEvent<ValueType, KeyExtractor> & updateEvent, ContinuousBuffer const &buffer) {
		return readContentChecked(line, lineNumber, remainingLine, std::make_pair(&updateEvent.mNewValue, buffer));
	}

	template<typename ContentType> static ContinuousBuffer readContentChecked(std::string const & line, size_t lineNumber, std::istream & remainingLine, std::pair<ContentType*, ContinuousBuffer> const & contentLocation) {
		ContinuousBuffer buffer = readContent(remainingLine, contentLocation);
		if(remainingLine.fail() || remainingLine.bad()) {
			throw idx::benchmark::BenchmarkEventReaderException("Unsuported content", line, lineNumber, __FILE__, __LINE__);
		}
		return buffer;
	}
};

}}

#endif
#ifndef __IDX__BENCHMARK__CONTENT_READER__HPP__
#define __IDX__BENCHMARK__CONTENT_READER__HPP__

#include <iomanip>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>

#include "idx/benchmark/ContinuosBuffer.hpp"

namespace idx { namespace benchmark {

inline void* advanceTargetMemoryPointerByBytes(void* memoryPointer, size_t bytesToAdvance) {
	return reinterpret_cast<char*>(memoryPointer) + bytesToAdvance;
}

template<typename ContentType> struct ContentReader {
	static ContinuousBuffer read(std::istream & input, std::pair<ContentType*, ContinuousBuffer> const & contentLocation) {
		input >> (*contentLocation.first);
		return contentLocation.second;
	}
};

template<typename ContentType> struct ContentReader<ContentType*> {
	static ContinuousBuffer read(std::istream & input, std::pair<ContentType**, ContinuousBuffer> const & contentLocation) {
		const std::pair<ContentType*, ContinuousBuffer> & contentBuffer = contentLocation.second.template allocate<ContentType>();
		*contentLocation.first = contentBuffer.first;
		return ContentReader<ContentType>::read(input, contentBuffer);
	};
};

template<> struct ContentReader<char*> {
	static ContinuousBuffer read(std::istream & input, std::pair<char **, ContinuousBuffer> const & contentLocation) {
		std::string stringContent;
		input >> std::quoted(stringContent);
		ContinuousBuffer const & nextBuffer = contentLocation.second.advance(stringContent.size() + 1);
		char* bufferContentLocation = reinterpret_cast<char*>(contentLocation.second.mRemainingBuffer);
		*contentLocation.first = bufferContentLocation;
		strcpy(bufferContentLocation, stringContent.c_str());
		return nextBuffer;
	}
};

template<> struct ContentReader<char const*> {
	static ContinuousBuffer read(std::istream & input, std::pair<char const **, ContinuousBuffer> const & contentLocation) {
		return ContentReader<char*>::read(input, std::make_pair(const_cast<char**>(contentLocation.first), contentLocation.second));
	}
};

template<typename PairKeyType, typename PairValueType> struct ContentReader<std::pair<PairKeyType, PairValueType>> {
	static ContinuousBuffer read(std::istream & input, std::pair<std::pair<PairKeyType, PairValueType>*, ContinuousBuffer> const & contentLocation) {
		return ContentReader<PairValueType>::read(input, {
			&(contentLocation.first->second),
			ContentReader<PairKeyType>::read(input, { &(contentLocation.first->first), contentLocation.second })
		});
	}
};

template<typename ContentType> inline ContinuousBuffer readContent(std::istream & input, std::pair<ContentType*, ContinuousBuffer> const & contentLocation) {
	return ContentReader<typename std::remove_pointer<decltype(contentLocation.first)>::type>::read(input, contentLocation);
}

/*template<typename KeyType, typename ValueType> struct ContentReader<std::pair<KeyType, ValueType>> {
	const std::pair<KeyType, ValueType> read(std::istream & input, const std::ofstream const & output) {
		std::string content;
		input >> content;
		return strdup(content.c_str());
	}
};*/



}}

#endif
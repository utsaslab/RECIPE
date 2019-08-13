#ifndef __IDX__BENCHMARK__BENCHMARK_EVENT_READER_EXCEPTION__HPP__
#define __IDX__BENCHMARK__BENCHMARK_EVENT_READER_EXCEPTION__HPP__

#include <exception>
#include <stdexcept>
#include <sstream>

namespace idx { namespace benchmark {

class BenchmarkEventReaderException : public std::runtime_error {
public:
	BenchmarkEventReaderException(std::string const & message, std::string const & lineContent, size_t lineNumber, const char * sourceFile, int sourceFileLine)
		: runtime_error(generateMessage( message, lineContent, lineNumber, sourceFile, sourceFileLine ))
	{}

private:
	static std::string generateMessage(std::string const & message, std::string const & lineContent, size_t lineNumber, const char *sourceFile, int sourceFileLine)
	{
		std::ostringstream errorMessageStream;
		errorMessageStream  << message << " on line " << lineNumber << " \"" << lineContent << "\" at " <<  sourceFile << ":" << sourceFileLine;
		return errorMessageStream.str();
	}
};

}}

#endif
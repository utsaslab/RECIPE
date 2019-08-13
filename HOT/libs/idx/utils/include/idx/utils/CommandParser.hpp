//
// Created by Robert Binna on 09.04.15.
//
#ifndef __IDX__UTILS__COMMAND_PARSER__
#define __IDX__UTILS__COMMAND_PARSER__

#include <algorithm>
#include <cstdint>
#include <functional>
#include <fstream>
#include <iostream>
#include <set>
#include <map>
#include <random>
#include <string>
#include <sstream>

namespace idx { namespace utils {

template <typename T>
T lexical_cast(std::string argumentToCast)
{
	T var;
	std::istringstream iss;
	iss.str(argumentToCast);
	iss >> var;
	// deal with any error bits that may have been set on the stream
	return var;
}

template<> std::string lexical_cast<std::string>(std::string argumentToCast)
{
	return argumentToCast;
}

template<> bool lexical_cast<bool>(std::string str)
{
	bool var;
	std::istringstream iss(str);
	if(str == "true") {
		var = (bool) true;
	} else if(str == "false") {
			var = (bool) false;
	} else {
			iss >> var;
	}

	return var;
}

std::vector<std::string> split(std::string const & str, char const delimiter){
	std::vector<std::string> result;
	auto checkDelimiter = [&delimiter](char character) {
		return character == delimiter;
	};

	auto e=str.end();
	auto i=str.begin();
	while(i!=e){
		i=std::find_if_not(i,e, checkDelimiter);
		if(i==e) break;
		auto j=find_if(i,e, checkDelimiter);
		result.push_back(std::string(i,j));
		i=j;
	}
	return result;
}

bool fileExists(std::string const & filename) {
	std::ifstream ifile(filename.c_str());
	return ifile.is_open();
}

class CommandParser {

private:
	int mArgc;
	char** mArgv;
	std::map<std::string, std::string> mArguments;
	std::function<void()> mUsageMessageGenerator;

public:
	CommandParser(int argc, char** argv, std::function<void()> usageMessageGenerator)
		: mArgc(argc), mArgv(argv), mArguments(), mUsageMessageGenerator(usageMessageGenerator)
	{
		parse();
	}

	void printUsageMessage() const {
		mUsageMessageGenerator();
		exit(1);
	}

	void checkAllowedParams(std::set<std::string> const & allowedParams) const {
		for(std::pair<std::string, std::string> const & argument : mArguments) {
			if(allowedParams.find(argument.first) == allowedParams.end()) {
				if(isVerbose()) {
					std::cerr << "Parameter " << argument.first << "is not supported" << std::endl;
				}
				printUsageMessage();
			}
		}
	}

	std::map<std::string, std::string> getRawArguments() const {
		return mArguments;
	}

	bool has(std::string const & parameterName) const {
		return mArguments.find(parameterName) != mArguments.end();
	}

	template<class ParamType> ParamType get(std::string const & paramName, ParamType const & defaultValue) const {
		bool hasParam = has(paramName);
		return hasParam ? lexical_cast<ParamType>(mArguments.at(paramName)) : defaultValue;
	}

	template<class ParamType> ParamType expect(std::string const & paramName) const {
		if(!has(paramName)) {
			if(isVerbose()) {
				std::cerr << "Expected Parameter: " << paramName << std::endl;
			}
			printUsageMessage();
		}
		std::string argument = mArguments.at(paramName);
		return lexical_cast<ParamType>(argument);
	};

	template<class ParamType> std::vector<ParamType> getListOf(std::string const & paramName, std::vector<ParamType> allowedValues) const {
		std::string rawParamValue = expect<std::string>(paramName);
		std::vector<std::string> rawValues = split(rawParamValue, ',');
		std::set<std::string> uniqueRawValues(rawValues.begin(), rawValues.end());
		std::vector<ParamType> values;
		if(rawValues.size() != uniqueRawValues.size()) {
			printUsageMessage();
		}
		for(std::string rawValue : rawValues) {
			ParamType value = lexical_cast<ParamType>(rawValue);
			if(std::find(allowedValues.begin(), allowedValues.end(), value) == allowedValues.end()) {
				if(isVerbose()) {
					std::cerr << "Invalid List Value for: " << paramName << std::endl;
				}
				printUsageMessage();
			}
			values.push_back(lexical_cast<ParamType>(value));
		}

		return values;
	}

	template<class ParamType> ParamType expectOneOf(std::string const & paramName, std::vector<ParamType> allowedValues) const {
		ParamType paramValue = expect<ParamType>(paramName);
		if(std::find(allowedValues.begin(), allowedValues.end(), paramValue) == allowedValues.end()) {
			if(isVerbose()) {
				std::cerr << "Invalid Value for: " << paramName << std::endl;
			}
			printUsageMessage();
		}
		return paramValue;
	}

	std::string expectExistingFile(std::string const & paramName) const {
		std::string fileName = expect<std::string>(paramName);
		if(!fileExists(fileName)) {
			if(isVerbose()) {
				std::cerr << "File: " << fileName << " for param " << paramName << "does not exist" << std::endl;
			}
			printUsageMessage();
		}
		return fileName;
	}

	bool isVerbose() const {
		return get<bool>("verbose", false);
	}

private:

	void parse() {
		for(int i=1; i < mArgc; ++i) {
			parseSingleArgument(mArgv[i]);
		}
		if(isVerbose()) {
			std::cout << "commandline arguments:" << std::endl;
			for(auto param : mArguments) {
				std::cout << "\t" << param.first << "=" << param.second << std::endl;
			}
			std::cout << std::endl;
		}
	}

	void parseSingleArgument(std::string argument) {
		std::string paramName { "" };
		std::string paramValue { "" };
		size_t delimiterPosition = argument.find("=");

		if(delimiterPosition > 0) {
			paramName = argument.substr(0, delimiterPosition);
			if(delimiterPosition < argument.length()) {
				paramValue = argument.substr(delimiterPosition + 1);
			}
			if(paramName.size() > 2 && paramName[0] == '-') {
				paramName = paramName.substr(1);
			} else {
				printUsageMessage();
			}
		}
		
		mArguments[paramName] = paramValue;
	};


};

} }

#endif

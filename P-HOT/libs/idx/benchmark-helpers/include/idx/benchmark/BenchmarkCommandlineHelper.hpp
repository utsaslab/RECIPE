#ifndef __IDX__BENCHMARK__BENCHMARK_HELPER__
#define __IDX__BENCHMARK__BENCHMARK_HELPER__


#include <cstdint>
#include <string>
#include <iostream>
#include <fstream>

#include <idx/utils/CommandParser.hpp>
#include <idx/utils/DataSetGenerators.hpp>

#include "idx/benchmark/BenchmarkConfiguration.hpp"

namespace idx { namespace benchmark {



class BenchmarkCommandlineHelper {

private:
	int mArgc;
	char **mArgv;
	std::map<std::string, std::string> mAdditionalConfigOptions;
	idx::utils::CommandParser mParser;

public:
	BenchmarkCommandlineHelper(int argc, char **argv, std::string /* dataStructureName */, std::map<std::string, std::string> const & additionalConfigOptions) : mArgc(argc), mArgv(argv), mAdditionalConfigOptions(additionalConfigOptions), mParser(argc, argv, [=]() {
		std::cout << std::endl;
		std::cout << "Usage: " << argv[0] << " -insert=<insertType> [-insertModifier=<modifierType>] [-input=<insertFileName>] -size=<size> [-lookup=<lookupType>] [-lookupFile=<lookupFileName>] [-help] [-verbose=<true/false>] [-insertModifier=<true/false>]";
		for(auto const & entry : additionalConfigOptions) {
			std::cout << " [-" << entry.first << "=<" << entry.first << ">]";
		}
		std::cout << std::endl;
		std::cout << "\tdescription: " << argv[0] << " " << std::endl;
		std::cout << "\t\tinserts <size> values of a given generator type (<insertType>) into the index structure" << std::endl;
		std::cout << "\t\tAfter that lookup is executed with provided data. Either a modification of the input type or a separate data file" << std::endl;
		std::cout << "\t\tThe lookup is executed n times the size of the lookup data set, where n is the smallest natural number which results in at least 100 million lookup operations" << std::endl << std::endl;
		std::cout << "\tswitches:" << std::endl;
		std::cout << "\t\t-insert: specifies the distribution which will be used to insert data" << std::endl;
		std::cout << "\t\t-size: specifies the number of values to insert" << std::endl;
		std::cout << "\t\t-insertModifier: specifies the order to insert the values" << std::endl;
		std::cout << "\t\t-lookupModifier: specifies the order to lookup the values." << std::endl;
		std::cout << "\t\t-input: specifies the file to read data from in case the insertType is file. " << std::endl;
		std::cout << "\t\t\tEach line of the file contains a single value. " << std::endl;
		std::cout << "\t\t\tThe first line contains the total number of values in the file." << std::endl;
		std::cout << "\t\t-help: show the usage dialog. " << std::endl;
		std::cout << "\t\t-insertOnly: specifies whether only the insert operation should be executed." << std::endl;
		std::cout << "\t\t-threads: specifies the number of threads used for inserts as well as lookups." << std::endl;
		std::cout << "\t\t-verbose: specifies to show debug messages. " << std::endl;


		std::cout << std::endl;
		std::cout << "\tpotential parameter values:" << std::endl;
		std::cout << "\t\t<insertType>: is either dense/pseudorandom/random or file. In case of file the -insertFile parameter must be provided." << std::endl;
		std::cout << "\t\t<modifierType>: is on of sequential/random/reverse and modifies the input data before it is inserted." << std::endl;
		std::cout << "\t\t<lookupType>: is either a modifier (\"sequential\"/\"random\" or \"reverse\") on the input data which will be used to modify the input data before executing the lookup" << std::endl;
		std::cout << "\t\t\tor \"file\" which requires the additional parameter lookupFile specifying a file containing  the lookup data." << std::endl;
		std::cout << "\t\t\tIn case no lookup type is specified the same data which was used for insert is used for the lookup." << std::endl;
		std::cout << "\t\t<insertFileName>: Only used in case the <modifierType> \"file\" is specified. It specifies the name of a dat file containing the input data." << std::endl;
		std::cout << "\t\t<insertFileName>: Only used in case the <lookupType> \"file\" is specified. It specifies the name of a dat file containing the lookup data." << std::endl;
		std::cout << std::endl << std::endl;
		for(auto const & entry : additionalConfigOptions) {
			std::cout << "\t\t<" << entry.first << ">: " << entry.second << std::endl;
		}
		std::cout << std::endl;
	} ) {
	}

	BenchmarkConfiguration parseArguments() {
		std::set<std::string> allowedConfigOptions { "insert", "insertOnly", "insertModifier", "input", "size", "lookup", "lookupFile", "verbose", "threads" };
		for(auto const & entry : mAdditionalConfigOptions) {
			allowedConfigOptions.insert(entry.first);
		}
		mParser.checkAllowedParams(allowedConfigOptions);
		return { mParser };
	}
};

} }

#endif //__IDX__BENCHMARK__BENCHMARK_HELPER__
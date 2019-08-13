#ifndef __IDX__BENCHMARK__STRING_BENCHMARK_HELPER__
#define __IDX__BENCHMARK__STRING_BENCHMARK_HELPER__


#include <cstdint>
#include <string>
#include <iostream>
#include <fstream>

#include <idx/utils/CommandParser.hpp>
#include <idx/utils/DataSetGenerators.hpp>

#include "idx/benchmark/StringBenchmarkConfiguration.hpp"

namespace idx { namespace benchmark {



class StringBenchmarkCommandlineHelper {

private:
	int mArgc;
	char **mArgv;
	std::map<std::string, std::string> const & mAdditionalConfigOptions;
	idx::utils::CommandParser mParser;

public:
	StringBenchmarkCommandlineHelper(int argc, char **argv, std::string /* dataStructureName */, std::map<std::string, std::string> const & additionalConfigOptions) : mArgc(argc), mArgv(argv), mAdditionalConfigOptions(additionalConfigOptions), mParser(argc, argv, [&]() {
		std::cout << std::endl;
		std::cout << "Usage: " << argv[0] << " -inputFile=<insertFile> [-insertModifier=<modifierType>] -size=<size> [-lookupModifier=<lookupType>] [-lookupFile=<lookupFileName>] [-help] [-verbose=<true/false>]";// [-writeDotRepresentation=<dotFileName>]";
		for(auto const & entry : additionalConfigOptions) {
			std::cout << " [-" << entry.first << "=<" << entry.first << ">]";
		}
		std::cout << std::endl;
		std::cout << "\tdescription: " << argv[0] << "Inserts <size> strings into the benchmarked index structure " << std::endl;
		std::cout << "\t\tAfter that lookup is executed with either the inserted strings or a new set of string. Both the insertion as well as the lookup order can be modified." << std::endl;
		std::cout << "\t\tThe lookup is executed n times the size of the lookup data set, where n is the smallest natural number which results in at least 100 million lookup operations" << std::endl << std::endl;
		std::cout << "\tparameters:" << std::endl;
		std::cout << "\t\t<insertFile>: The absolute filename of the strings to lookup. Each line of the file contains a single key. The first line contains the total number of keys contained in the file." << std::endl;
		std::cout << "\t\t<modifierType>: is one of sequential/random/reverse and modifies the input data before it is inserted." << std::endl;
		std::cout << "\t\t<lookupModifier>: is either a modifier (\"sequential\"/\"random\" or \"reverse\") on the lookupFile or if not provided a modifier on the input" << std::endl;
		std::cout << "\t\t\tIn case no lookup modifer is specified the same data which was used for insert is used for the lookup." << std::endl;
		std::cout << "\t\t<insertFileName>: It specifies the name of the file containing the input data." << std::endl;
		std::cout << "\t\t<lookupFileName>: It specifies the name of the file containing the lookup data." << std::endl;
		std::cout << std::endl << std::endl;
		for(auto const & entry : additionalConfigOptions) {
			std::cout << "\t\t<" << entry.first << ">: " << entry.second << std::endl;
		}
		std::cout << std::endl;
		std::cout << "\t" << "-verbose: specifies whether verbose debug output should be printed or not." << std::endl;
		std::cout << "\t" << "-insertOnly: specifies whether only the insert operation should be executed." << std::endl;
		std::cout << "\t" << "-threads: specifies the number of threads used for insertion as well as lookup operations." << std::endl;
		//std::cout << "\t" << "-writeDotRepresentation: specifies a filename where a dot representation of the graph should be writte to, if no filename is specified no dot representation will be generated." << std::endl;
		std::cout << "\t" << "-help: prints this usage message and terminates the application." << std::endl;
		std::cout << std::endl;
	} ) {
	}

	StringBenchmarkConfiguration parseArguments() {
		std::set<std::string> allowedConfigOptions { "inputFile", "insertOnly", "insertModifier", "size", "lookupModifier", "lookupFile", "verbose", "writeDotRepresentation", "threads" };
		for(auto const & entry : mAdditionalConfigOptions) {
			allowedConfigOptions.insert(entry.first);
		}
		return { mParser };
	}
};

} }

#endif //__IDX__BENCHMARK__BENCHMARK_HELPER__
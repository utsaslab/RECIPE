#ifndef __IDX__BENCHMARK__DAT_FILE_BENCHMARK_COMMANDLINE_HELPER__
#define __IDX__BENCHMARK__DAT_FILE_BENCHMARK_COMMANDLINE_HELPER__


#include <cstdint>
#include <string>
#include <iostream>
#include <fstream>
#include <string>

#include <idx/utils/CommandParser.hpp>
#include <idx/utils/DataSetGenerators.hpp>

#include "idx/benchmark/DatFileBenchmarkConfiguration.hpp"

namespace idx { namespace benchmark {


class DatFileBenchmarkCommandlineHelper {

private:
	int mArgc;
	char **mArgv;
	idx::utils::CommandParser mParser;

public:
	DatFileBenchmarkCommandlineHelper(int argc, char **argv, std::string const & dataStructureName) : mArgc(argc), mArgv(argv), mParser(argc, argv, [=]() {
		std::cout << std::endl;
		std::cout << "Usage: " << argv[0] << " -inputFile=<insertFile> [-insertModifier=<modifierType>] -size=<size> [-lookupModifier=<lookupType>] [-lookupFile=<lookupFileName>] [-help] [-verbose=<true/false>] [-writeDotRepresentation=<dotFileName>]" << std::endl;
		std::cout << "\tdescription: " << argv[0] << "Inserts <size> values of a given generator type (<insertType>) into the " << dataStructureName << std::endl;
		std::cout << "\t\tAfter that lookup is executed with provided data. Either a modification of the input type or a separate data file" << std::endl;
		std::cout << "\t\tThe lookup is executed n times the size of the lookup data set, where n is the smallest natural number which results in at least 100 million lookup operations" << std::endl << std::endl;
		std::cout << "\tparameters:" << std::endl;
		std::cout << "\t\t<insertFile>: The filename of the strings to lookup." << std::endl;
		std::cout << "\t\t<modifierType>: is one of sequential/random/reverse and modifies the input data before it is inserted." << std::endl;
		std::cout << "\t\t<lookupModifier>: is either a modifier (\"sequential\"/\"random\" or \"reverse\") on the lookupFile or if not provided a modifier on the input" << std::endl;
		std::cout << "\t\t\tIn case no lookup modifer is specified the same data which was used for insert is used for the lookup." << std::endl;
		std::cout << "\t\t<insertFileName>: It specifies the name of the file containing the input data." << std::endl;
		std::cout << "\t\t<lookupFileName>: It specifies the name of the file containing the lookup data." << std::endl;
		std::cout << std::endl << std::endl;
		std::cout << "\t" << "-verbose: specifies whether verbose debug output should be printed or not." << std::endl;
		std::cout << "\t" << "-insertOnly: specifies whether only the insert operation should be executed." << std::endl;
		std::cout << "\t" << "-writeDotRepresentation: specifies a filename where a dot representation of the graph should be writte to, if no filename is specified no dot representation will be generated." << std::endl;
		std::cout << "\t" << "-help: prints this usage message and terminates the application." << std::endl;
		std::cout << std::endl;
	} ) {
	}

	DatFileBenchmarkConfiguration parseArguments() {
		mParser.checkAllowedParams({ "inputFile", "insertOnly", "insertModifier", "size", "lookupModifier", "lookupFile", "verbose", "writeDotRepresentation" });
		return { mParser };
	}
};

} }

#endif //__IDX__BENCHMARK__DAT_FILE_BENCHMARK_COMMANDLINE_HELPER__
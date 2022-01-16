#ifndef __IDX__BENCHMARK__DAT_FILE__
#define __IDX__BENCHMARK__DAT_FILE__

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <iostream>
#include <memory>
#include <cstring>
#include <sstream>
#include <vector>

namespace idx { namespace benchmark {

struct TupleComparator
{
	size_t mNumberBytes;

	TupleComparator(size_t numberBytes) : mNumberBytes(numberBytes) {
	}

	bool operator()(uint8_t * s1, uint8_t * s2) const
	{
		return memcmp(s1, s2, mNumberBytes) < 0;
	}
};

struct TupleReverseComparator
{
	size_t mNumberBytes;

	TupleReverseComparator(size_t numberBytes) : mNumberBytes(numberBytes) {
	}

	bool operator()(uint8_t * s1, uint8_t * s2) const
	{
		return memcmp(s1, s2, mNumberBytes) > 0;
	}
};

std::vector <std::string> readSpaceSeparatedTuple(std::string line) {
	std::istringstream lineInput(line);
	return {std::istream_iterator < std::string > {lineInput}, std::istream_iterator < std::string > {}};
}

class DatFile;

class DatFile {
private:
	std::vector <uint> mColumnSizes;
	std::vector <uint> mColumnOffsets;
	uint mTupleSizeInBytes;

	std::vector<uint8_t *> mRawTuples;

	DatFile(std::vector <uint> columnSizes, std::vector <uint> columnOffsets, uint tupleSizeInBytes, std::vector<uint8_t *> && rawTuples)
		: mColumnSizes(columnSizes), mColumnOffsets(columnOffsets), mTupleSizeInBytes(tupleSizeInBytes), mRawTuples(rawTuples) {
	}

public:
	//RValue Copy Constructor needed...
	DatFile(DatFile &&rValue) : mColumnSizes(std::move(rValue.mColumnSizes)),
								mColumnOffsets(std::move(rValue.mColumnOffsets)),
								mTupleSizeInBytes(rValue.mTupleSizeInBytes),
								mRawTuples(std::move(rValue.mRawTuples)) {
	}

	DatFile(DatFile const & other) :
								mColumnSizes(other.mColumnSizes),
								mColumnOffsets(other.mColumnOffsets),
								mTupleSizeInBytes(other.mTupleSizeInBytes),
								mRawTuples(other.mRawTuples) {
	}

	DatFile & operator=(DatFile &&rValue) {
		mColumnSizes = std::move(rValue.mColumnSizes);
		mColumnOffsets = std::move(rValue.mColumnOffsets);
		mRawTuples = std::move(rValue.mRawTuples);
		mTupleSizeInBytes = rValue.mTupleSizeInBytes;
		return *this;
	}

	uint getTupleSizeInBytes() const {
		return mTupleSizeInBytes;
	}

	uint64_t getNumberTuples() const {
		return mRawTuples.size();
	}

	uint64_t getCell(size_t tupleId, size_t columnId) const {
		uint8_t *rawTuple = mRawTuples[tupleId];
		switch (mColumnSizes[columnId]) {
			case 1:
				return rawTuple[mColumnOffsets[columnId]];
				break;
			case 2:
				return __builtin_bswap16(*reinterpret_cast<uint16_t *>(rawTuple + mColumnOffsets[columnId]));
				break;
			case 4:
				return __builtin_bswap32(*reinterpret_cast<uint32_t *>(rawTuple + mColumnOffsets[columnId]));
				break;
			case 8:
				return __builtin_bswap64(*reinterpret_cast<uint64_t *>(rawTuple + mColumnOffsets[columnId]));
				break;
			default:
				std::cout << "Unsupported column size: " << mColumnSizes[columnId] << std::endl;
				exit(1);
				break;
		}
	}

	uint8_t *getRawTuple(size_t tupleId) const {
		return mRawTuples[tupleId];
	}

	void sort() {
		std::sort(mRawTuples.begin(), mRawTuples.end(), TupleComparator(mTupleSizeInBytes));
	}

	void reverseSort() {
		std::sort(mRawTuples.begin(), mRawTuples.end(), TupleReverseComparator(mTupleSizeInBytes));
	}

	void randomize() {
		std::srand(1);
		std::srand(0);
		std::random_shuffle(mRawTuples.begin(), mRawTuples.end());
	}

	void deduplicateSorted() {
		size_t totalNumberTuples = mRawTuples.size();
		size_t numberUnqiueTuples = 1;
		for(size_t currentTuple = 1ul; currentTuple < totalNumberTuples; ++currentTuple) {
			if(std::memcmp(getRawTuple(currentTuple), getRawTuple(numberUnqiueTuples - 1), mTupleSizeInBytes) != 0) {
				if(numberUnqiueTuples != currentTuple) {
					std::memcpy(getRawTuple(numberUnqiueTuples), getRawTuple(currentTuple), mTupleSizeInBytes);
					++numberUnqiueTuples;
				}
			}
		}
		mRawTuples.erase(mRawTuples.begin() + (numberUnqiueTuples - 1), mRawTuples.end());
	}

	void writeTo(std::string outputFileName) {
		std::ofstream out { outputFileName };
		writeHeader(out);
		writeTuples(out);
		out.close();
	}

private:
	void writeHeader(std::ostream & out) {
		out << mRawTuples.size();
		for(uint columnSize : mColumnSizes) {
			out << " " << columnSize;
		}
		out << std::endl;
	}

	void writeTuples(std::ostream & out) {
		size_t numberTuples = mRawTuples.size();
		for(size_t i=0; i < numberTuples; ++i) {
			writeTuple(out, i);
		}
	}

	void writeTuple(std::ostream & out, size_t tupleIndex) {
		size_t numberColumns = mColumnSizes.size();
		for(size_t j=0; j < numberColumns; ++j) {
			if(j > 0) {
				out << " ";
			}
			out << getCell(tupleIndex, j);
		}
		out << std::endl;
	}

	static uint8_t *readRawTuple(std::istream &input, std::vector <uint> columnSizes, std::vector <uint> columnOffsets,
								 int bytesPerTuple) {
		uint8_t *rawTuple = new uint8_t[bytesPerTuple + 1]; //alignment for art tree maybe remove
		if((((uintptr_t) rawTuple)% 2) == 1) {
			rawTuple = rawTuple + 1;
		}

		for (int columnIndex = 0; columnIndex < columnSizes.size(); ++columnIndex) {
			int columnSize = columnSizes[columnIndex];
			int columnOffset = columnOffsets[columnIndex];

			std::string column;
			input >> column;
			uint64_t columnValue = std::stoll(column);

			switch (columnSize) {
				case 1:
					rawTuple[columnOffset];
					break;
				case 2:
					*reinterpret_cast<uint16_t *>(rawTuple + columnOffset) = __builtin_bswap16(columnValue);
					break;
				case 4:
					*reinterpret_cast<uint32_t *>(rawTuple + columnOffset) = __builtin_bswap32(columnValue);
					break;
				case 8:
					*reinterpret_cast<uint64_t *>(rawTuple + columnOffset) = __builtin_bswap64(columnValue);
					break;
				default:
					std::cout << "Unsupported column size: " << columnSize << std::endl;
					exit(1);
					break;
			}
		}

		return rawTuple;
	}

public:
	static std::shared_ptr<DatFile> read(std::string const &datFileName, int64_t maxNumberEntries = -1) {
		std::ifstream input(datFileName);

		std::string headers;
		std::getline(input, headers);
		std::vector <std::string> allHeaderFields = readSpaceSeparatedTuple(headers);

		std::vector <uint> columnSizes;
		std::vector <uint> columnOffsets;

		uint offset = 0;
		for (int i = 1; i < allHeaderFields.size(); ++i) {
			uint columnSize = std::stol(allHeaderFields[i]);
			columnSizes.push_back(columnSize);
			columnOffsets.push_back(offset);
			offset += columnSize;
		}

		int tupleSizeInBytes = offset;
		size_t numberEntries = std::stoll(allHeaderFields[0]);
		std::vector<uint8_t*> rawTuples;
		size_t entriesToRead =
			maxNumberEntries < 0 ? numberEntries : std::min((size_t) maxNumberEntries, numberEntries);
		rawTuples.reserve(entriesToRead);
		for (size_t i = 0; i < entriesToRead; ++i) {
			if((i % 1000000) == 0) {
				std::cout << "Read " << i <<  " tuples of " << entriesToRead << " tuples in total" << std::endl;
			}
			rawTuples.push_back(readRawTuple(input, columnSizes, columnOffsets, tupleSizeInBytes));
		}
		return std::shared_ptr<DatFile>(new DatFile(columnSizes, columnOffsets, tupleSizeInBytes, std::move(rawTuples)));
	}
};

} }

#endif /// __IDX__BENCHMARK__DAT_FILE__
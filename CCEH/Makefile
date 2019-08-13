CXX := g++
CFLAGS := -std=c++17 -I./ -lrt -lpthread -O3

all: ALL_CCEH Cuckoo LinearProbing Extendible Level Path

ALL_CCEH: CCEH_MSB CCEH_LSB

CCEH_MSB: src/CCEH.h src/CCEH_MSB.cpp
	$(CXX) $(CFLAGS) -c -o src/CCEH_MSB.o src/CCEH_MSB.cpp -DINPLACE
	$(CXX) $(CFLAGS) -c -o src/CCEH_MSB_CoW.o src/CCEH_MSB.cpp
	
CCEH_LSB: src/CCEH.h src/CCEH_LSB.cpp
	$(CXX) $(CFLAGS) -c -o src/CCEH_LSB.o src/CCEH_LSB.cpp -DINPLACE
	$(CXX) $(CFLAGS) -c -o src/CCEH_LSB_CoW.o src/CCEH_LSB.cpp

Cuckoo: src/cuckoo_hash.cpp src/cuckoo_hash.h
	$(CXX) $(CFLAGS) -c src/cuckoo_hash.cpp -o src/cuckoo_hash.o

LinearProbing: src/linear_probing.cpp src/linear_probing.h
	$(CXX) $(CFLAGS) -c src/linear_probing.cpp -o src/linear_probing.o

Extendible: src/extendible_hash.cpp src/extendible_hash.h
	$(CXX) $(CFLAGS) -c src/extendible_hash.cpp -o src/extendible_hash.o

Level: src/Level_hashing.cpp src/Level_hashing.h
	$(CXX) $(CFLAGS) -c src/Level_hashing.cpp -o src/Level_hashing.o

Path: src/path_hashing.cpp src/path_hashing.hpp
	$(CXX) $(CFLAGS) -c src/path_hashing.cpp -o src/path_hashing.o

clean:
	rm -rf src/*.o

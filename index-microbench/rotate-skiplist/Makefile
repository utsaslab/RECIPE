#-include Makefile.common

CXX=g++-5
CXX_FLAGS=-std=c++11 -pthread -O3 -g

.PHONY:	all clean

all:	main

ptst.o: ptst.h common.h 
	$(CXX) $(CFLAGS) $(CXX_FLAGS) -std=c++11 -c -o ./ptst.o ptst.cpp 

garbagecoll.o: garbagecoll.h ptst.h common.h
	$(CXX) $(CFLAGS) $(CXX_FLAGS) -std=c++11 -c -o ./garbagecoll.o garbagecoll.cpp

nohotspot_ops.o: skiplist.h background.h nohotspot_ops.h common.h 
	$(CXX) $(CFLAGS) $(CXX_FLAGS) -std=c++11 -c -o ./nohotspot_ops.o nohotspot_ops.cpp 

skiplist.o: skiplist.h background.h common.h
	$(CXX) $(CFLAGS) $(CXX_FLAGS) -std=c++11 -c -o ./skiplist.o skiplist.cpp 

background.o: background.h skiplist.h common.h
	$(CXX) $(CFLAGS) $(CXX_FLAGS) -std=c++11 -c -o ./background.o background.cpp 

intset.o: intset.h nohotspot_ops.h skiplist.h
	$(CXX) $(CFLAGS) $(CXX_FLAGS) -std=c++11 -c -o ./intset.o intset.cpp 

test.o: intset.h test.c
	$(CXX) $(CFLAGS) $(CXX_FLAGS) -std=c++11 -c -o ./test.o test.c

main: intset.o background.o skiplist.o nohotspot_ops.o test.o ptst.o garbagecoll.o
	$(CXX) $(CFLAGS) $(CXX_FLAGS) garbagecoll.o ptst.o skiplist.o nohotspot_ops.o intset.o background.o test.o -o ./main $(LDFLAGS)

clean:
	-rm -rf *.o
	-rm -f ./main

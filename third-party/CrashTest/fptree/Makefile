LIBS := -lrt -ltbb -lpthread

CFLAGS := -O3 -std=c++17 -mrtm

all:
	g++ $(CFLAGS) -c FPTree_lock.cpp $(INCLUDES) 
	g++ $(CFLAGS) -c main.cpp 
	g++ $(CFLAGS) -c main.cpp -o main_mixed.o -DMIXED
	g++ $(CFLAGS) -o FPTree_lock main.o FPTree_lock.o $(LIBS)
	g++ $(CFLAGS) -o FPTree_lock_mixed main_mixed.o FPTree_lock.o $(LIBS) 

clean:
	rm -f FPTree_lock.o main.o main_mixed.o FPTree_lock FPTree_lock_mixed

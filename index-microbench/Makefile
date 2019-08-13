CC = gcc-5 
CXX = g++-5 -std=gnu++0x
DEPSDIR := masstree/.deps
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP
MEMMGR = -lpapi -ltcmalloc_minimal
CFLAGS = -g -O3 -Wno-invalid-offsetof -mcx16 -DNDEBUG -DBWTREE_NODEBUG $(DEPCFLAGS) -include masstree/config.h

# By default just use 1 thread. Override this option to allow running the
# benchmark with 20 threads. i.e. THREAD_NUM=20 make run_all_atrloc
THREAD_NUM?=1
TYPE?=bwtree

# Can also change it to rotate skiplist
SL_DIR=./nohotspot-skiplist

# skiplist source files
SL_OBJS=$(patsubst %.cpp,%.o,$(wildcard $(SL_DIR)/*.cpp))
$(info skip list object files: $(SL_OBJS))

SNAPPY = /usr/lib/libsnappy.so.1.3.0

all: workload

ifdef STRING_KEY
$(info Using string key as key type)
CFLAGS += -DUSE_GENERIC_KEY
endif

run_all: workload workload_string
	./workload a rand $(TYPE) $(THREAD_NUM) 
	./workload c rand $(TYPE) $(THREAD_NUM)
	./workload e rand $(TYPE) $(THREAD_NUM)
	./workload a mono $(TYPE) $(THREAD_NUM) 
	./workload c mono $(TYPE) $(THREAD_NUM)
	./workload e mono $(TYPE) $(THREAD_NUM)
	./workload_string a email $(TYPE) $(THREAD_NUM)
	./workload_string c email $(TYPE) $(THREAD_NUM)
	./workload_string e email $(TYPE) $(THREAD_NUM)

workload.o: workload.cpp microbench.h index.h util.h ./masstree/mtIndexAPI.hh ./BwTree/bwtree.h BTreeOLC/BTreeOLC.h BTreeOLC/BTreeOLC_child_layout.h ./pcm/pcm-memory.cpp ./pcm/pcm-numa.cpp ./papi_util.cpp
	$(CXX) $(CFLAGS) -c -o workload.o workload.cpp

workload: skiplist-clean workload.o bwtree.o artolc.o btree.o ./masstree/mtIndexAPI.a ./pcm/libPCM.a $(SL_OBJS)
	$(CXX) $(CFLAGS) -o workload workload.o bwtree.o artolc.o btree.o $(SL_OBJS) masstree/mtIndexAPI.a ./pcm/libPCM.a $(MEMMGR) -lpthread -lm -ltbb

workload_string.o: workload_string.cpp microbench.h index.h util.h ./masstree/mtIndexAPI.hh ./BwTree/bwtree.h BTreeOLC/BTreeOLC.h skiplist-clean
	$(CXX) $(CFLAGS) -c -o workload_string.o workload_string.cpp

workload_string: skiplist-clean workload_string.o bwtree.o artolc.o ./masstree/mtIndexAPI.a $(SL_OBJS)
	$(CXX) $(CFLAGS) -o workload_string workload_string.o bwtree.o artolc.o  $(SL_OBJS) masstree/mtIndexAPI.a $(MEMMGR) -lpthread -lm -ltbb

bwtree.o: ./BwTree/bwtree.h ./BwTree/bwtree.cpp
	$(CXX) $(CFLAGS) -c -o bwtree.o ./BwTree/bwtree.cpp

artolc.o: ./ARTOLC/*.cpp ./ARTOLC/*.h
	$(CXX) $(CFLAGS) ./ARTOLC/Tree.cpp -c -o artolc.o $(MEMMGR) -lpthread -lm -ltbb

btree.o: ./btree-rtm/*.c ./btree-rtm/*.h
	$(CXX) $(CFLAGS) ./btree-rtm/btree.c -c -o btree.o $(MEMMGR) -lpthread -lm

$(SL_DIR)/%.o: $(SL_DIR)/%.cpp $(SL_DIR)/%.h
	$(CXX) $(CFLAGS) -c -o $@ $< $(MEMMGR) -lpthread -lm -ltbb

generate_workload:
	./generate_all_workloads.sh

clean:
	$(RM) workload workload_string *.o *~ *.d
	$(RM) $(SL_DIR)/*.o

skiplist-clean:
	$(RM) rm $(SL_DIR)/*.o

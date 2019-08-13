
# This is the basic set of compiler flags  
# We also add more for different options 
CXX_FLAGS=-std=c++11 -pthread -Wall -Wextra

ifdef DEBUG
	CXX_FLAGS += -O1 -g 
else
	CXX_FLAGS += -O3
endif

all: rotate-skiplist.cpp rotate-skiplist.h
	g++ $(CXX_FLAGS) $(LD_FLAGS) rotate-skiplist.cpp main.cpp -o ./main
		

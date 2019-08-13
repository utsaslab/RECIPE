.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lpthread
CFLAGS=-O0 -std=c++11 -g

output = test

all: main

main: main.cpp
	g++ $(CFLAGS) -o test main.cpp $(LIBS)

clean:
	rm $(output)

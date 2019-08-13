
include Makefile.common

BINS = .
INCLUDE_DIR=./atomic_ops
BUILDIR=.

.PHONY:	all clean

all:	main

ptst.o: ptst.h 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/ptst.o ptst.cpp -I$(INCLUDE_DIR)

garbagecoll.o: garbagecoll.h ptst.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/garbagecoll.o garbagecoll.cpp -I$(INCLUDE_DIR)

nohotspot_ops.o: skiplist.h background.h nohotspot_ops.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/nohotspot_ops.o nohotspot_ops.cpp -I$(INCLUDE_DIR)

skiplist.o: skiplist.h background.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/skiplist.o skiplist.cpp -I$(INCLUDE_DIR)

background.o: background.h skiplist.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/background.o background.cpp -I$(INCLUDE_DIR)

intset.o: intset.h nohotspot_ops.h skiplist.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/intset.o intset.cpp -I$(INCLUDE_DIR)

test.o: intset.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o test.c -I$(INCLUDE_DIR)

main: intset.o background.o skiplist.o nohotspot_ops.o test.o ptst.o garbagecoll.o
	$(CC) $(CFLAGS) $(BUILDIR)/garbagecoll.o $(BUILDIR)/ptst.o $(BUILDIR)/skiplist.o $(BUILDIR)/nohotspot_ops.o $(BUILDIR)/intset.o $(BUILDIR)/background.o $(BUILDIR)/test.o -o $(BINS)/test $(LDFLAGS)

clean:
	-rm -f $(BINS)/*.o
	rm -f ./test

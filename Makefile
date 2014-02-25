PROG = cf-scheduler

OS = $(shell uname -s)

all:
	make -f Makefile.$(OS)

clean:
	rm -f $(PROG)

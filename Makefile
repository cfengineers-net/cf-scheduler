PROG = cf_scheduler

OS = $(shell uname -s)

MAKE = $(shell which gmake || which make)

all:
	$(MAKE) -f Makefile.$(OS)

clean:
	rm -f $(PROG)

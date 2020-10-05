CC ?= gcc
CXX ?= g++
CXXFLAGS ?= -O2 -g -Wall -std=gnu++17
CPPFLAGS=-ITurboPFor-Integer-Compression/
LDFLAGS ?=
INSTALL ?= install
PREFIX ?= /usr/local
URING_LIBS = $(shell pkg-config --libs liburing)

ifeq ($(URING_LIBS),)
  CPPFLAGS += -DWITHOUT_URING
endif

all: plocate plocate-build

plocate: plocate.o io_uring_engine.o TurboPFor-Integer-Compression/libic.a
	$(CXX) -o $@ $^ -lzstd $(URING_LIBS) $(LDFLAGS)

plocate-build: plocate-build.o TurboPFor-Integer-Compression/libic.a
	$(CXX) -o $@ $^ -lzstd $(LDFLAGS)

TurboPFor-Integer-Compression/libic.a:
	cd TurboPFor-Integer-Compression/ && $(MAKE)

clean:
	$(RM) plocate.o plocate-build.o io_uring_engine.o bench.o plocate plocate-build bench
	cd TurboPFor-Integer-Compression/ && $(MAKE) clean

install: all
	$(INSTALL) -m 2755 -g mlocate plocate $(PREFIX)/bin/
	$(INSTALL) -m 0755 plocate-build $(PREFIX)/sbin/
	$(INSTALL) -m 0755 update-plocate.sh /etc/cron.daily/plocate

bench.o: bench.cpp turbopfor.h

bench: bench.o io_uring_engine.o TurboPFor-Integer-Compression/libic.a
	$(CXX) -o $@ $^ $(URING_LIBS) $(LDFLAGS)

.PHONY: clean install

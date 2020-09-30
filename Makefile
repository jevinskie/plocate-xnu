CC ?= gcc
CXX ?= g++
CXXFLAGS ?= -O2 -g -Wall -std=gnu++17
CPPFLAGS=-ITurboPFor-Integer-Compression/
INSTALL ?= install
PREFIX ?= /usr/local
URING_LIBS = $(shell pkg-config --libs liburing)

ifeq ($(URING_LIBS),)
  CPPFLAGS += -DWITHOUT_URING
endif

all: plocate plocate-build

plocate: plocate.o io_uring_engine.o TurboPFor-Integer-Compression/libic.a
	$(CXX) -o $@ $^ -lzstd $(URING_LIBS)

plocate-build: plocate-build.o TurboPFor-Integer-Compression/libic.a
	$(CXX) -o $@ $^ -lzstd

TurboPFor-Integer-Compression/libic.a:
	cd TurboPFor-Integer-Compression/ && $(MAKE)

clean:
	$(RM) plocate.o plocate-build.o io_uring_engine.o plocate plocate-build
	cd TurboPFor-Integer-Compression/ && $(MAKE) clean

install: all
	$(INSTALL) -m 2755 -g mlocate plocate $(PREFIX)/bin/
	$(INSTALL) -m 0755 plocate-build $(PREFIX)/sbin/
	$(INSTALL) -m 0755 update-plocate.sh /etc/cron.daily/plocate

.PHONY: clean install

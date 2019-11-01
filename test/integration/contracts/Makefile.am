check_PROGRAMS = test_pio
TESTS = $(check_PROGRAMS)

MODULE_ROOT = ../../../source/module

TOOLS = ../../tools
VPATH = $(TOOLS)/

# XXX I would rather have this list of sources live in
# $(TOOlS)/Makefile.rules, but I haven't figured out how
# to have variables in that file included in automake targets
# in this file
#
# VPATH is not working with automake while trying to build a
# source package, it can not find these testing tool files
TESTING_SOURCES = $(TOOLS)/_display.c      \
                  $(TOOLS)/_events.c       \
                  $(TOOLS)/_expectations.c \
                  $(TOOLS)/testing.c

test_pio_SOURCES = test_pio.c $(TESTING_SOURCES)

AM_CPPFLAGS=-I$(MODULE_ROOT)    \
              $(TIRPC_CFLAGS)   \
              $(OPENSSL_CFLAGS) \
              $(GLOBUS_CFLAGS)  \
              $(HPSS_CFLAGS)    \
              -Wall -ggdb3 -I$(TOOLS)/

AM_LDFLAGS=$(TIRPC_LDFLAGS)   \
           $(OPENSSL_LDFLAGS) \
           $(GLOBUS_LDFLAGS)  \
           $(HPSS_LDFLAGS)

HPSS_LIBS=-lhpsskrb5auth -lhpssunixauth -lhpss -lhpsscs

LDADD = $(OPENSSL_LIBS) \
        $(TIRPC_LIBS)   \
        $(GLOBUS_LIBS)  \
        $(HPSS_LIBS)    \
        -lpthread

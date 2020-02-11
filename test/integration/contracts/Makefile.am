check_PROGRAMS = test_pio
TESTS = $(check_PROGRAMS)

MODULE_ROOT = ../../../source/module

FRAMEWORK = ../../framework

# XXX I would rather have this list of sources live in
# $(TOOlS)/Makefile.rules.
TESTING_SOURCES = $(FRAMEWORK)/_display.c      \
                  $(FRAMEWORK)/_events.c       \
                  $(FRAMEWORK)/_expectations.c \
                  $(FRAMEWORK)/testing.c

test_pio_SOURCES = test_pio.c $(TESTING_SOURCES)

AM_CPPFLAGS=-I$(MODULE_ROOT)    \
              $(TIRPC_CFLAGS)   \
              $(OPENSSL_CFLAGS) \
              $(GLOBUS_CFLAGS)  \
              $(HPSS_CFLAGS)    \
              -Wall -ggdb3 -I$(FRAMEWORK)/

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

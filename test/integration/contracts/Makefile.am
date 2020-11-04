check_PROGRAMS = test_pio
TESTS = $(check_PROGRAMS)

MODULE_ROOT = $(top_srcdir)/source/module
FRAMEWORK = $(top_srcdir)/test/framework

test_pio_SOURCES = test_pio.c

AM_CPPFLAGS= \
	-I$(MODULE_ROOT)  \
	$(TIRPC_CFLAGS)   \
	$(OPENSSL_CFLAGS) \
	$(GLOBUS_CFLAGS)  \
	$(HPSS_CFLAGS)    \
	-I$(FRAMEWORK)    \
	-Wall -ggdb3

AM_LDFLAGS= \
	$(TIRPC_LDFLAGS)   \
	$(OPENSSL_LDFLAGS) \
	$(GLOBUS_LDFLAGS)  \
	$(HPSS_LDFLAGS)

HPSS_LIBS=-lhpsskrb5auth -lhpssunixauth -lhpss -lhpsscs

LDADD = \
	$(OPENSSL_LIBS) \
	$(TIRPC_LIBS)   \
	$(GLOBUS_LIBS)  \
	$(HPSS_LIBS)    \
	$(FRAMEWORK)/libframework.a \
	-lpthread

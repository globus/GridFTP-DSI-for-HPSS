include ../../../source/module/Makefile.rules

check_PROGRAMS = test_pio
TESTS = $(check_PROGRAMS)

MODULE_ROOT = $(top_srcdir)/source/module
FRAMEWORK = $(top_srcdir)/test/framework

test_pio_SOURCES = test_pio.c

AM_CPPFLAGS= \
	-I$(MODULE_ROOT)    \
	$(MODULE_CPP_FLAGS) \
	-I$(FRAMEWORK)      \
	-Wall -ggdb3

AM_LDFLAGS= \
	$(MODULE_LD_FLAGS)

LDADD = \
	$(MODULE_LIBS) \
	$(FRAMEWORK)/libframework.a \
	-lpthread

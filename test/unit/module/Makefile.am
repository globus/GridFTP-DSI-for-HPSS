include ../../../source/module/Makefile.rules

check_PROGRAMS = \
	test_pio \
	test_utils

TESTS = $(check_PROGRAMS)

FRAMEWORK = $(top_srcdir)/test/framework
MODULE= $(top_srcdir)/source/module

# -rdynamic forces the DSI to use our mocks/hooks
AM_CPPFLAGS= \
	$(MODULE_CPP_FLAGS) \
	-I$(FRAMEWORK)   \
	-I$(MODULE)      \
	-ggdb3           \
	-rdynamic        \
	-DMODULE="\"$(MODULE)/.libs/libglobus_gridftp_server_hpss_real.so\""

AM_CFLAGS= \
	$(MODULE_C_FLAGS)

AM_LDFLAGS=$(MODULE_LD_FLAGS) -ldl -rdynamic

test_pio_SOURCES =      \
	driver.c        \
	gridftp_mocks.c \
	hpss_mocks.c    \
	test_pio.c

test_pio_LDADD = $(FRAMEWORK)/libframework.a

test_utils_SOURCES = driver.c test_utils.c
test_utils_LDADD = $(FRAMEWORK)/libframework.a

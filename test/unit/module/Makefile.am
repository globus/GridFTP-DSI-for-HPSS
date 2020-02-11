check_PROGRAMS = test_pio
TESTS = $(check_PROGRAMS)

FRAMEWORK = $(top_srcdir)/test/framework
MODULE = $(top_srcdir)/source/module

# -rdynamic forces the DSI to use our mocks/hooks
AM_CPPFLAGS=$(GLOBUS_CFLAGS) \
            $(HPSS_CFLAGS)   \
            -I$(FRAMEWORK)       \
            -I$(MODULE)      \
            -ggdb3           \
            -rdynamic        \
            -DMODULE="\"$(MODULE)/.libs/libglobus_gridftp_server_hpss_real.so\""

AM_LDFLAGS=$(GLOBUS_LDFLAGS) $(HPSS_LDFLAGS) -ldl -rdynamic

test_pio_SOURCES = test_pio.c               \
                   gridftp_mocks.c          \
                   hpss_mocks.c             \
                   $(FRAMEWORK)/_display.c      \
                   $(FRAMEWORK)/_events.c       \
                   $(FRAMEWORK)/_expectations.c \
                   $(FRAMEWORK)/testing.c

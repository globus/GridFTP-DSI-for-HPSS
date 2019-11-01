check_PROGRAMS = test_pio
TESTS = $(check_PROGRAMS)

TOOLS = $(top_srcdir)/test/tools
MODULE = $(top_srcdir)/source/module

# -rdynamic forces the DSI to use our mocks/hooks
AM_CPPFLAGS=$(GLOBUS_CFLAGS) \
            $(HPSS_CFLAGS)   \
            -I$(TOOLS)       \
            -I$(MODULE)      \
            -ggdb3           \
            -rdynamic        \
            -DMODULE="\"$(MODULE)/.libs/libglobus_gridftp_server_hpss_real.so\""

AM_LDFLAGS=$(GLOBUS_LDFLAGS) $(HPSS_LDFLAGS) -ldl -rdynamic

test_pio_SOURCES = test_pio.c               \
                   gridftp_mocks.c          \
                   hpss_mocks.c             \
                   $(TOOLS)/_display.c      \
                   $(TOOLS)/_events.c       \
                   $(TOOLS)/_expectations.c \
                   $(TOOLS)/testing.c

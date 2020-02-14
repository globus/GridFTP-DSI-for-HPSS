noinst_PROGRAMS = symlink

symlink_SOURCES = symlink.c


AM_CPPFLAGS=$(TIRPC_CFLAGS) $(HPSS_CFLAGS) -ggdb3
AM_LDFLAGS=$(TIRPC_LDFLAGS) $(HPSS_LDFLAGS)

HPSS_LIBS=-lhpsskrb5auth -lhpssunixauth -lhpss -lhpsscs

symlink_LDADD = $(TIRPC_LIBS) $(HPSS_LIBS)

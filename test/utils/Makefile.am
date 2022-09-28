include ../../source/module/Makefile.rules

noinst_PROGRAMS = symlink

symlink_SOURCES = symlink.c

AM_CPPFLAGS= \
	$(MODULE_CPP_FLAGS) \
	-ggdb3

AM_CFLAGS= $(MODULE_C_FLAGS)

AM_LDFLAGS=$(MODULE_LD_FLAGS)

symlink_LDADD = $(MODULE_LIBS)

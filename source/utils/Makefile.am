include ../module/Makefile.rules

noinst_PROGRAMS=stage

stage_SOURCES=stage.c

AM_CPPFLAGS=$(MODULE_CPP_FLAGS) -I../module/

AM_CFLAGS=$(MODULE_C_FLAGS)

AM_LDFLAGS=$(MODULE_LD_FLAGS)

stage_LDADD=                                                 \
	../module/.libs/libglobus_gridftp_server_hpss_real.a \
	$(MODULE_LIBS)                                       \
	-lpthread

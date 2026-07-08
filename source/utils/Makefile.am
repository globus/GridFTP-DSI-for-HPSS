include ../module/Makefile.rules

noinst_PROGRAMS=\
    stage \
    batch_stage

AM_CPPFLAGS=$(MODULE_CPP_FLAGS) -I../module/

AM_CFLAGS=$(MODULE_C_FLAGS)

AM_LDFLAGS=$(MODULE_LD_FLAGS)

LDADD=                                                   \
	../module/.libs/libglobus_gridftp_server_hpss_real.a \
	$(MODULE_LIBS)                                       \
	-lpthread

lib_LTLIBRARIES = libglobus_gridftp_server_hpss_real.la

SOURCES = _globus_gridftp_server.h \
          authenticate.c  \
          authenticate.h  \
          cksm.c          \
          cksm.h          \
          commands.c      \
          commands.h      \
          config.c        \
          config.h        \
          dsi.c           \
          fixups.c        \
          fixups.h        \
          hooks.c         \
          hpss.c          \
          hpss.h          \
          hpss_error.c    \
          hpss_error.h    \
          hpss_log.c      \
          hpss_log.h      \
          logging.c       \
          logging.h       \
          pio.c           \
          pio.h           \
          pool.c          \
          pool.h          \
          retr.c          \
          retr.h          \
          stage.c         \
          stage.h         \
          stat.c          \
          stat.h          \
          stor.c          \
          stor.h          \
          local_strings.c \
          local_strings.h \
          test.c          \
          test.h          \
          utils.c         \
          utils.h

if ENABLE_GCSV5
SOURCES += gcsv5.c
endif

libglobus_gridftp_server_hpss_real_la_SOURCES=$(SOURCES)

include Makefile.rules

AM_CPPFLAGS=$(MODULE_CPP_FLAGS)
AM_CFLAGS=$(MODULE_C_FLAGS)

AM_LDFLAGS=                                                \
	$(MODULE_LD_FLAGS)                                 \
	-Wl,--wrap=hpss_PIOExecute                         \
	-Wl,--wrap=globus_gridftp_server_begin_transfer    \
	-Wl,--wrap=globus_gridftp_server_finished_transfer \
	-Wl,--wrap=getpwnam_r                              \
	-Wl,--wrap=hpss_LoadDefaultThreadState

libglobus_gridftp_server_hpss_real_la_LIBADD=$(MODULE_LIBS)

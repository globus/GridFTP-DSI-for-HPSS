lib_LTLIBRARIES = libglobus_gridftp_server_hpss_real.la

SOURCES = dsi.c \
	      config.c  \
	      authenticate.c \
	      commands.c \
	      stor.c \
	      retr.c \
	      cksm.c \
	      pio.c \
	      dl.c \
	      markers.c \
	      stage.c \
	      stat.c

AM_CFLAGS=$(GLOBUS_CFLAGS) $(HPSS_CFLAGS)
AM_LDFLAGS=$(GLOBUS_LDFLAGS) $(HPSS_LDFLAGS)

libglobus_gridftp_server_hpss_real_la_SOURCES=$(SOURCES)
libglobus_gridftp_server_hpss_real_la_LIBADD=$(GLOBUS_LIBS) $(HPSS_LIBS)

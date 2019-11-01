lib_LTLIBRARIES = libglobus_gridftp_server_hpss_real.la

SOURCES = authenticate.c \
          authenticate.h \
          cksm.c         \
          cksm.h         \
          commands.c     \
          commands.h     \
          config.c       \
          config.h       \
          dl.c           \
          dl.h           \
          dsi.c          \
          markers.c      \
          markers.h      \
          pio.c          \
          pio.h          \
          retr.c         \
          retr.h         \
          stage.c        \
          stage.h        \
          stat.c         \
          stat.h         \
          stor.c         \
          stor.h

libglobus_gridftp_server_hpss_real_la_SOURCES=$(SOURCES)

AM_CPPFLAGS=$(TIRPC_CFLAGS) $(OPENSSL_CFLAGS) $(GLOBUS_CFLAGS) $(HPSS_CFLAGS)

AM_LDFLAGS=$(TIRPC_LDFLAGS)   \
           $(OPENSSL_LDFLAGS) \
           $(GLOBUS_LDFLAGS)  \
           $(HPSS_LDFLAGS)

#
# IU and NERSC report that using krb fails because libhpssgss.so doesn't link
# against libhpsskrbauth.so. NERSC reported that libhpssunixauth.so was
# necessary in their environment. It is necessary to link these here; the
# XDR/libtirpc issue causes the runtime dynamic linking to fail without this.
#
HPSS_LIBS=-lhpsskrb5auth -lhpssunixauth -lhpss -lhpsscs

# TIRPC must always be first
libglobus_gridftp_server_hpss_real_la_LIBADD=$(TIRPC_LIBS)   \
                                             $(OPENSSL_LIBS) \
                                             $(GLOBUS_LIBS)  \
                                             $(HPSS_LIBS)

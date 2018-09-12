HPSS_ROOT=$(HPSS_LOCATION)

#
# Include the HPSS Makefile to get build information. The HPSS Makefile
# expects LOCAL_ROOT to be set.
#
LOCAL_ROOT=$(HPSS_ROOT)
include $(HPSS_LOCATION)/Makefile.macros

bin_PROGRAMS = dsi_client

dsi_client_SOURCES = dsi_client.c
dsi_client_CPPFLAGS = -std=gnu99
dsi_client_LDFLAGS =-L$(HPSS_ROOT)/lib -Wl,-rpath,$(HPSS_ROOT)/lib $(GLOBUS_LDFLAGS)
dsi_client_LDADD = -lpthread \
                   -lglobus_common \
                   -ltirpc \
                   ../source/module/libglobus_gridftp_server_hpss_real.la \
                   -lhpsskrb5auth \
                   -lhpssunixauth \
                   -lhpss


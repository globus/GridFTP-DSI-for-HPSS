
SOURCES = hpss.c
AM_CFLAGS=$(GLOBUS_CFLAGS)
AM_LDFLAGS=$(GLOBUS_LDFLAGS)

AM_CFLAGS+=-DMODULE_NAME=globus_gridftp_server_hpss_module
AM_CFLAGS+=-DMODULE_NAME_STRING=\"hpss\"
lib_LTLIBRARIES = libglobus_gridftp_server_hpss.la   
libglobus_gridftp_server_hpss_la_SOURCES=$(SOURCES)
libglobus_gridftp_server_hpss_la_LIBADD = -ldl

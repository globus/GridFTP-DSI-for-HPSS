
SOURCES = hpss.c
AM_CFLAGS=$(GLOBUS_CFLAGS)
AM_LDFLAGS=$(GLOBUS_LDFLAGS)

if ENABLE_GCSV5
AM_CFLAGS+=-DMODULE_NAME=globus_gridftp_server_hpss_module
AM_CFLAGS+=-DMODULE_NAME_STRING=\"hpss\"
lib_LTLIBRARIES = libglobus_gridftp_server_hpss.la   
libglobus_gridftp_server_hpss_la_SOURCES=$(SOURCES)
libglobus_gridftp_server_hpss_la_LIBADD = -ldl
else
AM_CFLAGS+=-DMODULE_NAME=globus_gridftp_server_hpss_local_module
AM_CFLAGS+=-DMODULE_NAME_STRING=\"hpss_local\"
lib_LTLIBRARIES = libglobus_gridftp_server_hpss_local.la   
libglobus_gridftp_server_hpss_local_la_SOURCES=$(SOURCES)
libglobus_gridftp_server_hpss_local_la_LIBADD = -ldl
endif

HPSS_ROOT=$(HPSS_LOCATION)
MODULE_SRC_PATH = ../../source/module

#
# Include the HPSS Makefile to get build information. The HPSS Makefile
# expects LOCAL_ROOT to be set.
#
LOCAL_ROOT=$(HPSS_ROOT)
include $(HPSS_LOCATION)/Makefile.macros

bin_PROGRAMS = stage_file

AM_CPPFLAGS = -std=gnu99 -Wall -DLINUX -DTESTING -I$(MODULE_SRC_PATH) -I$(HPSS_ROOT)/include -I/usr/include/globus
LDADD = -lhpss -lglobus_gridftp_server -ltirpc
AM_LDFLAGS = -L$(HPSS_ROOT)/lib -Wl,-rpath,$(HPSS_ROOT)/lib

stage_file_SOURCES = stage_file.c $(MODULE_SRC_PATH)/stage.c


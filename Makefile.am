##########################################################################
# University of Illinois/NCSA Open Source License
#
# Copyright © 2012, NCSA.  All rights reserved.
#
# Developed by:
#
# Storage Enabling Technologies (SET)
#
# Nation Center for Supercomputing Applications (NCSA)
#
# http://www.ncsa.illinois.edu
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the .Software.),
# to deal with the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
#    + Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimers.
#
#    + Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimers in the
#      documentation and/or other materials provided with the distribution.
#
#    + Neither the names of SET, NCSA
#      nor the names of its contributors may be used to endorse or promote
#      products derived from this Software without specific prior written
#      permission.
#
# THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS WITH THE SOFTWARE.
############################################################################

LOCAL_ROOT=$(HPSS_DIR)
include $(HPSS_DIR)/Makefile.macros

# SUBDIRS is an automake macro which contains
# the list of subdirectories that have Makefile.am's
SUBDIRS=

# Build targets of the form <dir>_<primary> where dir is where to install it:
#   Ex. bin, man, lib, pkglib, include
# and primary is what type of target it is:
#   Ex. PROGRAMS, MANS, LTLIBRARIES, SCRIPTS, DATA, LIBRARIES, HEADERS
# Note: LTLIBRARIES is a libtool library
# Note: The names of these libraries are important, the gridftp server appends
# our dsi name (hpss_control or hpss_data) onto libglobus_gridftp_server_ when loading 
# our DSI.
lib_LTLIBRARIES = libglobus_gridftp_server_hpss_local.la
#	              libglobus_gridftp_server_hpss_control.la \
#	              libglobus_gridftp_server_hpss_data.la

Common_Sources = gridftp_dsi_hpss_transfer_control.c \
	             gridftp_dsi_hpss_transfer_control.h \
	             gridftp_dsi_hpss_transfer_data.c    \
	             gridftp_dsi_hpss_transfer_data.h    \
	             gridftp_dsi_hpss_data_ranges.c      \
	             gridftp_dsi_hpss_data_ranges.h      \
	             gridftp_dsi_hpss_pio_control.c      \
	             gridftp_dsi_hpss_pio_control.h      \
	             gridftp_dsi_hpss_range_list.c       \
	             gridftp_dsi_hpss_range_list.h       \
	             gridftp_dsi_hpss_commands.c         \
	             gridftp_dsi_hpss_commands.h         \
	             gridftp_dsi_hpss_checksum.c         \
	             gridftp_dsi_hpss_checksum.h         \
	             gridftp_dsi_hpss_pio_data.c         \
	             gridftp_dsi_hpss_pio_data.h         \
	             gridftp_dsi_hpss_gridftp.c          \
	             gridftp_dsi_hpss_gridftp.h          \
	             gridftp_dsi_hpss_session.c          \
	             gridftp_dsi_hpss_session.h          \
	             gridftp_dsi_hpss_buffer.c           \
	             gridftp_dsi_hpss_buffer.h           \
	             gridftp_dsi_hpss_config.c           \
	             gridftp_dsi_hpss_config.h           \
	             gridftp_dsi_hpss_misc.c             \
	             gridftp_dsi_hpss_misc.h             \
	             gridftp_dsi_hpss_list.c             \
	             gridftp_dsi_hpss_list.h             \
	             gridftp_dsi_hpss_msg.c              \
	             gridftp_dsi_hpss_msg.h

#	             gridftp_dsi_hpss_ipc_data.c         \
#	             gridftp_dsi_hpss_ipc_data.h         \
#	             gridftp_dsi_hpss_xio_server.c       \
#	             gridftp_dsi_hpss_xio_server.h       \
#	             gridftp_dsi_hpss_xio_client.c       \
#	             gridftp_dsi_hpss_xio_client.h       \
#	             gridftp_dsi_hpss_ipc_control.c      \
#	             gridftp_dsi_hpss_ipc_control.h

#DMALLOC_FLAGS=-DDMALLOC -DDMALLOC_FUNC_CHECK -I/usr/local/dmalloc/include
#DMALLOC_LIBS=/usr/local/dmalloc/lib/libdmallocth.a

# For each build target, define the primaries. SOURCES lists the source files
# included for 'make dist'. LDFLAGS are linker flags. CFLAGS are compiler flags.
# LDADD are linker adds (libraries need to build programs) and LIBADD are
# library adds (libraries need to build libraries).
# 
libglobus_gridftp_server_hpss_local_la_SOURCES = $(Common_Sources) gridftp_dsi_hpss_local.c
#libglobus_gridftp_server_hpss_control_la_SOURCES = $(Common_Sources) gridftp_dsi_hpss_control.c
#libglobus_gridftp_server_hpss_data_la_SOURCES = $(Common_Sources) gridftp_dsi_hpss_data.c

libglobus_gridftp_server_hpss_local_la_LIBADD = -lhpss $(HPSS_AUTH_LIBS) $(GLOBUS_LIBS) $(DMALLOC_LIBS)
#libglobus_gridftp_server_hpss_control_la_LIBADD = -lhpss $(HPSS_AUTH_LIBS) $(GLOBUS_LIBS) $(DMALLOC_LIBS)
#libglobus_gridftp_server_hpss_data_la_LIBADD = -lhpss $(HPSS_AUTH_LIBS) $(GLOBUS_LIBS) $(DMALLOC_LIBS)
# These primaries are common to both targets, hence no prefix
LDFLAGS= $(RPATHOPT) -L$(HPSS_DIR)/lib $(GLOBUS_LDFLAGS) $(DMALLOC_LIBS)
CFLAGS= $(DMALLOC_FLAGS) $(GLOBUS_CFLAGS) $(CC_BIT64_FLAGS) -D$(BUILD_PLATFORM)
# This hack is in until I can figure out why globus_makefile_header in
# 5.2.0 does not return include/globus/<flavor>/
CPPFLAGS= -I$(HPSS_DIR)/include $(GLOBUS_CPPFLAGS) -I@GLOBUS_LOCATION@/include/globus/@GLOBUS_FLAVOR_NAME@

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
# Note: The names of these libraries are important, the gridftp serer appends
# our dsi name (hpss_control or hpss_data) onto libglobus_gridftp_server_ when
# loading our DSI.
lib_LTLIBRARIES = libglobus_gridftp_server_hpss.la

Sources = globus_gridftp_server_hpss_module.c \
	      globus_gridftp_server_hpss_common.c \
	      globus_gridftp_server_hpss_common.h \
	      globus_gridftp_server_hpss_config.c \
	      globus_gridftp_server_hpss_config.h

# For each build target, define the primaries. SOURCES lists the source files
# included for 'make dist'. LDFLAGS are linker flags. CFLAGS are compiler flags.
# LDADD are linker adds (libraries need to build programs) and LIBADD are
# library adds (libraries need to build libraries).
# 
libglobus_gridftp_server_hpss_la_SOURCES = $(Sources)
#install:

# These primaries are common to both targets, hence no prefix
libglobus_gridftp_server_hpss_la_LIBADD = -lhpss $(HPSS_AUTH_LIBS) $(GLOBUS_LIBS)
LDFLAGS= $(RPATHOPT) -L$(HPSS_DIR)/lib $(GLOBUS_LDFLAGS)
CFLAGS= $(GLOBUS_CFLAGS) $(CC_BIT64_FLAGS) -D$(BUILD_PLATFORM)
# This hack is in until I can figure out why globus_makefile_header in
# 5.2.0 does not return include/globus/<flavor>/
CPPFLAGS= -I$(HPSS_DIR)/include $(GLOBUS_CPPFLAGS) -I@GLOBUS_LOCATION@/include/globus/@GLOBUS_FLAVOR_NAME@



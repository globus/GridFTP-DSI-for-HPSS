##########################################################################
# University of Illinois/NCSA Open Source License
#
# Copyright © 2012-2015, NCSA.  All rights reserved.
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

libglobus_gridftp_server_hpss_local_la_CPPFLAGS = -Wall $(GLOBUS_LIBS) -I/usr/lib64/globus/include -I/usr/include/globus
libglobus_gridftp_server_hpss_local_la_SOURCES = hpss_local.c common_loader.c
libglobus_gridftp_server_hpss_local_la_LIBADD = -ldl

# Uncommeting this line would cause libtool to use gcc to link the libraries
# instead of g++ when at least one file ends in .cc
#CXXLINK = $(LIBTOOL) --tag=CC --mode=link $(CCLD) $(AM_CCFLAGS) $(CCFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@


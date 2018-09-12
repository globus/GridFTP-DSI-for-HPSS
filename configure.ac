dnl
dnl  University of Illinois/NCSA Open Source License
dnl
dnl  Copyright © 2012-2015, NCSA.  All rights reserved.
dnl
dnl  Developed by:
dnl
dnl  Storage Enabling Technologies (SET)
dnl
dnl  Nation Center for Supercomputing Applications (NCSA)
dnl
dnl  http://dims.ncsa.uiuc.edu/set/uberftp
dnl
dnl  Permission is hereby granted, free of charge, to any person obtaining a
dnl  copy of this software and associated documentation files (the .Software.),
dnl  to deal with the Software without restriction, including without limitation
dnl  the rights to use, copy, modify, merge, publish, distribute, sublicense,
dnl  and/or sell copies of the Software, and to permit persons to whom the
dnl  Software is furnished to do so, subject to the following conditions:
dnl
dnl     + Redistributions of source code must retain the above copyright notice,
dnl       this list of conditions and the following disclaimers.
dnl
dnl     + Redistributions in binary form must reproduce the above copyright
dnl       notice, this list of conditions and the following disclaimers in the
dnl       documentation and/or other materials provided with the distribution.
dnl
dnl     + Neither the names of SET, NCSA
dnl       nor the names of its contributors may be used to endorse or promote
dnl       products derived from this Software without specific prior written
dnl       permission.
dnl
dnl  THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
dnl  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
dnl  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
dnl  THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
dnl  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
dnl  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
dnl  DEALINGS WITH THE SOFTWARE.
dnl

AC_INIT([GridFTP HPSS DSI], [2.5.2], [Globus <support@globus.org>])
AM_INIT_AUTOMAKE
AC_PROG_LIBTOOL
AC_PROG_CXX
AC_PROG_CC
AC_CONFIG_MACRO_DIR([m4])

#
# For checksums
#
AC_CHECK_HEADERS([openssl/md5.h], [MD5_HEADER="yes"], [AC_MSG_ERROR(Missing openssl/md5.h)])

#
# Globus Setup
#
AC_ARG_WITH(globus,
	[  --with-globus=value     location of Globus installation, defaults to /usr],
	[  globus_location="$withval" ],
	[  globus_location="/usr" ]
)

#
# globus_common.h depends on globus_config.h, so do it first
#
if test "x$globus_location" == "x/usr" ; then
	# RPM installation
	CPPFLAGS="-I$globus_location/lib64/globus/include -I/usr/include/globus"
	AC_CHECK_HEADERS([globus_config.h],
	                 [GLOBUS_CONFIG_CPPFLAGS="$CPPFLAGS"],
	                 [AC_MSG_ERROR([globus_config.h not found, install globus-core])])
else
	# Source installation
	CPPFLAGS=-I$globus_location/include/globus/gcc64dbg
	AC_CHECK_HEADERS([globus_config.h],
	                 [GLOBUS_CONFIG_CPPFLAGS="$CPPFLAGS"],
	                 [AC_MSG_ERROR(globus_config.h not found in $globus_location)])
fi

#
# Now check for globus_common.h using globus_config.h's location
#
CPPFLAGS="-I$globus_location/include/globus $GLOBUS_CONFIG_CPPFLAGS"
AC_CHECK_HEADERS([globus_common.h],
                 [GLOBUS_COMMON_CPPFLAGS="-I$globus_location/include/globus"],
                 [AC_MSG_ERROR(globus_common.h not found)])

#
# Sum up all the Globus CPPFLAGS
#
GLOBUS_CPPFLAGS=$GLOBUS_CONFIG_CPPFLAGS
if test "${GLOBUS_CONFIG_CPPFLAGS}" != "${GLOBUS_COMMON_CPPFLAGS}"; then
	GLOBUS_CPPFLAGS="$GLOBUS_CPPFLAGS $GLOBUS_COMMON_CPPFLAGS"
fi

#
# Now check for globus_gridftp_server.h
#
CPPFLAGS="${GLOBUS_CPPFLAGS}"
AC_CHECK_HEADERS([globus_gridftp_server.h],
                 [],
                 [AC_MSG_ERROR(globus_gridftp_server.h not found)])

AC_SUBST(GLOBUS_CPPFLAGS)

#
# Find the needed Globus libraries
#
LDFLAGS="-L$globus_location/lib64/"
AC_CHECK_LIB([globus_gridftp_server],
             [globus_gridftp_server_begin_transfer],
             [GLOBUS_LDFLAGS=-L$globus_location/lib64],
             [AC_MSG_ERROR(libglobus_gridftp_server.so not found)])

AC_SUBST(GLOBUS_LDFLAGS)

dnl
dnl Find the HPSS installation
dnl
hpss_location=/opt/hpss
AC_ARG_WITH(hpss, AC_HELP_STRING([--with-hpss=dir],\
   [location of HPSS installation, defaults to /opt/hpss]),
   [hpss_location=$withval],
   [])
AC_SUBST(HPSS_LOCATION, $hpss_location)

dnl
dnl These values are hardcoded for now, until I can get the packaging stuff working again
dnl
DIRT_TIMESTAMP=`perl -e 'print time'`
DIRT_BRANCH_ID=99999

AC_CONFIG_HEADERS([config.h])

AC_SUBST([MAJOR_VERSION], [${PACKAGE_VERSION%%.*}])
AC_SUBST([MINOR_VERSION], [${PACKAGE_VERSION##*.}])
AC_SUBST(DIRT_TIMESTAMP)
AC_SUBST(DIRT_BRANCH_ID)
AC_SUBST(REAL_CC, CC)

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([source/Makefile])
AC_CONFIG_FILES([source/loaders/Makefile])
AC_CONFIG_FILES([source/loaders/version.h])
AC_CONFIG_FILES([source/module/Makefile])
AC_CONFIG_FILES([utils/Makefile])
AC_OUTPUT

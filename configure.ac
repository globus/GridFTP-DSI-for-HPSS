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

AC_INIT([GridFTP HPSS DSI], [2.8], [Globus <support@globus.org>])
AM_INIT_AUTOMAKE
AC_PROG_LIBTOOL
AC_PROG_CXX
AC_PROG_CC
AC_CONFIG_MACRO_DIR([m4])

PKG_CHECK_MODULES([OPENSSL],[openssl])
PKG_CHECK_MODULES([GLOBUS], [globus-gridftp-server])

# hpss-clnt-devel-7.5 has a pkg config but 7.3, 7.4 does not
HPSS_CFLAGS="-DLINUX -DLITTLEEND -I/opt/hpss/include"
HPSS_LDFLAGS="-L/opt/hpss/lib -Wl,-rpath,/opt/hpss/lib"

SAVE_CPPFLAGS="${CPPFLAGS}"
CPPFLAGS="${HPSS_CFLAGS}"
AC_CHECK_HEADERS([hpss_String.h \
                  hpss_errno.h \
                  hpss_mech.h \
                  hpss_api.h \
                  hpss_version.h \
                  hpss_xml.h \
                  hpss_Getenv.h \
                  hpss_net.h \
                  hpss_stat.h],
                 [], 
                 [AC_MSG_ERROR(hpss-lib-devel is not installed)])
CPPFLAGS="${SAVE_CPPFLAGS}"

AC_CHECK_LIB(hpss, hpss_Lstat, [hpss-lib-devel is not installed])

#
# IU and NERSC report that using krb fails because libhpssgss.so doesn't link against
# libhpsskrbauth.so. NERSC reported that libhpssunixauth.so was necessary in their
# environment. It is necessary to link these here; the XDR/libtirpc issue causes the
# runtime dynamic linking to fail without this.
#
HPSS_LIBS="-lhpsskrb5auth -lhpssunixauth -lhpss -lhpsscs"
AC_SUBST(HPSS_CFLAGS)
AC_SUBST(HPSS_LDFLAGS)
AC_SUBST(HPSS_LIBS)


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
AC_CONFIG_FILES([test/module/Makefile])
AC_OUTPUT

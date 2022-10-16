# Use fedora version formatting, ie. 2.17, 2.17~rc1.
AC_INIT([globus-gridftp-server-hpss], [2.18~rc2], [Globus <support@globus.org>])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
# automake 1.11.1 (RHEL 6.8) does not support AM_PROG_AR
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])
AC_PROG_CC
# -std=gnu99 is not supported on RHEL 6.8
CC="${CC} -std=gnu99"

AC_ARG_ENABLE([gcsv5],
    [AS_HELP_STRING([--enable-gcsv5], [Build for Globus Connect Server v5])])

AM_CONDITIONAL([ENABLE_GCSV5], [test x$enable_gcsv5 = xyes])

PKG_CHECK_MODULES([OPENSSL],[openssl])
PKG_CHECK_MODULES([TIRPC],  [libtirpc])

# GCSv4 ships globus-gridftp-server-devel. GCSv5 ships
# globus-connect-gridftp-server-devel. pkg-config reports
# globus-connect-gridftp-server-devel as globus-gridftp-server-devel. So, do
# look for globus-gridftp-server-devel for GCSv4 and GCSv5, and do allow
# GCSv5 versions to be used for the GCSv4 build. Do not search for
# globus-connect-gridftp-server-devel.

MIN_GCS_VERSION=12.24
if [[ x$enable_gcsv5 = xyes ]];
then
    MIN_GCS_VERSION=15.91
fi

PKG_CHECK_MODULES([GLOBUS], [globus-gridftp-server >= $MIN_GCS_VERSION globus-common])

AC_ARG_WITH([hpss],
            [AS_HELP_STRING([--with-hpss=<path>],
                            [path to HPSS installation. [default=/opt/hpss]])],
            [HPSS_ROOT=$withval],
            [HPSS_ROOT=/opt/hpss])

# hpss-clnt-devel-7.5 has a pkg config but 7.3, 7.4 does not
HPSS_CFLAGS="-DLINUX -DLITTLEEND -I${HPSS_ROOT}/include"
HPSS_LDFLAGS="-L${HPSS_ROOT}/lib -Wl,-rpath,${HPSS_ROOT}/lib"

SAVE_CPPFLAGS="${CPPFLAGS}"
# RHEL 8.4 doesn't supply /usr/include/rpc/rpc.h do we need to pass through the
# tirpc module includes found above.
CPPFLAGS="${TIRPC_CFLAGS} ${HPSS_CFLAGS}"
AC_CHECK_HEADERS([hpss_String.h \
                  hpss_errno.h \
                  hpss_mech.h \
                  hpss_api.h \
                  hpss_version.h \
                  hpss_xml.h \
                  hpss_Getenv.h \
                  hpss_net.h \
                  hpss_stat.h],
                 [DONT_POLLUTE_INCLUDES=], 
                 [AC_MSG_ERROR(hpss-lib-devel is not installed)])
CPPFLAGS="${SAVE_CPPFLAGS}"

SAVE_LDFLAGS="${LDFLAGS}"
LDFLAGS="${HPSS_LDFLAGS}"
AC_CHECK_LIB(hpss,
             hpss_Lstat,
             [DONT_POLLUTE_LIBS=],
             [AC_MSG_ERROR(hpss-lib-devel is not installed)])
LDFLAGS="${SAVE_LDFLAGS}"

HPSS_MAJOR=`grep -E "#define HPSS_MAJOR_VERSION" /opt/hpss/include/hpss_version.h | sed "s/HPSS_MAJOR_VERSION *0/HPSS_MAJOR_VERSION /"  | awk '{print $3}'`
HPSS_MINOR=`grep -E "#define HPSS_MINOR_VERSION" /opt/hpss/include/hpss_version.h | sed "s/VERSION *0/VERSION /"  | awk '{print $3}'`


AC_SUBST(MIN_GCS_VERSION)
AC_SUBST(HPSS_CFLAGS)
AC_SUBST(HPSS_LDFLAGS)
AC_SUBST([MAJOR_VERSION], [${PACKAGE_VERSION%%.*}])
_MINOR_VERSION="${PACKAGE_VERSION#*.}"
AC_SUBST([MINOR_VERSION], [${_MINOR_VERSION%~*}])
AC_SUBST(PACKAGE_NAME)
AC_SUBST(HPSS_MAJOR)
AC_SUBST(HPSS_MINOR)

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([source/Makefile])
AC_CONFIG_FILES([source/loader/Makefile])
AC_CONFIG_FILES([source/loader/version.h])
AC_CONFIG_FILES([source/module/Makefile])
AC_CONFIG_FILES([source/utils/Makefile])
AC_CONFIG_FILES([test/Makefile])
AC_CONFIG_FILES([test/framework/Makefile])
AC_CONFIG_FILES([test/utils/Makefile])
AC_CONFIG_FILES([test/unit/Makefile])
AC_CONFIG_FILES([test/unit/module/Makefile])
AC_CONFIG_FILES([test/integration/Makefile])
AC_CONFIG_FILES([test/integration/contracts/Makefile])
AC_CONFIG_FILES([packaging/fedora/globus-gridftp-server-hpss.spec])

AC_OUTPUT

AC_INIT([globus-gridftp-server-hpss], [2.9], [Globus <support@globus.org>])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
AM_PROG_AR
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])
AC_PROG_CC
CC="${CC} -std=gnu11"

PKG_CHECK_MODULES([OPENSSL],[openssl])
PKG_CHECK_MODULES([GLOBUS], [globus-gridftp-server >= 12])
PKG_CHECK_MODULES([TIRPC],  [libtirpc])

AC_ARG_WITH([hpss],
            [AS_HELP_STRING([--with-hpss=<path>],
                            [path to HPSS installation. [default=/opt/hpss]])],
            [HPSS_ROOT=$withval],
            [HPSS_ROOT=/opt/hpss])

# hpss-clnt-devel-7.5 has a pkg config but 7.3, 7.4 does not
HPSS_CFLAGS="-DLINUX -DLITTLEEND -I${HPSS_ROOT}/include"
HPSS_LDFLAGS="-L${HPSS_ROOT}/lib -Wl,-rpath,${HPSS_ROOT}/lib"

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

SAVE_LDFLAGS="${LDFLAGS}"
LDFLAGS="${HPSS_LDFLAGS}"
AC_CHECK_LIB(hpss,
             hpss_Lstat,
             [DONT_POLLUTE_LIBS=],
             [AC_MSG_ERROR(hpss-lib-devel is not installed)])
LDFLAGS="${SAVE_LDFLAGS}"

HPSS_MAJOR=`grep -E "#define HPSS_MAJOR_VERSION" /opt/hpss/include/hpss_version.h | sed "s/HPSS_MAJOR_VERSION *0/HPSS_MAJOR_VERSION /"  | awk '{print $3}'`
HPSS_MINOR=`grep -E "#define HPSS_MINOR_VERSION" /opt/hpss/include/hpss_version.h | sed "s/VERSION *0/VERSION /"  | awk '{print $3}'`


AC_SUBST(HPSS_CFLAGS)
AC_SUBST(HPSS_LDFLAGS)
AC_SUBST([MAJOR_VERSION], [${PACKAGE_VERSION%%.*}])
AC_SUBST([MINOR_VERSION], [${PACKAGE_VERSION##*.}])
AC_SUBST(PACKAGE_NAME)
AC_SUBST(HPSS_MAJOR)
AC_SUBST(HPSS_MINOR)

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([source/Makefile])
AC_CONFIG_FILES([source/loader/Makefile])
AC_CONFIG_FILES([source/loader/version.h])
AC_CONFIG_FILES([source/module/Makefile])
AC_CONFIG_FILES([test/Makefile])
AC_CONFIG_FILES([test/unit/Makefile])
AC_CONFIG_FILES([test/unit/module/Makefile])
AC_CONFIG_FILES([test/integration/Makefile])
AC_CONFIG_FILES([test/integration/contracts/Makefile])
AC_CONFIG_FILES([packaging/fedora/globus-gridftp-server-hpss.spec])

AC_OUTPUT

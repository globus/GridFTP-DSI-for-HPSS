# Use fedora version formatting, ie. 2.17, 2.17~rc1.
AC_INIT([globus-gridftp-server-hpss], [2.22], [Globus <support@globus.org>])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
# automake 1.11.1 (RHEL 6.8) does not support AM_PROG_AR
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])
AC_PROG_CC
# -std=gnu99 is not supported on RHEL 6.8
CC="${CC} -std=gnu99"

#
# Configure Options
#

AC_ARG_ENABLE([gcsv5],
    [AS_HELP_STRING([--enable-gcsv5], [Build for Globus Connect Server v5])])

AM_CONDITIONAL([ENABLE_GCSV5], [test x$enable_gcsv5 = xyes])

AC_ARG_WITH([hpss],
            [AS_HELP_STRING([--with-hpss=<path>],
                            [path to HPSS installation. [default=/opt/hpss]])],
            [HPSS_ROOT=$withval],
            [HPSS_ROOT=/opt/hpss])

#
# Globus Dependencies
#

# - GCSv4 ships globus-gridftp-server-devel.
# - GCSv5 ships globus-connect-gridftp-server-devel.
# - pkg-config reports globus-connect-gridftp-server-devel as globus-gridftp-server-devel.
# - look for globus-gridftp-server-devel for GCSv4 and GCSv5
# - allow GCSv5 versions to be used for the GCSv4 build
# - Do not search for globus-connect-gridftp-server-devel

MIN_GCSV4_GRIDFTP_VERSION=12.24
MIN_GCSV5_GRIDFTP_VERSION=15.91
MIN_GCSV5_VERSION=5.4.43

# - Do not check for globus-connect-server version MIN_GCSV5_VERSION. We leave that
#   as a Requires in the packaging so that we can build GCSv5 releases on hosts running
#   GCSv4.

# If the caller is enabling GCSv5 support, do not let the following PKG_CHECK_MODULE
# succced on the V4 GridFTP version check.
MIN_GRIDFTP_VERSION=$MIN_GCSV4_GRIDFTP_VERSION
if [[ x$enable_gcsv5 = xyes ]];
then
	MIN_GRIDFTP_VERSION=$MIN_GCSV5_GRIDFTP_VERSION
fi

# - Track whether it is possible to build GCSv5 versions, regardless of whether or not
#   the user asked us to build it, so that we can notify the release build system.
_GCSV5_BUILD_AVAILABLE=0

# - Check whether we have the v5 or v4 version of the gridftp server libraries.
PKG_CHECK_MODULES(
	[GLOBUS],
	[globus-gridftp-server >= $MIN_GCSV5_GRIDFTP_VERSION globus-common],
	[_GCSV5_BUILD_AVAILABLE=1],
	[PKG_CHECK_MODULES(
		[GLOBUS],
		[globus-gridftp-server >= $MIN_GRIDFTP_VERSION globus-common]
	)]
)

# - This is used by the release process to know whether or not to attempt to
#   build v5 packages.
AM_CONDITIONAL([GCSV5_BUILD_AVAILABLE], [test x$_GCSV5_BUILD_AVAILABLE = x1])

# - Do not use Globus Community Toolkit versions
_REPO_URL="https://downloads.globus.org/globus-connect-server/stable/installers/repo/rpm/globus-repo-latest.noarch.rpm"
_GCT_MSG="Globus Community Toolkit versions (13.x) are not compatible. "`
	`"This happens when Globus is installed from EPEL. Use the RPM repo "`
	`"definitions at $_REPO_URL and install globus-gridftp-server-devel version 12.x or 15.x."

PKG_CHECK_MODULES(
	[GCT],
	[globus-gridftp-server >= 13 globus-gridftp-server < 15],
	[AC_MSG_ERROR($_GCT_MSG)],
	[]
)


#
# Always-Required Third-Party Dependencies
#

PKG_CHECK_MODULES([OPENSSL],[openssl])
PKG_CHECK_MODULES([TIRPC],  [libtirpc])

#
# GCSv5-Required Third-Party Dependencies
#

# We need to check for GCSv5 dependencies in two scenarios:
# - if the user is building for GCSv5, the dependencies _must_ be installed
# - we want to tell automake whether a GCSv5 release build is possible, in which
#   case the dependencies may or may not be installed
if [[ x$enable_gcsv5 = xyes ]];
then
	PKG_CHECK_MODULES([JANSSON], [jansson])
elif [[ x$_GCSV5_BUILD_AVAILABLE = x1 ]];
then
	PKG_CHECK_MODULES([JANSSON], [jansson], [], [_GCSV5_BUILD_AVAILABLE=0])
fi

#
# HPSS Dependencies
#

HPSS_MAJOR=`grep -E "#define HPSS_MAJOR_VERSION" ${HPSS_ROOT}/include/hpss_version.h | sed "s/HPSS_MAJOR_VERSION *0/HPSS_MAJOR_VERSION /"  | awk '{print $3}'`
HPSS_MINOR=`grep -E "#define HPSS_MINOR_VERSION" ${HPSS_ROOT}/include/hpss_version.h | sed "s/VERSION *0/VERSION /"  | awk '{print $3}'`


# HPSS < 7.5 did not have a pkg_config
if [[[ $HPSS_MAJOR -gt 7 || $HPSS_MINOR -ge 5 ]]];
then
    export PKG_CONFIG_PATH=${HPSS_ROOT}/lib
    PKG_CHECK_MODULES(
	   [HPSS],
	   [hpss],
	   [_HPSS_LIB_FOUND=1],
	   [_HPSS_LIB_FOUND=0],
   )

    fields=($HPSS_CFLAGS $HPSS_LIBS)
    for f in ${fields[[@]]}
    do
        case $f in
            -D*|-I*|-pthread|-fms-extensions|-m64)
                _HPSS_CPPFLAGS+="$f "
                ;;
            -l*)
                _HPSS_LIBS+="$f "
                ;;
            -L*|-Wl*)
                _HPSS_LDFLAGS+="$f "
                ;;
            *)
                echo Unexpected field $f, ignoring
                ;;
        esac
    done

    HPSS_CPPFLAGS=$_HPSS_CPPFLAGS
    HPSS_LDFLAGS=$_HPSS_LDFLAGS
    HPSS_LIBS=$_HPSS_LIBS

else
    HPSS_CPPFLAGS="-DLINUX -DLITTLEEND -I${HPSS_ROOT}/include"
    HPSS_LDFLAGS="-L${HPSS_ROOT}/lib -Wl,-rpath,${HPSS_ROOT}/lib"
    HPSS_LIBS="-lhpsskrb5auth -lhpssunixauth -lhpss -lhpsscs"
fi

SAVE_CPPFLAGS="${CPPFLAGS}"
# RHEL 8.4 doesn't supply /usr/include/rpc/rpc.h so we need to pass through the
# tirpc module includes found above.
CPPFLAGS="${TIRPC_CFLAGS} ${HPSS_CPPFLAGS}"
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


#
# Output
#


AC_SUBST(MIN_GCSV4_GRIDFTP_VERSION)
AC_SUBST(MIN_GCSV5_GRIDFTP_VERSION)
AC_SUBST(MIN_GCSV5_VERSION)
AC_SUBST(HPSS_CPPFLAGS)
AC_SUBST(HPSS_LDFLAGS)
AC_SUBST(HPSS_LIBS)
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

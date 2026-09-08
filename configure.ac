# Use fedora version formatting, ie. 2.17, 2.17~rc1.
AC_INIT([globus-gridftp-server-hpss], [2.26~a7], [Globus <support@globus.org>])
# foreign: don't require GNU standard files (ChangeLog, AUTHORS, NEWS, etc.)
AM_INIT_AUTOMAKE([-Wall -Werror foreign])

AM_PROG_AR                  # locate the archiver; required when libtool builds static libs
AC_CONFIG_MACRO_DIR([m4])   # tell aclocal to search m4/ for third-party macros (e.g. libtool)
LT_INIT                     # initialize libtool for portable shared/static library builds
AC_PROG_CC                  # locate and test the C compiler; sets CC, CFLAGS, GCC

#
# Code for the newest C standard supported by our oldest supported distro (EL8)
#
AC_MSG_CHECKING([whether ${CC} accepts -std=gnu17])
ac_save_CC="${CC}"
CC="${CC} -std=gnu17"
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([],[])],
    [AC_MSG_RESULT([yes])],
    [AC_MSG_RESULT([no])
     CC="${ac_save_CC}"
     AC_MSG_ERROR([${CC} does not support -std=gnu17; GCC 8 or newer is required.])])

#
# Configure Options
#

AC_ARG_WITH([hpss],
            [AS_HELP_STRING([--with-hpss=<path>],
                            [path to HPSS installation. [default=/opt/hpss]])],
            [HPSS_ROOT=$withval],
            [HPSS_ROOT=/opt/hpss])

AM_CONDITIONAL([NONCANONICAL_HPSS], [test "$HPSS_ROOT" != "/opt/hpss"])

#
# Globus Dependencies
#

#  GCSv5 ships globus-connect-gridftp-server-devel but pkg-config reports it as
#  globus-gridftp-server-devel.

# 16.18 introduced API necessary for the improved staging development
MIN_GCSV5_GRIDFTP_VERSION=16.18
# Used only in the RPM spec as a runtime dep on globus-connect-server54.
MIN_GCSV5_VERSION=5.4.43

PKG_CHECK_MODULES(
	[GLOBUS],
	[globus-gridftp-server >= $MIN_GCSV5_GRIDFTP_VERSION globus-common]
)

# Detect and reject Globus Community Toolkit 13.x (from EPEL): error on found,
# not on missing — the inverse of normal PKG_CHECK_MODULES usage.
_REPO_URL="https://downloads.globus.org/globus-connect-server/stable/installers/repo/rpm/globus-repo-latest.noarch.rpm"
_GCT_MSG="Globus Community Toolkit versions (13.x) are not compatible. \
This happens when Globus is installed from EPEL. Use the RPM repo \
definitions at $_REPO_URL and install globus-gridftp-server-devel version 12.x or 15.x."

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
PKG_CHECK_MODULES([JANSSON], [jansson])

#
# HPSS Dependencies
#

# hpss_version.h zero-pads version fields (e.g., decimal "08"); strip leading
# zeros so the values are valid in shell arithmetic comparisons.
HPSS_MAJOR=$(awk '/#define HPSS_MAJOR_VERSION/ {gsub(/^0+/, "", $3); print $3}' \
             "${HPSS_ROOT}/include/hpss_version.h")
HPSS_MINOR=$(awk '/#define HPSS_MINOR_VERSION/ {gsub(/^0+/, "", $3); print $3}' \
             "${HPSS_ROOT}/include/hpss_version.h")

AS_IF([test "$HPSS_MAJOR" -lt 7 || { test "$HPSS_MAJOR" -eq 7 && test "$HPSS_MINOR" -lt 3; }],
    [AC_MSG_ERROR([HPSS ${HPSS_MAJOR}.${HPSS_MINOR} is not supported. This project requires HPSS >= 7.3.])])

# HPSS < 7.5 did not have a pkg_config
AS_IF([test "$HPSS_MAJOR" -gt 7 || { test "$HPSS_MAJOR" -eq 7 && test "$HPSS_MINOR" -ge 5; }],
[
    # Prepend HPSS lib dir; ${VAR:+:${VAR}} avoids a trailing colon when VAR is unset.
    PKG_CONFIG_PATH="${HPSS_ROOT}/lib${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
    export PKG_CONFIG_PATH
    PKG_CHECK_MODULES(
	   [HPSS],
	   [hpss],
	   [_HPSS_LIB_FOUND=1],
	   [_HPSS_LIB_FOUND=0],
    )

    # PKG_CHECK_MODULES returns a flat blob mixing preprocessor flags with linker
    # flags; split into CPPFLAGS/LDFLAGS/LIBS so AC_CHECK_HEADERS and AC_CHECK_LIB
    # can each receive only the subset they understand.
    for f in $HPSS_CFLAGS $HPSS_LIBS
    do
        case $f in
            -D*|-I*|-pthread|-fms-extensions|-m64)
                _HPSS_CPPFLAGS="${_HPSS_CPPFLAGS}$f "
                ;;
            -l*)
                _HPSS_LIBS="${_HPSS_LIBS}$f "
                ;;
            -L*|-Wl*)
                _HPSS_LDFLAGS="${_HPSS_LDFLAGS}$f "
                ;;
            *)
                echo Unexpected field $f, ignoring
                ;;
        esac
    done

    HPSS_CPPFLAGS=$_HPSS_CPPFLAGS
    HPSS_LDFLAGS=$_HPSS_LDFLAGS
    HPSS_LIBS=$_HPSS_LIBS
],
[
    HPSS_CPPFLAGS="-DLINUX -DLITTLEEND -I${HPSS_ROOT}/include"
    HPSS_LDFLAGS="-L${HPSS_ROOT}/lib -Wl,-rpath,${HPSS_ROOT}/lib"
    HPSS_LIBS="-lhpsskrb5auth -lhpssunixauth -lhpss -lhpsscs"
])

# AC_CHECK_HEADERS reads CPPFLAGS; inject HPSS flags temporarily then restore.
SAVE_CPPFLAGS="${CPPFLAGS}"
# RHEL 8.4 doesn't supply /usr/include/rpc/rpc.h so we need to pass through the
# tirpc module includes found above.
CPPFLAGS="${TIRPC_CFLAGS} ${HPSS_CPPFLAGS}"

# action-if-found is a no-op; HAVE_xxx macros are always written to config.h
# regardless of this slot.
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

AC_CHECK_HEADER([hpss_api_test.h],
    [AC_DEFINE([HPSS_API_TEST], [1],
               [Defined when building against a test environment instead of a production HPSS environment])],
    [])
AM_CONDITIONAL([HPSS_API_TEST], [test "x$ac_cv_header_hpss_api_test_h" = xyes])

CPPFLAGS="${SAVE_CPPFLAGS}"

# AC_CHECK_LIB reads LDFLAGS; inject HPSS flags temporarily then restore.
SAVE_LDFLAGS="${LDFLAGS}"
LDFLAGS="${HPSS_LDFLAGS}"
# Explicit no-op suppresses AC_CHECK_LIB's default of prepending -lhpss to
# LIBS; we manage HPSS_LIBS ourselves.
AC_CHECK_LIB(hpss,
             hpss_Lstat,
             [DONT_POLLUTE_LIBS=],
             [AC_MSG_ERROR(hpss-lib-devel is not installed)])
LDFLAGS="${SAVE_LDFLAGS}"


#
# Distro Detection
#
# Read /etc/os-release to build a distro tag that matches the tarball filename
# suffix (e.g. "fedora44", "rhel9"). Baked into the installed libraries so
# `strings libglobus_gridftp_server_hpss_real.so | grep HPSS_DSI_ORIGIN` identifies
# which tarball a given installation came from.
#
AC_MSG_CHECKING([OS distribution tag])
AS_IF([test -f /etc/os-release],
    [distro_tag=$(. /etc/os-release 2>/dev/null && printf '%s%s' "$ID" "$VERSION_ID")],
    [distro_tag=unknown])
# Mock builds incorporate the mock version so the library ident string and
# artifact names are fully self-describing: HPSS_DSI_ORIGIN will read
# "mock1.0.fedora44" instead of "fedora44", tarballs carry -mock1.0, and
# RPMs are named with -mock1.0.
AS_IF([test "x$ac_cv_header_hpss_api_test_h" = xyes],
    [mock_version=$(awk '/#define HPSS_MOCK_VERSION_STRING/ {gsub(/"/, "", $3); print $3}' \
                        "${HPSS_ROOT}/include/hpss_mock_version.h")
     distro_tag="mock${mock_version}.${distro_tag}"
     MOCK_SUFFIX="-mock${mock_version}"],
    [MOCK_SUFFIX=])
AC_DEFINE_UNQUOTED([DISTRO_TAG], ["$distro_tag"],
    [OS distribution tag (ID + VERSION_ID from /etc/os-release) for the library ident string])
AC_MSG_RESULT([$distro_tag])

#
# Output
#


AC_SUBST(MIN_GCSV5_GRIDFTP_VERSION)
AC_SUBST(MIN_GCSV5_VERSION)
AC_SUBST(HPSS_ROOT)
AC_SUBST(HPSS_CPPFLAGS)
AC_SUBST(HPSS_LDFLAGS)
AC_SUBST(HPSS_LIBS)
AC_SUBST(MOCK_SUFFIX)
# %%.*: strip first dot and everything after, leaving just the major number.
AC_SUBST([MAJOR_VERSION], [${PACKAGE_VERSION%%.*}])
# Two-step: PACKAGE_VERSION uses ~ for pre-release suffixes (e.g., 2.25~a3);
# #*. strips the major prefix, then %~* strips the pre-release suffix.
_MINOR_VERSION="${PACKAGE_VERSION#*.}"
AC_SUBST([MINOR_VERSION], [${_MINOR_VERSION%~*}])
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

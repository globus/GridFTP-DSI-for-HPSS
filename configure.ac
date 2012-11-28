dnl

AC_REVISION($Revision: 1.8 $)
AC_INIT([GridFTP HPSS DSI], [1.0 rc2])

AC_PROG_LIBTOOL

AM_INIT_AUTOMAKE

dnl
dnl Find Globus installation
dnl
globus_location="no"
AC_ARG_WITH(globus,
	[  --with-globus=PATH      Location of Globus Installation],
	[
		globus_location="$withval"
	]
)

if test "x$globus_location" = "xyes" ; then
	AC_MSG_ERROR(--with-globus=PATH must specify a path)
elif test "x$globus_location" = "xno" ; then
	if test "x$GLOBUS_LOCATION" = "x" ; then
		AC_MSG_ERROR(--with-globus=PATH must specify a path)
	else
		globus_location="$GLOBUS_LOCATION"
	fi
fi

if test ! -d "$globus_location" ; then
	AC_MSG_ERROR(--with-globus=PATH must specify a valid path)
fi
export GLOBUS_LOCATION=$globus_location
AC_SUBST(GLOBUS_LOCATION)

dnl
dnl Determine which flavor to use
dnl
globus_flavor="no"
AC_ARG_WITH(globus-flavor,
	[  --with-globus-flavor=TYPE       Describe Globus flavor type (ex: gcc32dbg)],
	[ globus_flavor="$withval" ]
)

if test "x$globus_flavor" = "xyes" ; then
	AC_MSG_ERROR(--with-globus-flavor=TYPE must specify a flavor)
fi
if test "x$globus_flavor" = "xno" ; then
	AC_MSG_ERROR(--with-globus-flavor=TYPE must specify a flavor)
fi
AC_SUBST(GLOBUS_FLAVOR_NAME, $globus_flavor)

#
# Find globus-makefile-header so we can build our cflags, ldflags, compiler choice, etc
#
AC_MSG_CHECKING(for globus-makefile-header)
if test -x $globus_location/bin/globus-makefile-header ; then
	globus_makefile_header="$globus_location/bin/globus-makefile-header"
else
	AC_MSG_ERROR(Cannot find globus-makefile-header: GPT installation is incomplete)
fi
AC_MSG_RESULT(yes)

dnl
dnl Run globus-makefile-header to get the build variables
dnl
${globus_makefile_header} --flavor=${globus_flavor} $linkflag globus_gssapi_gsi 2>/dev/null | sed 's/ = \(.*\)/="\1"/' > ./.globus_makefile_header.output
. ./.globus_makefile_header.output
rm -f ./.globus_makefile_header.output

AC_MSG_CHECKING(for Globus compiler choice)
if test -z "$GLOBUS_CC" ; then
	AC_MSG_ERROR(GLOBUS_CC missing)
fi
AC_MSG_RESULT(yes)
CC=$GLOBUS_CC
AC_PROG_CC()

AC_MSG_CHECKING(for GLOBUS_CFLAGS)
if test -z "$GLOBUS_CFLAGS" ; then
	AC_MSG_ERROR(GLOBUS_CFLAGS missing)
fi
AC_MSG_RESULT(yes)
AC_SUBST(GLOBUS_CFLAGS)

AC_MSG_CHECKING(for GLOBUS_CPPFLAGS)
if test -z "$GLOBUS_CPPFLAGS" ; then
	AC_MSG_ERROR(GLOBUS_CPPFLAGS missing)
fi
AC_MSG_RESULT(yes)
AC_SUBST(GLOBUS_CPPFLAGS)

AC_MSG_CHECKING(for GLOBUS_PKG_LIBS)
if test -z "$GLOBUS_PKG_LIBS" ; then
	AC_MSG_ERROR(GLOBUS_PKG_LIBS missing)
fi
AC_MSG_RESULT(yes)
AC_SUBST(GLOBUS_LIBS, $GLOBUS_PKG_LIBS)

AC_MSG_CHECKING(for GLOBUS_LDFLAGS)
if test -z "$GLOBUS_LDFLAGS" ; then
	AC_MSG_ERROR(GLOBUS_LDFLAGS missing)
fi
AC_MSG_RESULT(yes)
AC_SUBST(GLOBUS_LDFLAGS)


dnl
dnl Find the HPSS installation
dnl
hpssdir=/opt/hpss
AC_ARG_WITH(hpssdir, AC_HELP_STRING([--with-hpssdir=dir],\
   [Look for the HPSS library and headers in <dir>/lib and <dir>/include.  Default dir is /opt/hpss.]),
   [hpssdir=$withval], [])
AC_SUBST(HPSS_DIR, $hpssdir)

#hpsslibdir=$hpssdir
#AC_ARG_WITH(hpsslibdir, AC_HELP_STRING([--with-hpsslibdir=dir],\
#   [Look for the HPSS library in <dir>/lib/.  Default is the --with-hpssdir value.]),
#   [hpsslibdir=$withval], [])
#AC_SUBST(HPSS_LIB_DIR, $hpsslibdir)

#hpssincludedir=$hpssdir
#AC_ARG_WITH(hpssincludedir, AC_HELP_STRING([--with-hpssincludedir=dir],\
#   [Look for the HPSS headers in <dir>/include/.  Default is the --with-hpssdir value.]),
#   [hpssincludedir=$withval], [])
#AC_SUBST(HPSS_INCLUDE_DIR, $hpssincludedir)

rpathopt='-Wl,-rpath,$(HPSS_DIR)/lib'
AC_ARG_WITH(rpath, AC_HELP_STRING([--without-rpath],\
    [Build without rpath configured.]),
    [rpathopt=''], [])
AC_SUBST(RPATHOPT, $rpathopt)

#
# Note from Damian Hazen of LBL:
# The auth libraries are mechanism plugins and we shouldn't be linking
# against them directly.  I tried removing this, and instead just
# linking against libhpss but it doesn't work because the mechanisms need
# symbols defined in libhpssgss.so.  This is normally fine since
# libhpssgss.so is a dependency of libhpss.so which we link against.
# But, since the DSI is dlopen'd itself, and w/o the RTLD_GLOBAL flag
# to make the symbols globally visable, it doesn't work here.  The
# right fix I think is to be able to specify RTLD_GLOBAL to dlopen for
# the DSI plugin.  That's a change to globus, so I've just left for
# now.  If the ability to pass RTLD_GLOBAL when mapping in the DSI is
# added, then the --with-unixauth and --with-krb5auth options should be
# removed from configure, and the sec. plugins could function as
# intended.  You would then add a configuration option for
# authentication type, and make the following calls:
#
# authenticator_str = config_get_keytab_file(SessionHandle->ConfigHandle);
# authn_mech_str = config_get_authn_mech(SessionHandle->ConfigHandle);
#
# hpss_AuthnMechTypeFromString(authn_mech_str, &authn_mech);
# hpss_ParseAuthString(authenticator_str,
#                      &authn_mech,
#                      &authenticator_type,
#                      &authenticator);
#
# hpss_GetConfiguration(&api_config);
# api_config.AuthnMech = authn_mech;
#
# hpss_SetLoginCred(login_name,
#	            authn_mech,
#	            hpss_rpc_cred_client,
#	            authenticator_type,
#	            authenticator);
#
# This would let you change the authentication mechanism
# w/o a recompile -- woohoo!
#
#
# Also, the symbol issue is the same for the authz mechanisms.  For
# example, if a site is using kerberos for authentication and unix for
# authorization, then they need to link against both the kerberos and
# unix mechanisms (the unix sec library has both auth and authz
# interfaces).  Because of this, I've made linking against the unix
# library unconditional.  We should also add an option for sites using
# LDAP for authz.
#


hpssauthlibs=''
#AC_ARG_WITH(hpss-auth-libs, AC_HELP_STRING([--with-hpss-auth-libs],\
#    [Link against libhpsskrb5auth and libhpssunixauth.]),
#    [hpssauthlibs="$hpssauthlibs -lhpsskrb5auth -lhpssunixauth"], [])

AC_ARG_WITH(unixauth, 
    AC_HELP_STRING([--with-unixauth], [Link against libhpssunixauth.]),
    [
        hpssauthlibs="$hpssauthlibs -lhpssunixauth"
        AC_DEFINE(HPSS_UNIX_AUTH, 1, [Using Unix authentication])
    ], 
    [])

AC_ARG_WITH(krb5auth, 
    AC_HELP_STRING([--with-krb5auth], [Link against libhpsskrb5auth.]),
    [
        hpssauthlibs="$hpssauthlibs -lhpsskrb5auth -lhpssunixauth"
        AC_DEFINE(HPSS_KRB5_AUTH, 1, [Using Kerberos authentication])
    ],
    [])

if test "${hpssauthlibs+set}" != set; then
	echo "Must specifiy one of --with-unixauth or --with-krb5auth"
	exit
fi

AC_SUBST(HPSS_AUTH_LIBS, $hpssauthlibs)

#AC_SUBST(CFLAGS, "-D_LARGEFILE_SOURCE=1 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DLINUX")

dnl
dnl These values are hardcoded for now, until I can get the packaging stuff working again
dnl
DIRT_TIMESTAMP=`perl -e 'print time'`
DIRT_BRANCH_ID=99999

AC_CONFIG_HEADERS([config.h])

AC_SUBST(GPT_MAJOR_VERSION, 1)
AC_SUBST(GPT_MINOR_VERSION, 0)
AC_SUBST(DIRT_TIMESTAMP)
AC_SUBST(DIRT_BRANCH_ID)

prefix='$(GLOBUS_LOCATION)'
exec_prefix='$(GLOBUS_LOCATION)'

AC_OUTPUT(
    Makefile
    version.h
)

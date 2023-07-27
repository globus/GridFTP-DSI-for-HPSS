#ifdef GCSV5
/*******************************************************************************
 * This file provides functions used by Globus Connect Server v5. 
 *
 * The GCS Manager needs certain information that is only aviailable via the
 * HPSS C API. GCSM could call HPSS functions directly via ctypes but it would
 * run into the tirpc loader issue. So we'll save GCSM the trouble and provide
 * simpler interfaces here. Then GCSM only needs to load the HPSS module and
 * find the function to call. This should work out since the HPSS connector is
 * required for full functionality with HPSS storage gateways.
 *
 * Notes about the functions herein:
 *
 * - Try not to trigger any internal logging
 * - Avoid situations that might hang the query, for example, stale core server
 *   connections or bad usernames
 * - Watch out for deadlocked HPSS API cleanup functions
 * - Send back meaningful error messages to be logged by GCSM
 * - make sure requests are serialized
 * - don't leave connections established or background operations running
 * - we could cache results to make things a bit more efficient
 * - we could skip hpss_PurgeLoginCred() for better performance
 *
 * The code here largely borrows from other functions in the DSI but they are
 * modified to fit our constraints.
 ******************************************************************************/

/*
 * System includes
 */
#include <sys/types.h>
#include <pthread.h>
#include <stdarg.h>
#include <pwd.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>
#include <hpss_String.h>
#include <hpss_mech.h>

/*
 * Local includes
 */
#include "local_strings.h"

static int
login(const char *  AuthenticationMech,
      const char *  Authenticator,
      const char *  LoginName,
      const char *  UserName,
      const char ** ErroMsg);

static void logout();

/*******************************************************************************
 * get_home_directory()
 *
 *  GCSM needs the user's home directory for expanding path_restrictions when a
 * gridftp login occurs. Historically, the DSI gets the user's home directory
 * from the user credential via the call hpss_GetThreadUcred(). We can not just
 * simply parse passwd/group files because some sites use HPSS LDAP; this call
 * is the only way to get it in all cases.
 *
 * Background:
 * 
 *  HPSS supports 'unix' and 'krb5' for authentication. It supports 'unix' and
 * 'ldap' for authorization. The authentication portion is something we have
 * dealt with for years; the admin configures the DSI to use a specific
 * mechanism which tells the HPSS API how to send the keytab to HPSS. There are
 * even env vars which tell which method is being used (see authenticate.c).
 *
 * Authorization seems to be a little more illusive though. The admin configures
 * the node to use one or the other in /var/hpss/etc/authz.conf. There are no
 * environment variables or configuration options as far as I can tell that hint
 * to which mechanism is in use. This leaves us with all sorts of fun use cases:
 *
 * 1) 'unix' and HPSS_UNIX_USE_SYSTEM_COMMANDS=FALSE: passwd and group live in
 *    ${HPSS_UNIX_AUTH_PASSWD}, defaults to /var/hpss/etc/.
 *
 * 2) 'unix' and HPSS_UNIX_USE_SYSYTEM_COMMANDS=TRUE: passwd and group live in
 *    /etc/ (uses system files). This option is also used when the host OS
 *    uses nis or ldap although HPSS system accounts exist in /etc/passwd.
 *
 * 3) 'ldap': account information is provided through undocumented magic.
 *
 * Since the beginning of this project, we have required that the DTN provide
 * uid/gid translations at the sysytem level, regardless of authorization used,
 * so that gridftp can translate gfs_stat_t entries into username/group.
 * Although gridftp still performs that translation, nothing in the Globus
 * platform actually makes use of the  user or group names. Therefore it's all
 * for not. _But_, we take advantage of the uid/gid translations at system level
 * in order to translate the username to a uid in order to complete the login
 * process. Unfortunatley, that translation does not necessarily have the
 * correct home directory.
 ******************************************************************************/

static pthread_mutex_t _lock = PTHREAD_MUTEX_INITIALIZER;

static void
get_exclusive_lock()
{
    pthread_mutex_lock(&_lock);
}

static void
release_lock()
{
    pthread_mutex_unlock(&_lock);
}

//// This function is exported via the HPSS (loader) DSI

// returns home directory or NULL on error
int
get_home_directory(const char *  AuthenticationMech, // Location of credentials
                   const char *  Authenticator, // <type>:<path>
                   const char *  LoginName, // HPSS super user
                   const char *  UserName, // User to switch to
                   const char ** HomeDirectory, // Username's HPSS home directory
                   const char ** ErrorMsg) // Error message to log
{
    *HomeDirectory = NULL;
    *ErrorMsg = NULL;

    // Because the login code must be used sequentially. If this becomes a
    // bottleneck, consider caching results.
    get_exclusive_lock();

    int retval = login(AuthenticationMech, Authenticator, LoginName, UserName, ErrorMsg);
    if (retval != HPSS_E_NOERROR)
        goto cleanup;

    /*
     * Pulling the HPSS directory from the user's credential will support
     * sites that use HPSS LDAP.
     */
    sec_cred_t user_cred;
    retval = hpss_GetThreadUcred(&user_cred);
    if (retval != HPSS_E_NOERROR)
    {
        *ErrorMsg = "Can not get current HPSS user credential";
        goto cleanup;
    }
    *HomeDirectory = strdup(user_cred.Directory);

cleanup:
    // We must always logout
    logout();
    release_lock();
    return retval;
}

static int
login(const char *  AuthenticationMech,
      const char *  Authenticator,
      const char *  LoginName,
      const char *  UserName,
      const char ** ErrorMsg)
{
    *ErrorMsg = NULL;

    hpss_authn_mech_t authn_mech; // enum {krb5, unix }
    int retval = hpss_AuthnMechTypeFromString(AuthenticationMech, &authn_mech);
    if (retval != HPSS_E_NOERROR)
    {
        *ErrorMsg = "Failed to log into HPSS because the configured "
                    "authentication mechanism is not known to HPSS.";
        return retval;
    }

    hpss_rpc_auth_type_t auth_type; // enum {invalid, krb5, unix, gsi, spkm}
    char * authenticator;
    retval = hpss_ParseAuthString((char *)Authenticator,
                                  &authn_mech,
                                  &auth_type,
                                  (void **)&authenticator);
    if (retval != HPSS_E_NOERROR)
    {
        *ErrorMsg = "Failed to log into HPSS because the authenticator "
                    "could not be parsed.";
        return retval;
    }

    /* Now log into HPSS using our configured 'super user' */
    retval = hpss_SetLoginCred((char *)LoginName,
                               authn_mech,
                               hpss_rpc_cred_client,
                               auth_type,
                               authenticator);
    if (retval != HPSS_E_NOERROR)
    {
        *ErrorMsg = "Failed to log into HPSS";
        return retval;
    }

    /* Find the passwd entry. */
    struct passwd *passwd = NULL;
    struct passwd  passwd_buf;
    char           buffer[1024];
    retval = getpwnam_r(UserName, &passwd_buf, buffer, sizeof(buffer), &passwd);
    if (retval != 0)
    {
        *ErrorMsg = "A system error occurred while looking up user's HPSS "
                    "account with getpwname_r.";
        return errno;
    }

    if (passwd == NULL)
    {
        *ErrorMsg = "Unable to find the user's HPSS account with getpwnam()";
        return ENOENT;
    }

    /*
     * Now masquerade as this user. This will lookup uid in our realm and
     * set our credential to that user. The lookup is determined by the
     * /var/hpss/etc/auth.conf, authz.conf files.
     */
    retval = hpss_LoadDefaultThreadState(passwd->pw_uid, hpss_Umask(0), NULL);
    if (retval != HPSS_E_NOERROR)
    {
        *ErrorMsg = "Could not change active HPSS uid.";
        return retval;
    }

    return HPSS_E_NOERROR;
}

static void
logout()
{
    // Remove our superuser credential
    hpss_PurgeLoginCred();
    // Drop connections to the core server
    hpss_ClientAPIReset();
}
#endif /* GCSV5 */

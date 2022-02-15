/*
 * System includes.
 */
#include <sys/types.h>
#include <pwd.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * Local includes
 */
#include "authenticate.h"
#include "logging.h"
#include "hpss.h"

globus_result_t
authenticate_get_uid(char *UserName, int *Uid)
{
    struct passwd *passwd = NULL;
    struct passwd  passwd_buf;
    char           buffer[1024];
    int            retval = 0;

    /* Find the passwd entry. */
    retval = getpwnam_r(UserName, &passwd_buf, buffer, sizeof(buffer), &passwd);
    if (retval != 0)
        return GlobusGFSErrorSystemError("getpwnam_r", errno);

    if (passwd == NULL)
    {
        ERROR("Unable to look up the user's HPSS account with getpwnam()");
        return HPSSLoginDenied();
    }

    /* Copy out the uid */
    *Uid = passwd->pw_uid;

    return GLOBUS_SUCCESS;
}

globus_result_t
authenticate(char * LoginName, // User w/credentials. defaults to hpssftp
             char * AuthenticationMech, // Location of credentials
             char * Authenticator, // <type>:<path>
             char * UserName) // User to switch to
{
    hpss_authn_mech_t authn_mech; // enum {krb5, unix, gsi, spkm}

    char * authn_mech_string = AuthenticationMech;
    if (!authn_mech_string)
        authn_mech_string = Hpss_Getenv("HPSS_API_AUTHN_MECH");
    if (!authn_mech_string)
        authn_mech_string = Hpss_Getenv("HPSS_PRIMARY_AUTHN_MECH");

    int retval = Hpss_AuthnMechTypeFromString(authn_mech_string, &authn_mech);
    if (retval != HPSS_E_NOERROR)
    {
        ERROR("User could not log into HPSS because the configured "
              "authentication mechanism \"%s\" is not known to HPSS.",
              authn_mech_string);
        return HPSSConfigurationError();
    }

    char * authenticator_string = Authenticator;
    if (!authenticator_string)
        authenticator_string = Hpss_Getenv("HPSS_PRIMARY_AUTHENTICATOR");

    hpss_rpc_auth_type_t auth_type; // enum {invalid, krb5, unix, gsi, spkm}
    char * authenticator;
    retval = Hpss_ParseAuthString(authenticator_string,
                                  &authn_mech,
                                  &auth_type,
                                  (void **)&authenticator);

    if (!LoginName)
        LoginName = "hpssftp";

    /* Now log into HPSS using our configured 'super user' */
    retval = Hpss_SetLoginCred(LoginName,
                               authn_mech,
                               hpss_rpc_cred_client,
                               auth_type,
                               authenticator);
    if (retval != HPSS_E_NOERROR)
    {
        ERROR("Could not log into HPSS as \"%s\". %s",
              LoginName,
              hpss_ErrnoString(hpss_error_get(retval).returned_value));
        return HPSSConfigurationError();
    }

    if (UserName)
    {
        int uid = -1;
        globus_result_t result = authenticate_get_uid(UserName, &uid);
        if (result)
            return result;

        /*
         * Now masquerade as this user. This will lookup uid in our realm and
         * set our credential to that user. The lookup is determined by the
         * /var/hpss/etc/auth.conf, authz.conf files.
         */
        retval = Hpss_LoadDefaultThreadState(uid, Hpss_Umask(0), NULL);
        if (retval != HPSS_E_NOERROR)
        {
            ERROR("Could not change active HPSS UID to %d. %s",
              uid,
              hpss_ErrnoString(hpss_error_get(retval).returned_value));
            return HPSSLoginDenied();
        }
    }
    return GLOBUS_SUCCESS;
}

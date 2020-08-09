/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2015 NCSA.  All rights reserved.
 *
 * Developed by:
 *
 * Storage Enabling Technologies (SET)
 *
 * Nation Center for Supercomputing Applications (NCSA)
 *
 * http://www.ncsa.illinois.edu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the .Software.),
 * to deal with the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 *    + Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimers.
 *
 *    + Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimers in the
 *      documentation and/or other materials provided with the distribution.
 *
 *    + Neither the names of SET, NCSA
 *      nor the names of its contributors may be used to endorse or promote
 *      products derived from this Software without specific prior written
 *      permission.
 *
 * THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS WITH THE SOFTWARE.
 */

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
        return GlobusGFSErrorGeneric("Account not found");

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
        return GlobusGFSErrorSystemError("hpss_AuthnMechTypeFromString()",
                                         -retval);

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
        return GlobusGFSErrorSystemError("hpss_SetLoginCred()", -retval);

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
            return GlobusGFSErrorSystemError("hpss_LoadDefaultThreadState()",
                                             -retval);
    }

    return GLOBUS_SUCCESS;
}

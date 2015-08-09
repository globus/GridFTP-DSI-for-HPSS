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
 * HPSS includes
 */
#include <hpss_String.h>
#include <hpss_errno.h>
#include <hpss_mech.h>
#include <hpss_api.h>

/*
 * Local includes
 */
#include "authenticate.h"


globus_result_t
authenticate_get_uid(char * UserName, int * Uid)
{
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(authenticate_get_uid);

	/* Find the passwd entry. */
	retval = getpwnam_r(UserName,
	                    &passwd_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &passwd);
	if (retval != 0)
		return GlobusGFSErrorSystemError("getpwnam_r", errno);

	if (passwd == NULL)
		return GlobusGFSErrorGeneric("Account not found");

	/* Copy out the uid */
	*Uid = passwd->pw_uid;

	return GLOBUS_SUCCESS;
}

globus_result_t
authenticate(char * LoginName,
             char * AuthenticationMech,
             char * Authenticator,
             char * UserName)
{
	int                  uid           = -1;
	char               * authenticator = NULL;
	globus_result_t      result        = GLOBUS_SUCCESS;
	hpss_rpc_auth_type_t auth_type;
	api_config_t         api_config;

	GlobusGFSName(authenticate);

	/* Get the current HPSS client configuration. */
	int retval = hpss_GetConfiguration(&api_config);
	if (retval != HPSS_E_NOERROR)
		return GlobusGFSErrorSystemError("hpss_GetConfiguration", -retval);

	/* Translate the authentication mechanism. */
	retval = hpss_AuthnMechTypeFromString(AuthenticationMech, &api_config.AuthnMech);
	if (retval != HPSS_E_NOERROR)
		return GlobusGFSErrorSystemError("hpss_AuthnMechTypeFromString()", -retval);

	/* Parse the authenticator. */
	retval = hpss_ParseAuthString(Authenticator,
	                              &api_config.AuthnMech,
	                              &auth_type,
	                              (void **)&authenticator);
	if (retval != HPSS_E_NOERROR)
		return GlobusGFSErrorSystemError("hpss_ParseAuthString()", -retval);

	/* Now set the current HPSS client configuration. */
//	api_config.Flags  =  API_USE_CONFIG;
//	retval = hpss_SetConfiguration(&api_config);
//	if (retval != HPSS_E_NOERROR)
//		return GlobusGFSErrorSystemError("hpss_SetConfiguration()", -retval);

	/* Now log into HPSS using our configured 'super user' */
	retval = hpss_SetLoginCred(LoginName,
	                           api_config.AuthnMech,
	                           hpss_rpc_cred_client,
	                           auth_type,
	                           authenticator);
	if (retval != HPSS_E_NOERROR)
		return GlobusGFSErrorSystemError("hpss_SetLoginCred()", -retval);


	result = authenticate_get_uid(UserName, &uid);
	if (result) return result;

	/*
	 * Now masquerade as this user. This will lookup uid in our realm and
	 * set our credential to that user. The lookup is determined by the
	 * /var/hpss/etc/auth.conf, authz.conf files.
	 */
	retval = hpss_LoadDefaultThreadState(uid, hpss_Umask(0), NULL);
	if (retval != HPSS_E_NOERROR)
		return GlobusGFSErrorSystemError("hpss_LoadDefaultThreadState()", -retval);

    return GLOBUS_SUCCESS;
}

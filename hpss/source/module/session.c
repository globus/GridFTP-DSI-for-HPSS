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
 * System includes
 */
#include <stdlib.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>

/*
 * Local includes
 */
#include "authenticate.h"
#include "session.h"
#include "config.h"

globus_result_t
session_get_uid(char * UserName, int * Uid)
{
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(session_get_uid);

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
session_init(globus_gfs_session_info_t * GFSSessionInfo, session_t ** Session)
{
	globus_result_t result = GLOBUS_SUCCESS;
	int             uid    = 0;
	sec_cred_t      user_cred;

	GlobusGFSName(session_init);

	/* Allocate the session struct */
	*Session = globus_malloc(sizeof(session_t));
	if (!*Session)
		return GlobusGFSErrorMemory("session_t");
	memset(*Session, 0, sizeof(session_t));

	/* Initialize the configuration. */
	result = config_init(&(*Session)->Config);
	if (result != GLOBUS_SUCCESS)
		return result;

	/*
	 * Authenticate to HPSS.
	 */

	/* Get the uid of the user logging in. */
	result = session_get_uid(GFSSessionInfo->username, &uid);
	if (result != GLOBUS_SUCCESS)
		return result;

	/* Now authenticate. */
	result = authenticate((*Session)->Config->LoginName,
	                      (*Session)->Config->AuthenticationMech,
	                      (*Session)->Config->Authenticator,
	                       uid);
	if (result != GLOBUS_SUCCESS)
		return result;

	/*
	 * Find the user's home directory.
	 */

	/*
	 * Pulling the HPSS directory from the user's credential will support
	 * sites that use HPSS LDAP.
	 */
	result = hpss_GetThreadUcred(&user_cred);
	if (result != GLOBUS_SUCCESS)
		return result;

	/* Copy it out. */
	(*Session)->HomeDirectory = globus_libc_strdup(user_cred.Directory);
	if (!(*Session)->HomeDirectory)
		return GlobusGFSErrorMemory("home_directory");

	return GLOBUS_SUCCESS;
}

void
session_destroy(session_t * Session)
{
	if (Session)
	{
		config_destroy(Session->Config);
		if (Session->HomeDirectory)
			globus_free(Session->HomeDirectory);
		globus_free(Session);
	}
}


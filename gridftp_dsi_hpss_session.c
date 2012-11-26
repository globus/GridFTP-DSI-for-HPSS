/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2012 NCSA.  All rights reserved.
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
#include <stdlib.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_api.h>

/*
 * Local includes.
 */
#ifdef REMOTE
#include "gridftp_dsi_hpss_ipc_control.h"
#include "gridftp_dsi_hpss_ipc_data.h"
#endif /* REMOTE */
#include "gridftp_dsi_hpss_commands.h"
#include "gridftp_dsi_hpss_session.h"
#include "gridftp_dsi_hpss_config.h"
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_msg.h"
#include "config.h"

#ifdef DMALLOC
/*
 * Dmalloc
 */
#include <dmalloc.h>
#endif /* DMALLOC */

struct session_handle {
	globus_gfs_session_info_t   SessionInfo;
	globus_bool_t               Authenticated;

	/*
	 * Location for callers to store predefined objects.
	 */
	struct {
		void * Object;
	} ObjectCache[SESSION_CACHE_OBJECT_ID_MAX];

	/*
	 * Per session preferences.
	 */
	struct {
		int FamilyID;
		int CosID;
	} Pref;

	struct {
		char * HardLinkFrom;
	} Cmd;

	config_handle_t * ConfigHandle;
};

void
session_destroy_session_info(globus_gfs_session_info_t * SessionInfo)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (SessionInfo->username != NULL)
		globus_free(SessionInfo->username);
	if (SessionInfo->password != NULL)
		globus_free(SessionInfo->password);
	if (SessionInfo->subject != NULL)
		globus_free(SessionInfo->subject);
	if (SessionInfo->cookie != NULL)
		globus_free(SessionInfo->cookie);
	if (SessionInfo->host_id != NULL)
		globus_free(SessionInfo->host_id);

	SessionInfo->username = NULL;
	SessionInfo->password = NULL;
	SessionInfo->subject  = NULL;
	SessionInfo->cookie   = NULL;
	SessionInfo->host_id  = NULL;

	GlobusGFSHpssDebugExit();
}

static globus_result_t
session_copy_session_info(globus_gfs_session_info_t * Source,
                          globus_gfs_session_info_t * Destination)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	memset(Destination, 0, sizeof(globus_gfs_session_info_t));

	Destination->del_cred  = Source->del_cred;
	Destination->free_cred = Source->free_cred;
	Destination->map_user  = Source->map_user;

	if (Source->username != NULL)
	{
		Destination->username = globus_libc_strdup(Source->username);
		if (Destination->username == NULL)
		{
			result = GlobusGFSErrorMemory("session info");
			goto cleanup;
		}
	}

	if (Source->password != NULL)
	{
		Destination->password = globus_libc_strdup(Source->password);
		if (Destination->password == NULL)
		{
			result = GlobusGFSErrorMemory("session info");
			goto cleanup;
		}
	}

	if (Source->subject != NULL)
	{
		Destination->subject = globus_libc_strdup(Source->subject);
		if (Destination->subject == NULL)
		{
			result = GlobusGFSErrorMemory("session info");
			goto cleanup;
		}
	}

	if (Source->cookie != NULL)
	{
		Destination->cookie = globus_libc_strdup(Source->cookie);
		if (Destination->cookie == NULL)
		{
			result = GlobusGFSErrorMemory("session info");
			goto cleanup;
		}
	}

	if (Source->host_id != NULL)
	{
		Destination->host_id = globus_libc_strdup(Source->host_id);
		if (Destination->host_id == NULL)
		{
			result = GlobusGFSErrorMemory("session info");
			goto cleanup;
		}
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		session_destroy_session_info(Destination);
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}



/*
 * Authenticate to HPSS.
 */
static globus_result_t
session_auth_to_hpss(session_handle_t * SessionHandle)
{
	int                 uid         = -1;
	int                 retval      = 0;
	char              * login_name  = NULL;
	char              * keytab_file = NULL;
	globus_result_t     result      = GLOBUS_SUCCESS;
	api_config_t        api_config;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Get the login name of the priviledged user. */
	login_name = config_get_login_name(SessionHandle->ConfigHandle);
	if (login_name == NULL)
	{
		result = GlobusGFSErrorGeneric("LoginName missing from config file");
		goto cleanup;
	}

	/* Get the keytab file to use for authentication. */
	keytab_file = config_get_keytab_file(SessionHandle->ConfigHandle);
	if (keytab_file == NULL)
	{
		result = GlobusGFSErrorGeneric("Keytab missing from config file");
		goto cleanup;
	}

	/*
	 * Get the current HPSS client configuration.
	 */
	retval = hpss_GetConfiguration(&api_config);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_GetConfiguration", -retval);
		goto cleanup;
	}

	/* Indicate that we are doing unix authentication. */
	api_config.Flags     =  API_USE_CONFIG;
#ifdef HPSS_UNIX_AUTH
	api_config.AuthnMech =  hpss_authn_mech_unix;
#elif defined HPSS_KRB5_AUTH
	api_config.AuthnMech =  hpss_authn_mech_krb5;
#else
#error MUST BE CONFIGURED TO USE KRB5 OR UNIX AUTHENTICATION
#endif

	/* Now set the current HPSS client configuration. */
	retval = hpss_SetConfiguration(&api_config);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_SetConfiguration", -retval);
		goto cleanup;
	}

	/* Now log into HPSS using our configured 'super user' */
	retval = hpss_SetLoginCred(login_name,
	                           hpss_authn_mech_unix, /* XXX detect this */
	                           hpss_rpc_cred_client,
	                           hpss_rpc_auth_type_keytab,
	                           keytab_file);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_SetLoginCred", -retval);
		goto cleanup;
	}

	/*
	 * Now we need to masquerade as this user on our HPSS connection.
	 */

	/* Get the user's UID. */
	result = misc_username_to_uid(SessionHandle->SessionInfo.username, &uid);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Now masquerade as this user. This will lookup uid in our realm and
	 * set our credential to that user. The lookup is determined by the
	 * /var/hpss/etc/auth.conf, authz.conf files.
	 */
	retval = hpss_LoadDefaultThreadState(uid, 0022, NULL);
	if(retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_LoadDefaultThreadState", -retval);
		goto cleanup;
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * This is the basic (minimal) initialization, used at the start of
 * all sessions.
 */
globus_result_t
session_init(globus_gfs_session_info_t *  SessionInfo,
             session_handle_t          ** SessionHandle)
{
	globus_result_t result = GLOBUS_SUCCESS;

    GlobusGFSName(__func__);
    GlobusGFSHpssDebugEnter();

	/* Allocate our session handle. */
	*SessionHandle = (session_handle_t *) globus_calloc(1, sizeof(session_handle_t));
	if (*SessionHandle == NULL)
	{
		result = GlobusGFSErrorMemory("session handle");
		goto cleanup;
	}

	/*
	 * Copy out the session info.
	 */
	result = session_copy_session_info(SessionInfo, &(*SessionHandle)->SessionInfo);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Indicate that no authentication has occurred. */
	(*SessionHandle)->Authenticated = GLOBUS_FALSE;

	/*
	 * Initialize the preferences.
	 */
	(*SessionHandle)->Pref.CosID    = SESSION_NO_COS_ID;
	(*SessionHandle)->Pref.FamilyID = SESSION_NO_FAMILY_ID;

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		/* Destroy the session handle. */
		session_destroy(*SessionHandle);
		/* Release our handle */
		*SessionHandle = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * This is called after session_init() when a session needs to authenticate
 * to HPSS.
 */
globus_result_t
session_authenticate(session_handle_t * SessionHandle)
{
	globus_result_t result = GLOBUS_SUCCESS;

    GlobusGFSName(__func__);
    GlobusGFSHpssDebugEnter();

	/* Check if we have already authenticated. */
	if (SessionHandle->Authenticated == GLOBUS_TRUE)
		goto cleanup;

	/* Parse the config files. */
	result = config_init(NULL, &SessionHandle->ConfigHandle);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("config_init", result);
		goto cleanup;
	}

	/* Authenticate to HPSS. */
	result = session_auth_to_hpss(SessionHandle);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Attempt to authenticate to HPSS", result);
		goto cleanup;
	}

	/* Indicate that we have authenticated. */
	SessionHandle->Authenticated = GLOBUS_TRUE;

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

void
session_destroy(session_handle_t * Session)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Session != NULL)
	{
		/* Destroy the session info. */
		session_destroy_session_info(&Session->SessionInfo);

		/* Destroy the command data. */
		if (Session->Cmd.HardLinkFrom != NULL)
			globus_free(Session->Cmd.HardLinkFrom);

		/* Destroy the config handle. */
		config_destroy(Session->ConfigHandle);

		/* Destroy the session handle. */
		globus_free(Session);
	}

	GlobusGFSHpssDebugExit();
}

/*
 * A copy of the pointer to our session_info is returned;
 * nothing is dup'ed.
 */
globus_gfs_session_info_t *
session_get_session_info(session_handle_t * SessionHandle)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();

	return &SessionHandle->SessionInfo;
}

/*
 * Returned string is not a dup, original is sent.
 */
char *
session_get_username(session_handle_t * SessionHandle)
{
	char * user_name = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/*
	 * Make sure we are called in a context in which we have a username.
	 */
	globus_assert(SessionHandle->SessionInfo.username != NULL);

	/* Save the username. */
	user_name = SessionHandle->SessionInfo.username;

	GlobusGFSHpssDebugExit();

	return user_name;
}

void
session_pref_set_family_id(session_handle_t * SessionHandle,
                           int                FamilyID)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Save the family id. */
	SessionHandle->Pref.FamilyID = FamilyID;

	GlobusGFSHpssDebugExit();
}

int
session_pref_get_family_id(session_handle_t * SessionHandle)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();

	return SessionHandle->Pref.FamilyID;
}

void
session_pref_set_cos_id(session_handle_t * SessionHandle,
                        int                CosID)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Save the cos id. */
	SessionHandle->Pref.CosID = CosID;

	GlobusGFSHpssDebugExit();
}

int
session_pref_get_cos_id(session_handle_t * SessionHandle)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();
	return SessionHandle->Pref.CosID;
}

/*
 * HardLinkFrom string is dup'ed. Previous value is free'd. 
 */
globus_result_t
session_cmd_set_hardlinkfrom(session_handle_t * SessionHandle,
                             char             * HardLinkFrom)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (SessionHandle->Cmd.HardLinkFrom != NULL)
		globus_free(SessionHandle->Cmd.HardLinkFrom);

	/* Save the hardlinkfrom value. */
	SessionHandle->Cmd.HardLinkFrom = globus_libc_strdup(HardLinkFrom);
	if (SessionHandle->Cmd.HardLinkFrom == NULL)
		result = GlobusGFSErrorMemory("HardLinkFrom");

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExit();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * Returned string is not a dup, original is sent.
 */
char *
session_cmd_get_hardlinkfrom(session_handle_t * SessionHandle)

{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();

	return SessionHandle->Cmd.HardLinkFrom;
}

/*
 * Call free on the value and set it to NULL.
 */
void
session_cmd_free_hardlinkfrom(session_handle_t * SessionHandle)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (SessionHandle->Cmd.HardLinkFrom != NULL)
		globus_free(SessionHandle->Cmd.HardLinkFrom);
	SessionHandle->Cmd.HardLinkFrom = NULL;

	GlobusGFSHpssDebugExit();
}

/*
 * Translates Cos to the id. Cos can be the name or the id.
 * Returns SESSION_NO_COS_ID if cos is not found.
 */
int
session_get_cos_id(session_handle_t * SessionHandle, char * Cos)
{
	int    cos_id   = SESSION_NO_COS_ID;
	char * cos_name = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Try to get it by name. */
	cos_id = config_get_cos_id(SessionHandle->ConfigHandle, Cos);
	if (cos_id != CONFIG_NO_COS_ID)
		goto cleanup;

	/* Convert the id. */
	cos_id = atoi(Cos);

	/* Lookup the name to be sure it exists. */
	cos_name = config_get_cos_name(SessionHandle->ConfigHandle, cos_id);
	if (cos_name == NULL)
		cos_id = SESSION_NO_COS_ID;

cleanup:
	GlobusGFSHpssDebugExit();

	return cos_id;
}

globus_bool_t
session_can_user_use_cos(session_handle_t * SessionHandle, int CosID)
{
	globus_bool_t result = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* See if it is configured that way. */
	result = config_user_use_cos(SessionHandle->ConfigHandle,
	                             SessionHandle->SessionInfo.username,
	                             CosID);
	if (result == GLOBUS_TRUE)
		goto cleanup;

	/* See if the user is an admin. */
	result = config_user_is_admin(SessionHandle->ConfigHandle,
	                              SessionHandle->SessionInfo.username);

cleanup:
	GlobusGFSHpssDebugExit();

	return result;
}

/*
 * CosList must be free'd by the caller.
 */
globus_result_t
session_get_user_cos_list(session_handle_t *  SessionHandle,
                          char             ** CosList)
{
	int                index     = 0;
	int                length    = 0;
	char            ** cos_array = NULL;
	globus_result_t    result    = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the returned list. */
	*CosList = NULL;

	result = config_get_user_cos_list(SessionHandle->ConfigHandle,
	                                  SessionHandle->SessionInfo.username,
	                                  &cos_array);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	if (cos_array != NULL)
	{
		/* Calculate the length of the returned string. */
		for (index = 0; cos_array[index] != NULL; index++)
		{
			length += strlen(cos_array[index]) + 1;
		}

		/* Allocate the string. */
		*CosList = (char *) globus_malloc(length + 1);
		if (*CosList == NULL)
		{
			result = GlobusGFSErrorMemory("cos list");
			goto cleanup;
		}

		/* Null terminate. */
		*CosList[0] = '\0';

		/* Create the lst. */
		for (index = 0; cos_array[index] != NULL; index++)
		{
			if (index != 0)
				strcat(*CosList, ",");
			strcat(*CosList, cos_array[index]);
		}
	}

cleanup:
	/* Free the COS array. */
	if (cos_array != NULL)
	{
		for (index = 0; cos_array[index] != NULL; index++)
		{
			globus_free(cos_array[index]);
		}
		globus_free(cos_array);
	}

	if (result != GLOBUS_SUCCESS)
	{
		/* Release the cos list. */
		if (*CosList != NULL)
			globus_free(*CosList);
		*CosList = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * Translates Family to the id. Family can be the name or the id.
 * Returns SESSION_NO_FAMILY_ID if Family is not found.
 */
int
session_get_family_id(session_handle_t * SessionHandle, char * Family)
{
	int    family_id   = SESSION_NO_FAMILY_ID;
	char * family_name = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Try to get it by name. */
	family_id = config_get_family_id(SessionHandle->ConfigHandle, Family);
	if (family_id != CONFIG_NO_FAMILY_ID)
		goto cleanup;

	/* Convert the id. */
	family_id = atoi(Family);

	/* Lookup the name to be sure it exists. */
	family_name = config_get_family_name(SessionHandle->ConfigHandle, family_id);
	if (family_name == NULL)
		family_id = SESSION_NO_COS_ID;

cleanup:
	GlobusGFSHpssDebugExit();

	return family_id;
}

globus_bool_t
session_can_user_use_family(session_handle_t * SessionHandle, int FamilyID)
{
	globus_bool_t result = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* See if it is configured that way. */
	result = config_user_use_family(SessionHandle->ConfigHandle,
	                                SessionHandle->SessionInfo.username,
	                                FamilyID);
	if (result == GLOBUS_TRUE)
		goto cleanup;

	/* See if the user is an admin. */
	result = config_user_is_admin(SessionHandle->ConfigHandle,
	                              SessionHandle->SessionInfo.username);

cleanup:
	GlobusGFSHpssDebugExit();

	return result;
}

/*
 * *FamilyList must be free'd by the caller.
 */
globus_result_t
session_get_user_family_list(session_handle_t *  SessionHandle,
                             char             ** FamilyList)
{
	int                index        = 0;
	int                length       = 0;
	char            ** family_array = NULL;
	globus_result_t    result       = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the returned list. */
	*FamilyList = NULL;

	result = config_get_user_family_list(SessionHandle->ConfigHandle,
	                                     SessionHandle->SessionInfo.username,
	                                     &family_array);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	if (family_array != NULL)
	{
		/* Calculate the length of the returned string. */
		for (index = 0; family_array[index] != NULL; index++)
		{
			length += strlen(family_array[index]) + 1;
		}

		/* Allocate the string. */
		*FamilyList = (char *) globus_malloc(length + 1);
		if (*FamilyList == NULL)
		{
			result = GlobusGFSErrorMemory("family list");
			goto cleanup;
		}

		/* Null terminate. */
		*FamilyList[0] = '\0';

		/* Create the lst. */
		for (index = 0; family_array[index] != NULL; index++)
		{
			if (index != 0)
				strcat(*FamilyList, ",");
			strcat(*FamilyList, family_array[index]);
		}
	}

cleanup:
	/* Free the Family array. */
	if (family_array != NULL)
	{
		for (index = 0; family_array[index] != NULL; index++)
		{
			globus_free(family_array[index]);
		}
		globus_free(family_array);
	}

	if (result != GLOBUS_SUCCESS)
	{
		/* Release the family list. */
		if (*FamilyList != NULL)
			globus_free(*FamilyList);
		*FamilyList = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

void
session_cache_insert_object(session_handle_t          *  SessionHandle,
                            session_cache_object_id_t    ObjectID,
                            void                      *  Object)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Make sure there are no collisions. */
	globus_assert(SessionHandle->ObjectCache[ObjectID].Object == NULL);

	/* Insert this object. */
	SessionHandle->ObjectCache[ObjectID].Object = Object;

	GlobusGFSHpssDebugExit();
}

void *
session_cache_lookup_object(session_handle_t          * SessionHandle,
                            session_cache_object_id_t   ObjectID)
{
	void * object = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Get the object. */
	object = SessionHandle->ObjectCache[ObjectID].Object;

	GlobusGFSHpssDebugExit();

	return object;
}

void *
session_cache_remove_object(session_handle_t          * SessionHandle,
                            session_cache_object_id_t   ObjectID)
{
	void * object = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Get the object. */
	object = SessionHandle->ObjectCache[ObjectID].Object;

	/* Remove it from the cache. */
	SessionHandle->ObjectCache[ObjectID].Object = NULL;

	GlobusGFSHpssDebugExit();
	return object;
}

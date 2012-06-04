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
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_api.h>
#include <u_signed64.h>

/*
 * Local includes.
 */
#include "version.h"
#include "globus_gridftp_server_hpss_common.h"
#include "globus_gridftp_server_hpss_config.h"

typedef struct buffer {
	char          * Data;
	globus_size_t   DataLength;
	globus_off_t    TransferOffset;
	struct buffer * Next;
	struct buffer * Prev;
} buffer_t;

typedef struct {
	globus_size_t      BufferLength;
	globus_bool_t      Eof;

	/* This lock controls the items below it. */
	globus_mutex_t     Lock;
	int                FullBufferCount;
	buffer_t         * FullBufferChain;
	int                EmptyBufferCount;
	buffer_t         * EmptyBufferChain;

	/* Indicate that someone is waiting on a specific offset. */
	struct {
		globus_cond_t Cond;
		globus_bool_t Valid;
		globus_off_t  TransferOffset;
	} Waiter;
} buffer_handle_t;

typedef struct pio_request {
	hpss_pio_operation_t PioOperation;
	globus_off_t         FileLength;
	globus_off_t         FileOffset;
	buffer_handle_t    * BufferHandle;
	globus_result_t      Result;

	struct {
		int            FileFD;
		hpss_pio_grp_t StripeGroup;
	} Coordinator;
	struct {
		hpss_pio_grp_t StripeGroup;
	} Participant;

	/* Lock controls everything below it. */
	globus_mutex_t Lock;
	globus_cond_t  Cond;
	globus_bool_t  CoordHasExitted;
	globus_bool_t  ParticHasExitted;
} pio_request_t;

typedef struct gridftp_request {
	buffer_handle_t * BufferHandle;

	/* This lock controls access to the variables below it. */
	globus_mutex_t  Lock;
	globus_cond_t   Cond;
	globus_bool_t   Eof;
	globus_result_t Result;
	globus_off_t    TransferOffset; /* For STOR operations. */
	int             OpCount; /* Number of outstanding read/writes */

} gridftp_request_t;

/*
 * This is used to define the debug print statements.
 */
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);

/* 
 * Authenticate to HPSS.
 */
static globus_result_t
globus_i_gfs_hpss_module_auth_to_hpss(
    globus_gfs_operation_t   Operation,
	char                   * UserName)
{
	int                 uid         = -1;
	int                 retval      = 0;
	char              * login_name  = NULL;
	char              * keytab_file = NULL;
	globus_result_t     result      = GLOBUS_SUCCESS;
	api_config_t        api_config;

	GlobusGFSName(globus_i_gfs_hpss_module_auth_to_hpss);
	GlobusGFSHpssDebugEnter();

	/* Get the login name of the priviledged user. */
	login_name  = globus_l_gfs_hpss_config_get_login_name();

	/* Get the keytab file to use for authentication. */
	keytab_file = globus_l_gfs_hpss_config_get_keytab();

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
/* XXX detect this */
	api_config.AuthnMech =  hpss_authn_mech_unix;

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
	result = globus_l_gfs_hpss_common_username_to_uid(UserName, &uid);
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
	if (login_name != NULL)
		free(login_name);
	if (keytab_file != NULL)
		free(keytab_file);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return result;
}

/*
 * Called at the start of a new session (control connection) just after
 * GSI authentication. Authenticate to HPSS.
 */
static void
globus_l_gfs_hpss_module_session_start(
    globus_gfs_operation_t      Operation,
    globus_gfs_session_info_t * SessionInfo)
{
	char              * home_directory = NULL;
    globus_result_t     result         = GLOBUS_SUCCESS;
    globus_gfs_stat_t * gfs_stat_array = NULL;
	int                 gfs_stat_count = 0;

    GlobusGFSName(globus_l_gfs_hpss_module_session_start);
    GlobusGFSHpssDebugEnter();
 
	/* Get the command line / config file configuration. */
	result = globus_l_gfs_hpss_config_init(NULL);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("globus_l_gfs_hpss_config_init", result);
		goto cleanup;
	}

	/* Authenticate to HPSS. */
	result = globus_i_gfs_hpss_module_auth_to_hpss(Operation, SessionInfo->username);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Attempt to authentication to HPSS", result);
		goto cleanup;
	}

	/*
	 * Get the user's home directory. We need this for the call to
	 * globus_gridftp_server_finished_session_start(). If we are running as
	 * root and we leave that value as NULL, then the server will look it up.
	 * If we run as an unprivileged user and we don't specify the home
	 * directory, the server will leave us in the home directory of the
	 * unprivileged user.
	 */
	result = globus_l_gfs_hpss_common_username_to_home(SessionInfo->username,
	                                                   &home_directory);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Let's make sure the home directory exists so we do not drop them into
	 * oblivion.
	 */
	result = globus_l_gfs_hpss_common_gfs_stat(home_directory,
	                                           GLOBUS_TRUE,  /* FileOnly        */
	                                           GLOBUS_FALSE, /* UseSymlinkInfo  */
	                                           GLOBUS_TRUE,  /* IncludePathStat */
	                                           &gfs_stat_array,
	                                           &gfs_stat_count);
	if (result != GLOBUS_SUCCESS)
	{
		/* Make the error message a little more obvious. */
		result = GlobusGFSErrorWrapFailed("Attempt to find home directory", result);

		goto cleanup;
	}

	/* It exists, that's good enough for us. */
	globus_l_gfs_hpss_common_destroy_gfs_stat_array(gfs_stat_array, gfs_stat_count);

	/*
	 * Inform the server that we are done. If we do not pass in a username, the
	 * server will use the name we mapped to with GSI. If we do not pass in a
	 * home directory, the server will (1) look it up if we are root or
	 * (2) leave it as the unprivileged user's home directory.
	 *
	 * As far as I can tell, the server keeps a pointer to home_directory and frees
	 * it when it is done.
	 */
	globus_gridftp_server_finished_session_start(Operation,
	                                             result,
	                                             NULL, /* session_handle, */
	                                             NULL,
	                                             home_directory);

	GlobusGFSHpssDebugExit();
	return;

cleanup:

	/* Inform the server that we have completed with an error. */
	globus_gridftp_server_finished_session_start(Operation,
	                                             result,
	                                             NULL,
	                                             NULL,
	                                             NULL);

	/* Debug output w/ error. */
	GlobusGFSHpssDebugExitWithError();
}

static void
globus_l_gfs_hpss_module_session_end(void * Arg)
{
	GlobusGFSName(globus_l_gfs_hpss_module_session_end);
	GlobusGFSHpssDebugEnter();

	/*
	 * Make sure something is passed back. If an error occurs soon enough in
	 * the connection (like authentication), session end may be called
	 * even though session start wasn't called.
	 */
	if (Arg == NULL)
		return;

	GlobusGFSHpssDebugExit();
}

#define GSU_MAX_USERNAME_LENGTH 256

static void
globus_l_gfs_hpss_module_list_single_line(
    hpss_stat_t * HpssStat,
	char        * FullPath,
    char        * Buffer,
    int           BufferLength)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	char              archive[3];
	globus_bool_t     archived  = GLOBUS_FALSE;
	char            * name      = NULL;
	char            * username  = NULL;
	char            * groupname = NULL;
	char              user[GSU_MAX_USERNAME_LENGTH];
	char              grp[GSU_MAX_USERNAME_LENGTH];
	struct tm       * tm;
	char              perms[11];
	time_t            mtime;
	char            * month_lookup[12] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
	                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

	GlobusGFSName(globus_l_gfs_hpss_module_list_single_line);
	GlobusGFSHpssDebugEnter();

	strcpy(perms, "----------");

	/* Grab the local time. */
	mtime = HpssStat->hpss_st_mtime;
	tm = localtime(&mtime);

	/* Ignore the return values for now. */
	result = globus_l_gfs_hpss_common_uid_to_username(HpssStat->st_uid, &username);
	globus_l_gfs_hpss_common_destroy_result(result);

	result = globus_l_gfs_hpss_common_gid_to_groupname(HpssStat->st_gid, &groupname);
	globus_l_gfs_hpss_common_destroy_result(result);

	if(S_ISDIR(HpssStat->st_mode))
	{
		perms[0] = 'd';
	}
	else if(S_ISLNK(HpssStat->st_mode))
	{
		perms[0] = 'l';
	}
	else if(S_ISFIFO(HpssStat->st_mode))
	{
		perms[0] = 'p';
	}
	else if(S_ISCHR(HpssStat->st_mode))
	{
		perms[0] = 'c';
	}
	else if(S_ISBLK(HpssStat->st_mode))
	{
		perms[0] = 'b';
	}

	if(S_IRUSR & HpssStat->st_mode)
	{
		perms[1] = 'r';
	}
	if(S_IWUSR & HpssStat->st_mode)
	{
		perms[2] = 'w';
	}
	if(S_IXUSR & HpssStat->st_mode)
	{
		perms[3] = 'x';
	}
	if(S_IRGRP & HpssStat->st_mode)
	{
		perms[4] = 'r';
	}
	if(S_IWGRP & HpssStat->st_mode)
	{
		perms[5] = 'w';
	}
	if(S_IXGRP & HpssStat->st_mode)
	{
		perms[6] = 'x';
	}
	if(S_IROTH & HpssStat->st_mode)
	{
		perms[7] = 'r';
	}
	if(S_IWOTH & HpssStat->st_mode)
	{
		perms[8] = 'w';
	}
	if(S_IXOTH & HpssStat->st_mode)
	{
		perms[9] = 'x';
	}

	if (username != NULL)
	{
		/* Field width of 8 and max 8 characters. */
		snprintf(user, sizeof(user), "%8.8s", username);
	} else
	{
		/* Field width of 8 with space padding. */
		snprintf(user, sizeof(user), "%8d", HpssStat->st_uid);
	}

	if (groupname != NULL)
	{
		/* Field width of 8 and max 8 characters. */
		snprintf(grp, sizeof(grp), "%8.8s", groupname);
	} else
	{
		/* Field width of 8 with space padding. */
		snprintf(grp, sizeof(grp), "%8d", HpssStat->st_gid);
	}

	name = strrchr(FullPath, '/');
	if (name == NULL)
		name = FullPath;
	else
		name++;

	/* Default archive status. */
	snprintf(archive, sizeof(archive), "%s", "DK");

	/* If this is a regular file... */
	if (S_ISREG(HpssStat->st_mode))
	{
		/* Determine if it is archived. */
		result = globus_l_gfs_hpss_common_file_archived(FullPath, &archived);
		if (result != GLOBUS_SUCCESS)
		{
			snprintf(archive, sizeof(archive), "%s", "??");
			globus_l_gfs_hpss_common_destroy_result(result);
		} else if (archived == GLOBUS_TRUE)
		{
			snprintf(archive, sizeof(archive), "%s", "AR");
		} else
		{
			snprintf(archive, sizeof(archive), "%s", "DK");
		}
	}

	snprintf(Buffer, 
	         BufferLength,
	         "%s %3d %s %s %s %12"GLOBUS_OFF_T_FORMAT" %s %2d %02d:%02d %s\n",
	         perms,
	         HpssStat->st_nlink,
	         user,
	         grp,
	         archive,
	         HpssStat->st_size,
	         month_lookup[tm->tm_mon],
	         tm->tm_mday,
	         tm->tm_hour,
	         tm->tm_min,
	         name);

	if (username != NULL)
		globus_free(username);
	if (groupname != NULL)
		globus_free(groupname);

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_module_init_buffer_handle(globus_gfs_operation_t   Operation,
                                            buffer_handle_t        * BufferHandle)
{
	GlobusGFSName(globus_i_gfs_hpss_module_init_buffer_handle);
	GlobusGFSHpssDebugEnter();

	memset(BufferHandle, 0, sizeof(buffer_handle_t));

	/* Determine the GridFTP blocksize (used as the server's buffer size). */
	globus_gridftp_server_get_block_size(Operation, 
	                                    &BufferHandle->BufferLength);

	globus_mutex_init(&BufferHandle->Lock, NULL);
	globus_cond_init(&BufferHandle->Waiter.Cond, NULL);

	GlobusGFSHpssDebugExit();
}

static globus_result_t
globus_i_gfs_hpss_module_allocate_buffer(buffer_handle_t *  BufferHandle,
                                         char            ** Buffer)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(globus_i_gfs_hpss_module_allocate_buffer);
	GlobusGFSHpssDebugEnter();

	/* Allocate the buffer. */
	*Buffer = (char *) globus_malloc(BufferHandle->BufferLength);
	if (*Buffer == NULL)
		result = GlobusGFSErrorMemory("buffer");

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static void
globus_i_gfs_hpss_module_destroy_buffer_handle(buffer_handle_t * BufferHandle)
{
	buffer_t * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_destroy_buffer_handle);
	GlobusGFSHpssDebugEnter();

	/* Free the full chain. */
	while ((buffer_s = BufferHandle->FullBufferChain) != NULL)
	{
		BufferHandle->FullBufferChain = buffer_s->Next;
		globus_free(buffer_s->Data);
		globus_free(buffer_s);
	}

	/* Free the empty chain. */
	while ((buffer_s = BufferHandle->EmptyBufferChain) != NULL)
	{
		BufferHandle->EmptyBufferChain = buffer_s->Next;
		globus_free(buffer_s->Data);
		globus_free(buffer_s);
	}

	globus_mutex_destroy(&BufferHandle->Lock);
	globus_cond_destroy(&BufferHandle->Waiter.Cond);

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_module_buffer_handle_set_eof(buffer_handle_t * BufferHandle)
{
	GlobusGFSName(globus_i_gfs_hpss_module_buffer_handle_set_eof);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		/* Indicate that we have reached EOF. */
		BufferHandle->Eof = TRUE;

		/* Wake the waiter if there is one. */
		if (BufferHandle->Waiter.Valid == GLOBUS_TRUE)
			globus_cond_signal(&BufferHandle->Waiter.Cond);
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_module_release_empty_buffer(buffer_handle_t * BufferHandle,
                                              void            * Buffer)
{
	buffer_t * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_release_empty_buffer);
	GlobusGFSHpssDebugEnter();

	/* Safety check in case we are called with a NULL buffer. */
	if (Buffer == NULL)
		goto cleanup;

	/*
	 * Allocate the buffer list entry.
	 */
	buffer_s = (buffer_t *) globus_calloc(1, sizeof(buffer_t));
	if (buffer_s == NULL)
	{
		/* Memory failure. Deallocate it instead. */
		globus_free(Buffer);
		goto cleanup;
	}

	/* Now put it on the list. */
	globus_mutex_lock(&BufferHandle->Lock);
	{
		/* Save the buffer. */
		buffer_s->Data = Buffer;

		/* Put it on the empty list. */
		buffer_s->Next = BufferHandle->EmptyBufferChain;
		if (BufferHandle->EmptyBufferChain != NULL)
			BufferHandle->EmptyBufferChain->Prev = buffer_s;
		BufferHandle->EmptyBufferChain = buffer_s;

		/* Increment the empty buffer count. */
		BufferHandle->EmptyBufferCount++;
	}
	globus_mutex_unlock(&BufferHandle->Lock);
	
cleanup:

	GlobusGFSHpssDebugExit();
	return;
}

static int
globus_i_gfs_hpss_module_get_emtpy_buffer(buffer_handle_t *  BufferHandle,
                                          char            ** Buffer)
{
	globus_result_t   result   = GLOBUS_SUCCESS;
	buffer_t        * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_get_emtpy_buffer);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Buffer = NULL;

	globus_mutex_lock(&BufferHandle->Lock);
	{
		if (BufferHandle->EmptyBufferChain != NULL)
		{
			/* Remove the next free buffer from the list. */
			buffer_s = BufferHandle->EmptyBufferChain;
			BufferHandle->EmptyBufferChain = buffer_s->Next;
			if (BufferHandle->EmptyBufferChain != NULL)
				BufferHandle->EmptyBufferChain->Prev = NULL;

			/* Decrement the empty buffer count. */
			BufferHandle->EmptyBufferCount--;

			/* Save the buffer. */
			*Buffer = buffer_s->Data;

			/* Deallocate the list structure. */
			globus_free(buffer_s);
		}
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	/* Allocate a new free buffer. */
	if (*Buffer == NULL)
		result = globus_i_gfs_hpss_module_allocate_buffer(BufferHandle, Buffer);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 *  Only return EOF if the full buffer chain is empty.
 *  Even though we lock the buffer handle, this function is really only
 *  safe for one requesting thread at a time since we can only have a
 *  single waiter. This function will not return EOF until the
 *  full buffer chain is empty.
 */
static globus_result_t
globus_i_gfs_hpss_module_get_full_buffer(buffer_handle_t *  BufferHandle,
                                         globus_off_t       TransferOffset,
                                         void            ** Buffer,
                                         globus_size_t   *  BufferLength,
                                         globus_bool_t   *  Eof)
{
	globus_result_t   result    = GLOBUS_SUCCESS;
	buffer_t        * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_get_full_buffer);
	GlobusGFSHpssDebugEnter();

	/* Initialize this return value. */
	*Eof = GLOBUS_FALSE;

	globus_mutex_lock(&BufferHandle->Lock);
	{
		/* Allow a first pass even on EOF. */
		while (buffer_s == NULL)
		{
			/* Search the full list for our buffer. */
			for (buffer_s  = BufferHandle->FullBufferChain;
			     buffer_s != NULL && buffer_s->TransferOffset != TransferOffset;
			     buffer_s  = buffer_s->Next);

			/* Don't bother waiting if we have received EOF. */
			if (BufferHandle->Eof == GLOBUS_TRUE)
				break;

			if (buffer_s == NULL)
			{
				/* Sanity check our assumption. */
				globus_assert(BufferHandle->Waiter.Valid == GLOBUS_FALSE);

				/* Wait for it. */
				BufferHandle->Waiter.Valid = GLOBUS_TRUE;
				{
					BufferHandle->Waiter.TransferOffset = TransferOffset;

					/* Setting eof on the buffer handle will wake us. */
					globus_cond_wait(&BufferHandle->Waiter.Cond,
					                 &BufferHandle->Lock);
				}
				BufferHandle->Waiter.Valid = GLOBUS_FALSE;
			}
		}

		/* Either we have a buffer, an error or EOF. */
		if (buffer_s != NULL)
		{
			/* Remove it from the list. */
			if (buffer_s->Prev != NULL)
				buffer_s->Prev->Next = buffer_s->Next;
			else
				BufferHandle->FullBufferChain = buffer_s->Next;

			if (buffer_s->Next != NULL)
				buffer_s->Next->Prev = buffer_s->Prev;

			/* Decrement the full buffer count. */
			BufferHandle->FullBufferCount--;

			/* Save the buffer and the number of bytes it contains. */
			*Buffer       = buffer_s->Data;
			*BufferLength = buffer_s->DataLength;

			/* Deallocate the buffer struct. */
			globus_free(buffer_s);
		}

		/* If the full buffer chain is empty, record the Eof value. */
		if (BufferHandle->FullBufferChain == NULL)
			*Eof = BufferHandle->Eof;
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static globus_result_t
globus_i_gfs_hpss_module_release_full_buffer(buffer_handle_t * BufferHandle,
                                             void            * Buffer,
                                             globus_off_t      TransferOffset,
                                             globus_size_t     BufferLength)
{
	globus_result_t   result   = GLOBUS_SUCCESS;
	buffer_t        * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_release_full_buffer);
	GlobusGFSHpssDebugEnter();

	/* Safety check in case we are called with a NULL buffer. */
	if (Buffer == NULL)
		goto cleanup;

	/* If this buffer contains no data... */
	if (BufferLength == 0)
		goto cleanup;

	/* Allocate the buffer list entry. */
	buffer_s = (buffer_t *) globus_calloc(1, sizeof(buffer_t));
	if (buffer_s == NULL)
	{
		result = GlobusGFSErrorMemory("buffer_t");
		goto cleanup;
	}

	/* Save the buffer. */
	buffer_s->Data           = Buffer;
	buffer_s->TransferOffset = TransferOffset;
	buffer_s->DataLength     = BufferLength;

	/* Now put it on the list. */
	globus_mutex_lock(&BufferHandle->Lock);
	{
/*
 * Assume these are coming in in ascending order.
 */
		/* Put it on the full list. */
		buffer_s->Next = BufferHandle->FullBufferChain;
		if (BufferHandle->FullBufferChain != NULL)
			BufferHandle->FullBufferChain->Prev = buffer_s;
		BufferHandle->FullBufferChain = buffer_s;

		/* Increment the full buffer count. */
		BufferHandle->FullBufferCount++;

		/* If there is a valid waiter... */
		if (BufferHandle->Waiter.Valid == GLOBUS_TRUE)
		{
			/* If they are waiting for this offset... */
			if (BufferHandle->Waiter.TransferOffset == TransferOffset)
			{
				/* Wake the waiter. */
				globus_cond_signal(&BufferHandle->Waiter.Cond);
			}
		}
	}
	globus_mutex_unlock(&BufferHandle->Lock);
	
	/* Release our reference to it. */
	Buffer = NULL;

cleanup:
	/* If we still have a reference to the buffer... */
	if (Buffer != NULL)
	{
		/* Put it on the empty list. */
		globus_i_gfs_hpss_module_release_empty_buffer(BufferHandle, Buffer);
	}

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * Due to a bug in hpss_PIORegister(), each time we call hpss_PIOExecute(), this callback
 * receives the buffer passed to hpss_PIORegister(). To avoid this bug, we must copy the
 * buffer instead of exchanging it. This is HPSS bug 1660.
 */
static int
globus_i_gfs_hpss_module_pio_register_read_callback(
	void         *  UserArg,
	u_signed64      TransferOffset,
	unsigned int *  BufferLength,
	void         ** Buffer)
{
	int             retval          = 0;
	pio_request_t * pio_request     = NULL;
	globus_off_t    transfer_offset = 0;
	char          * empty_buffer    = NULL;

    GlobusGFSName(globus_i_gfs_hpss_module_pio_register_read_callback);
    GlobusGFSHpssDebugEnter();

	/* Make sure we received our arg. */
	globus_assert(UserArg != NULL);

	/* Cast our arg. */
	pio_request = (pio_request_t *) UserArg;

	/* Convert the transfer offset */
	CONVERT_U64_TO_LONGLONG(TransferOffset, transfer_offset);

	if (*BufferLength != 0)
	{
		/* Get an empty buffer. */
		retval = globus_i_gfs_hpss_module_get_emtpy_buffer(pio_request->BufferHandle,
		                                                   &empty_buffer);
		if (retval != 0)
			goto cleanup;

		/* Copy the buffer. */
		memcpy(empty_buffer, *Buffer, *BufferLength);

		/* Release our full buffer. */
		retval = globus_i_gfs_hpss_module_release_full_buffer(pio_request->BufferHandle,
		                                                      empty_buffer,
		                                                      transfer_offset,
		                                                      *BufferLength);
		if (retval != 0)
			goto cleanup;
	}

cleanup:
	GlobusGFSHpssDebugExit();

	/*
	 * Notice that retval maybe non zero if we succeed but something else
	 * in the transfer failed.
	 */
	return retval;
}

/*
 * *Buffer is NULL on the first call even though we passed in a buffer
 *  to PIORegister.
 */
static int
globus_i_gfs_hpss_module_pio_register_write_callback(
	void         *  UserArg,
	u_signed64      TransferOffset,
	unsigned int *  BufferLength,
	void         ** Buffer)
{
	globus_result_t   result          = GLOBUS_SUCCESS;
	pio_request_t   * pio_request     = NULL;
	globus_off_t      transfer_offset = 0;
	globus_size_t     buffer_length   = 0;
	globus_bool_t     eof             = GLOBUS_FALSE;

    GlobusGFSName(globus_i_gfs_hpss_module_pio_register_write_callback);
    GlobusGFSHpssDebugEnter();

	/* Make sure we received our arg. */
	globus_assert(UserArg != NULL);

	/* Cast our arg. */
	pio_request = (pio_request_t *) UserArg;

	/* Convert the transfer offset */
	CONVERT_U64_TO_LONGLONG(TransferOffset, transfer_offset);

	/* Release our free buffer. */
	globus_i_gfs_hpss_module_release_empty_buffer(pio_request->BufferHandle, *Buffer);

	/* Release our reference to the buffer. */
	*Buffer = NULL;

	/* Get the full buffer @ transfer_offset. */
	result = globus_i_gfs_hpss_module_get_full_buffer(pio_request->BufferHandle,
	                                                  transfer_offset,
	                                                  Buffer,
	                                                  &buffer_length,
	                                                  &eof);
/* XXX eof here is unexpected. */

	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Copy out the buffer length. */
	*BufferLength = buffer_length;

cleanup:
	/* On error... */
	if (result != GLOBUS_SUCCESS)
	{
		globus_mutex_lock(&pio_request->Lock);
		{
			if (pio_request->Result == GLOBUS_SUCCESS)
				pio_request->Result = result;
			else
				globus_l_gfs_hpss_common_destroy_result(result);
		}
		globus_mutex_unlock(&pio_request->Lock);

		/* Deallocate our buffer. */
		if (*Buffer != NULL)
			globus_i_gfs_hpss_module_release_empty_buffer(pio_request->BufferHandle, *Buffer);
		*Buffer = NULL;

		GlobusGFSHpssDebugExitWithError();
		return 1;
	}

	GlobusGFSHpssDebugExit();
	return 0;
}

static void *
globus_i_gfs_hpss_module_pio_register(void * Arg)
{
	int               retval      = 0;
	globus_result_t   result      = GLOBUS_SUCCESS;
	pio_request_t   * pio_request = NULL;
	char            * buffer      = NULL;

    GlobusGFSName(globus_i_gfs_hpss_module_pio_register);
    GlobusGFSHpssDebugEnter();

	/*
	 * Make sure we received our arg.
	 */
	globus_assert(Arg != NULL);

	/* Cast the arg to our transfer request. */
	pio_request = (pio_request_t *) Arg;

	/*
	 * Allocate the buffer. (Is it even used?)
	 */
	result = globus_i_gfs_hpss_module_allocate_buffer(pio_request->BufferHandle, &buffer);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Now register this participant. */
	retval = hpss_PIORegister(
	             0,              /* Stripe element.      */
	             0,              /* DataNetAddr (Unused) */
	             buffer,         /* Buffer               */
	             pio_request->BufferHandle->BufferLength,
	             pio_request->Participant.StripeGroup,
	             pio_request->PioOperation == HPSS_PIO_WRITE ?
	              globus_i_gfs_hpss_module_pio_register_write_callback:
	              globus_i_gfs_hpss_module_pio_register_read_callback,
	             pio_request);

	/* If an error occurred... */
	if (retval != 0)
		result = GlobusGFSErrorSystemError("hpss_PIORegister", -retval);

cleanup:
	/*
	 * Release the buffer. On a write request, PIORegister() doesn't pass the buffer to
	 * the callback so we must release it. On a read operation, there's a bug in 
	 * hpss_PIORegister() which forces us to copy the data out instead of exchanging buffers.
	 */
	globus_i_gfs_hpss_module_release_empty_buffer(pio_request->BufferHandle, buffer);

	globus_mutex_lock(&pio_request->Lock);
	{
		if (result != GLOBUS_SUCCESS)
		{
			if (pio_request->Result == GLOBUS_SUCCESS)
				pio_request->Result = result;
			else
				globus_l_gfs_hpss_common_destroy_result(result);
		}

		/* Record that we have exitted. */
		pio_request->ParticHasExitted = GLOBUS_TRUE;

		/* Wake anyone that is waiting. */
		globus_cond_signal(&pio_request->Cond);
	}
	globus_mutex_unlock(&pio_request->Lock);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return NULL;
	}

	GlobusGFSHpssDebugExit();
	return NULL;
}

static void *
globus_i_gfs_hpss_module_pio_execute(void * Arg)
{
	int                  retval      = 0;
	pio_request_t      * pio_request = NULL;
	u_signed64           bytes_moved;
	u_signed64           transfer_offset;
	u_signed64           transfer_length;
	hpss_pio_gapinfo_t   gap_info;

    GlobusGFSName(globus_i_gfs_hpss_module_pio_execute);
    GlobusGFSHpssDebugEnter();

	/*
	 * Make sure we received our arg.
	 */
	globus_assert(Arg != NULL);

	/* Cast the arg to our pio request. */
	pio_request = (pio_request_t *) Arg;

	CONVERT_LONGLONG_TO_U64(pio_request->FileOffset, transfer_offset);
	CONVERT_LONGLONG_TO_U64(pio_request->FileLength, transfer_length);

	/*
	 * During a read from HPSS, if a 'gap' is found, hpss_PIOExecute kicks out with
	 * gap_info containing the offset and length of the gap. If we specify
	 * HPSS_PIO_HANDLE_GAP to hpss_PIOStart(), hpss_PIOExecute() will not kickout
	 * when a gap is encountered, instead it will retry the transfer after the gap.
	 * 
	 * To complicate things further, HPSS 7.4 has a bug with HPSS_PIO_HANDLE_GAP which
	 * causes it to loop forever. So we'll handle looping here. Call report 775.
	 */
	do {
		memset(&gap_info, 0, sizeof(hpss_pio_gapinfo_t));

		/* Now fire off the HPSS side of the transfer. */
		retval = hpss_PIOExecute(pio_request->Coordinator.FileFD,
		                         transfer_offset,
		                         transfer_length,
		                         pio_request->Coordinator.StripeGroup,
		                         &gap_info,
		                         &bytes_moved);

		if (retval == 0)
		{
			/* Remove the bytes transferred so far. */
			transfer_length = sub64m(transfer_length, bytes_moved);
			/* Remove any gap. */
			transfer_length = sub64m(transfer_length, gap_info.Length);

			/* Add the bytes transferred so far. */
			transfer_offset = add64m(transfer_offset, bytes_moved);
			/* Add any gap. */
			transfer_offset = add64m(transfer_offset, gap_info.Length);
		}
	} while (retval == 0 && gt64(transfer_length, cast64m(0)));

	/* If this was a read from HPSS... */
	if (pio_request->PioOperation == HPSS_PIO_READ)
	{
		/* Inform the buffer handle that we have reached EOF. */
		globus_i_gfs_hpss_module_buffer_handle_set_eof(pio_request->BufferHandle);
	}

	globus_mutex_lock(&pio_request->Lock);
	{
		/* If an error occurred... */
		if (retval != 0)
		{
			/* Record it. */
			if (pio_request->Result == GLOBUS_SUCCESS)
				pio_request->Result = GlobusGFSErrorSystemError("hpss_PIOExecute", -retval);
		}

		/* Record that we have exitted. */
		pio_request->CoordHasExitted = GLOBUS_TRUE;

		/* Wake anyone that is waiting. */
		globus_cond_signal(&pio_request->Cond);
	}
	globus_mutex_unlock(&pio_request->Lock);

	if (retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return NULL;
	}

	GlobusGFSHpssDebugExit();
	return NULL;
}

static globus_result_t
globus_i_gfs_hpss_module_pio_begin(pio_request_t        * PioRequest,
                                   int                    HpssFileFD,
                                   hpss_pio_operation_t   PioOperation,
                                   globus_off_t           FileOffset,
                                   globus_off_t           FileLength,
                                   unsigned32             BlockSize,
                                   unsigned32             FileStripeWidth,
                                   buffer_handle_t      * BufferHandle)
{
	int                 retval = 0;
	void              * buffer = NULL;
	unsigned int        buflen = 0;
	globus_result_t     result = GLOBUS_SUCCESS;
    globus_thread_t     thread;
	hpss_pio_params_t   pio_params;

    GlobusGFSName(globus_i_gfs_hpss_module_pio_begin);
    GlobusGFSHpssDebugEnter();

	/* Initialize the pio request structure. */
	memset(PioRequest, 0, sizeof(pio_request_t));
	globus_mutex_init(&PioRequest->Lock, NULL);
	globus_cond_init(&PioRequest->Cond, NULL);

	PioRequest->PioOperation       = PioOperation;
	PioRequest->FileLength         = FileLength;
	PioRequest->FileOffset         = FileOffset;
	PioRequest->BufferHandle       = BufferHandle;
	PioRequest->Coordinator.FileFD = HpssFileFD;

	/*
	 * Don't use HPSS_PIO_HANDLE_GAP, it's bugged in HPSS 7.4.
	 */
	pio_params.Operation       = PioOperation;
	pio_params.ClntStripeWidth = 1;
	pio_params.BlockSize       = BlockSize;
	pio_params.FileStripeWidth = FileStripeWidth;
	pio_params.IOTimeOutSecs   = 0;
	pio_params.Transport       = HPSS_PIO_MVR_SELECT;
	pio_params.Options         = 0;

	/* Now call the start routine. */
	retval = hpss_PIOStart(&pio_params, &PioRequest->Coordinator.StripeGroup);
	if (retval != 0)
	{
        result = GlobusGFSErrorSystemError("hpss_PIOStart", -retval);
		goto cleanup;
	}

	/*
	 * Copy the stripe group for the participant.
	 */
	retval = hpss_PIOExportGrp(PioRequest->Coordinator.StripeGroup,
	                           &buffer,
	                           &buflen);
	if (retval != 0)
	{
        result = GlobusGFSErrorSystemError("hpss_PIOExportGrp", -retval);
		goto cleanup;
	}

	retval = hpss_PIOImportGrp(buffer, buflen, &PioRequest->Participant.StripeGroup);
	if (retval != 0)
	{
        result = GlobusGFSErrorSystemError("hpss_PIOImportGrp", -retval);
		goto cleanup;
	}

	/* Launch the pio register thread. */
	retval = globus_thread_create(&thread,
	                              NULL,
	                              globus_i_gfs_hpss_module_pio_register,
	                              PioRequest);
	if (retval != 0)
	{
		/* Translate the error. */
        result = GlobusGFSErrorSystemError("globus_thread_create", retval);
        goto cleanup;
	}

	/* Launch the pio execute thread. */
	retval = globus_thread_create(&thread,
	                              NULL,
	                              globus_i_gfs_hpss_module_pio_execute,
	                              PioRequest);
	if (retval != 0)
	{
		/* Translate the error. */
        result = GlobusGFSErrorSystemError("globus_thread_create", retval);
        goto cleanup;
	}

cleanup:
	if (buffer != NULL)
		free(buffer);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

    GlobusGFSHpssDebugExit();
    return result;
}

/* 
 * XXX Make sure to prevent bug 1999, nothing can
 * prevent the seqeuence PIOImport->PIORegister->PIOEnd. Calling
 * PIOImport->PIOEnd will cause a segfault.
 */
static globus_result_t
globus_i_gfs_hpss_module_pio_end(pio_request_t * PIORequest)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(globus_i_gfs_hpss_module_pio_end);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&PIORequest->Lock);
	{
		while (PIORequest->CoordHasExitted == GLOBUS_FALSE)
		{
			globus_cond_wait(&PIORequest->Cond, &PIORequest->Lock);
		}
		hpss_PIOEnd(PIORequest->Coordinator.StripeGroup);

		while (PIORequest->ParticHasExitted == GLOBUS_FALSE)
		{
			globus_cond_wait(&PIORequest->Cond, &PIORequest->Lock);
		}
		hpss_PIOEnd(PIORequest->Participant.StripeGroup);

		/* Save the return value. */
		result = PIORequest->Result;
	}
	globus_mutex_unlock(&PIORequest->Lock);

	globus_mutex_destroy(&PIORequest->Lock);
	globus_cond_destroy(&PIORequest->Cond);

	GlobusGFSHpssDebugExit();

	return result;
}


static void
globus_i_gfs_hpss_module_gridftp_init_request(gridftp_request_t * GridFTPRequest,
                                              buffer_handle_t   * BufferHandle)
{
	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_init_request);
	GlobusGFSHpssDebugEnter();

	/* Initialize our request structure. */
	memset(GridFTPRequest, 0, sizeof(gridftp_request_t));

	globus_mutex_init(&GridFTPRequest->Lock, NULL);
	globus_cond_init(&GridFTPRequest->Cond, NULL);
	GridFTPRequest->BufferHandle   = BufferHandle;
	GridFTPRequest->Result         = GLOBUS_SUCCESS;
	GridFTPRequest->Eof            = GLOBUS_FALSE;
	GridFTPRequest->OpCount        = 0;
	GridFTPRequest->TransferOffset = 0;

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_module_gridftp_destroy_request(gridftp_request_t * GridFTPRequest)
{
	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_destroy_request);
	GlobusGFSHpssDebugEnter();

	globus_mutex_destroy(&GridFTPRequest->Lock);
	globus_cond_destroy(&GridFTPRequest->Cond);

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_module_gridftp_read_callback(
    globus_gfs_operation_t   Operation,
    globus_result_t          Result,
    globus_byte_t          * Buffer,
    globus_size_t            Length,
    globus_off_t             TransferOffset,
    globus_bool_t            Eof,
    void                   * UserArg)
{
	int                 ops_needed      = 0;
	char              * buffer          = NULL;
	globus_result_t     result          = GLOBUS_SUCCESS;
	gridftp_request_t * gridftp_request = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_read_callback);
	GlobusGFSHpssDebugEnter();

	/* Make sure we recieved our UserArg. */
	globus_assert(UserArg != NULL);

	/* Cast it to our gridftp request. */
	gridftp_request = (gridftp_request_t *) UserArg;

	/* If an error did not occur on this read... */
	if (Result == GLOBUS_SUCCESS)
	{
		/* Release it. */
		result = globus_i_gfs_hpss_module_release_full_buffer(
		                        gridftp_request->BufferHandle,
		                        Buffer,
		                        TransferOffset,
		                        Length);
	}

	/* On error, release the buffer */
	if (Result != GLOBUS_SUCCESS || result != GLOBUS_SUCCESS)
		globus_i_gfs_hpss_module_release_empty_buffer(gridftp_request->BufferHandle, Buffer);

	/* Copy out the error. */
	if (Result != GLOBUS_SUCCESS)
		result = Result;

	/* Remove our reference to the buffer. */
	Buffer = NULL;

	/* If we received EOF, alert the buffer handle. */
	if (Eof == GLOBUS_TRUE)
		globus_i_gfs_hpss_module_buffer_handle_set_eof(gridftp_request->BufferHandle);

	globus_mutex_lock(&gridftp_request->Lock);
	{
		/* Decrement the op count. */
		gridftp_request->OpCount--;

		/*
		 * Record any errors. 
		 */
		if (result != GLOBUS_SUCCESS && gridftp_request->Result == GLOBUS_SUCCESS)
			gridftp_request->Result = result;

		/* Record eof. */
		if (Eof == GLOBUS_TRUE)
			gridftp_request->Eof = GLOBUS_TRUE;

		/* If no errors have occurred and we have not reached EOF... */
		if (gridftp_request->Result == GLOBUS_SUCCESS && gridftp_request->Eof == GLOBUS_FALSE)
		{
			/* Get the optimal number. */
			globus_gridftp_server_get_optimal_concurrency(Operation, &ops_needed);

			/* Subtract the number of current operations. */
			ops_needed -= gridftp_request->OpCount;

			/* Submit the buffers. */
			for (; ops_needed > 0; ops_needed--)
			{
				/* Get the buffer at the current offset. */
				result = globus_i_gfs_hpss_module_get_emtpy_buffer(gridftp_request->BufferHandle, 
				                                                  &buffer);
				if (result != GLOBUS_SUCCESS)
					break;

				/* If we received a buffer and not just EOF... */
				if (buffer != NULL)
				{
					/* Now register the write operation. */
					result = globus_gridftp_server_register_read(
					             Operation,
					             (globus_byte_t *)buffer,
					             gridftp_request->BufferHandle->BufferLength,
					             globus_i_gfs_hpss_module_gridftp_read_callback,
					             gridftp_request);

					if (result != GLOBUS_SUCCESS)
						break;

					/* Release our buffer handle. */
					buffer = NULL;

					/* Increment OpCount. */
					gridftp_request->OpCount++;
				}
			}

			if (result != GLOBUS_SUCCESS)
				gridftp_request->Result = result;
		}

		/* Wake anyone that may be waiting. */
		if (gridftp_request->OpCount == 0 &&
		   (gridftp_request->Result  != GLOBUS_SUCCESS ||
		    gridftp_request->Eof     != GLOBUS_FALSE))
		{
			globus_cond_signal(&gridftp_request->Cond);
		}
	}
	globus_mutex_unlock(&gridftp_request->Lock);

	if (buffer != NULL)
		globus_i_gfs_hpss_module_release_empty_buffer(gridftp_request->BufferHandle, buffer);

	if (Result != GLOBUS_SUCCESS || result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

static globus_result_t
globus_i_gfs_hpss_module_gridftp_read_start(globus_gfs_operation_t   Operation,
                                            buffer_handle_t        * BufferHandle,
                                            gridftp_request_t      * GridFTPRequest)
{
	char            * buffer = NULL;
	globus_result_t   result = GLOBUS_SUCCESS;

	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_read_start);
	GlobusGFSHpssDebugEnter();

	/* Initialize our request structure. */
	globus_i_gfs_hpss_module_gridftp_init_request(GridFTPRequest, BufferHandle);


	/* Get the first full buffer. */
	result = globus_i_gfs_hpss_module_get_emtpy_buffer(BufferHandle, &buffer);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	globus_mutex_lock(&GridFTPRequest->Lock);
	{
		/* Initiate the first gridftp read. */
		result = globus_gridftp_server_register_read(
		             Operation,
		             (globus_byte_t *)buffer,
		             BufferHandle->BufferLength,
		             globus_i_gfs_hpss_module_gridftp_read_callback,
		             GridFTPRequest);

		if (result == GLOBUS_SUCCESS)
		{
			/* Initialize the op count. */
			GridFTPRequest->OpCount = 1;

			/* Release our buffer handle. */
			buffer = NULL;
		}
	}
	globus_mutex_unlock(&GridFTPRequest->Lock);

cleanup:

	/* If we still holding a buffer... */
	if (buffer != NULL)
		globus_i_gfs_hpss_module_release_empty_buffer(BufferHandle, buffer);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static void
globus_i_gfs_hpss_module_gridftp_write_callback(
    globus_gfs_operation_t   Operation,
    globus_result_t          Result,
    globus_byte_t          * Buffer,
    globus_size_t            Length,
    void                   * UserArg)
{
	int                 ops_needed      = 0;
	char              * buffer          = NULL;
	globus_size_t       buffer_length   = 0;
	globus_result_t     result          = GLOBUS_SUCCESS;
	gridftp_request_t * gridftp_request = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_write_callback);
	GlobusGFSHpssDebugEnter();

	/* Make sure we recieved our UserArg. */
	globus_assert(UserArg != NULL);

	/* Cast it to our gridftp request. */
	gridftp_request = (gridftp_request_t *) UserArg;

	/* If the write was successful... */
	if (Result == GLOBUS_SUCCESS)
	{
		/* Release the empty buffer. */
		globus_i_gfs_hpss_module_release_empty_buffer(gridftp_request->BufferHandle,
	                                                  Buffer);
	}

	/* On error, release the buffer (it's the only safe route) */
	if (Result != GLOBUS_SUCCESS)
		globus_i_gfs_hpss_module_release_empty_buffer(gridftp_request->BufferHandle, Buffer);

	/* Remove our reference to the buffer. */
	Buffer = NULL;

	globus_mutex_lock(&gridftp_request->Lock);
	{
		/* Decrement the op count. */
		gridftp_request->OpCount--;

		/*
		 * Record any errors. 
		 */
		if (Result != GLOBUS_SUCCESS && gridftp_request->Result == GLOBUS_SUCCESS)
			gridftp_request->Result = Result;

		/* If no errors have occurred and we have not reached EOF... */
		if (gridftp_request->Result == GLOBUS_SUCCESS && gridftp_request->Eof == GLOBUS_FALSE)
		{
			/* Get the optimal number. */
			globus_gridftp_server_get_optimal_concurrency(Operation, &ops_needed);

			/* Subtract the number of current operations. */
			ops_needed -= gridftp_request->OpCount;

			/* Submit the buffers. */
			for (; ops_needed > 0; ops_needed--)
			{
				/* Get the buffer at the current offset. */
				result = globus_i_gfs_hpss_module_get_full_buffer(gridftp_request->BufferHandle, 
				                                                  gridftp_request->TransferOffset,
				                                                  (void **)&buffer,
				                                                  &buffer_length,
				                                                  &gridftp_request->Eof);
				if (result != GLOBUS_SUCCESS)
					break;

				/* If we received a buffer and not just EOF... */
				if (buffer != NULL)
				{
					/* Now register the write operation. */
					result = globus_gridftp_server_register_write(
					             Operation,
					             (globus_byte_t *)buffer,
					             buffer_length,
					             gridftp_request->TransferOffset,
					             0,
					             globus_i_gfs_hpss_module_gridftp_write_callback,
					             gridftp_request);

					if (result != GLOBUS_SUCCESS)
						break;

					/* Release our buffer handle. */
					buffer = NULL;

					/* Increment OpCount. */
					gridftp_request->OpCount++;

					/* Increment the transfer offset. */
					gridftp_request->TransferOffset += buffer_length;
				}

				/* Break out if we received EOF. */
				if (gridftp_request->Eof == GLOBUS_TRUE)
					break;
			}

			if (result != GLOBUS_SUCCESS)
				gridftp_request->Result = result;
		}

		/* Wake anyone that may be waiting. */
		if (gridftp_request->OpCount == 0 &&
		   (gridftp_request->Result  != GLOBUS_SUCCESS ||
		    gridftp_request->Eof     != GLOBUS_FALSE))
		{
			globus_cond_signal(&gridftp_request->Cond);
		}
	}
	globus_mutex_unlock(&gridftp_request->Lock);

	if (buffer != NULL)
		globus_i_gfs_hpss_module_release_empty_buffer(gridftp_request->BufferHandle, buffer);

	if (Result != GLOBUS_SUCCESS || result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

static globus_result_t
globus_i_gfs_hpss_module_gridftp_write_start(globus_gfs_operation_t   Operation,
                                             buffer_handle_t        * BufferHandle,
                                             gridftp_request_t      * GridFTPRequest)
{
	char              * buffer          = NULL;
	globus_result_t     result          = GLOBUS_SUCCESS;
	globus_size_t       buffer_length   = 0;

	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_write_start);
	GlobusGFSHpssDebugEnter();

	/* Initialize our request structure. */
	globus_i_gfs_hpss_module_gridftp_init_request(GridFTPRequest, BufferHandle);


	/* Get the first full buffer. */
	result = globus_i_gfs_hpss_module_get_full_buffer(BufferHandle, 
	                                                  0,
	                                                  (void **)&buffer,
	                                                  &buffer_length,
	                                                  &GridFTPRequest->Eof);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* If we received a buffer and not just EOF... */
	if (buffer != NULL)
	{
		globus_mutex_lock(&GridFTPRequest->Lock);
		{
			/* Initiate the first gridftp write. */
			result = globus_gridftp_server_register_write(
			                   Operation,
			                   (globus_byte_t *)buffer,
			                   buffer_length,
			                   0, /* Transfer offset. */
			                   0,
			                   globus_i_gfs_hpss_module_gridftp_write_callback,
			                   GridFTPRequest);

			if (result == GLOBUS_SUCCESS)
			{
				/* Initialize the transfer offset in the request. */
				GridFTPRequest->TransferOffset = buffer_length;

				/* Initialize the op count. */
				GridFTPRequest->OpCount = 1;

				/* Release our buffer handle. */
				buffer = NULL;
			}
		}
		globus_mutex_unlock(&GridFTPRequest->Lock);
	}

cleanup:

	/* If we still holding a buffer... */
	if (buffer != NULL)
		globus_i_gfs_hpss_module_release_empty_buffer(GridFTPRequest->BufferHandle, buffer);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static globus_result_t
globus_i_gfs_hpss_module_gridftp_end(gridftp_request_t * GridFTPRequest)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_end);
	GlobusGFSHpssDebugEnter();

	/* Wait for the transfer to complete. */
	globus_mutex_lock(&GridFTPRequest->Lock);
	{
		while (GridFTPRequest->OpCount != 0 ||
		      (GridFTPRequest->Result  == GLOBUS_SUCCESS && 
		       GridFTPRequest->Eof     == GLOBUS_FALSE))
		{
			globus_cond_wait(&GridFTPRequest->Cond, &GridFTPRequest->Lock);
		}
	}
	globus_mutex_unlock(&GridFTPRequest->Lock);

	/* Save the return value. */
	result = GridFTPRequest->Result;

	/* Clean up the request memory. */
	globus_i_gfs_hpss_module_gridftp_destroy_request(GridFTPRequest);

	GlobusGFSHpssDebugExit();
	return result;
}

/*
 * Right now we handle list_type "LIST:", MSLD is list_type "TMSPUOIGDQLN". 
 * There may be others, like NLST?
 */
static void
globus_l_gfs_hpss_module_list(
    globus_gfs_operation_t       Operation,
    globus_gfs_transfer_info_t * TransferInfo,
    void                       * UserArg)
{
	int                 dirfd           = -1;
	int                 retval          = 0;
	char              * buffer          = NULL;
	char              * fullpath        = NULL;
	int                 buffer_length   = 0;
	globus_bool_t       first_pass      = GLOBUS_TRUE;
	globus_off_t        transfer_offset = 0;
	globus_result_t     gridftp_result  = GLOBUS_SUCCESS;
	globus_result_t     result          = GLOBUS_SUCCESS;
	gridftp_request_t   gridftp_request;
	buffer_handle_t     buffer_handle;
	hpss_dirent_t       dirent;
	hpss_stat_t         hpss_stat_buf;

	GlobusGFSName(globus_l_gfs_hpss_module_list);
	GlobusGFSHpssDebugEnter();

    /* Inform the server that we are starting. */
	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	/* Initialize the free & empty buffer chains. */
	globus_i_gfs_hpss_module_init_buffer_handle(Operation, &buffer_handle);

	/* Stat the given path. */
	retval = hpss_Lstat(TransferInfo->pathname, &hpss_stat_buf);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Lstat", -retval);
		goto cleanup;
	}

	/*
	 * Handle non directories.
	 */
	if (!S_ISDIR(hpss_stat_buf.st_mode))
	{
		/* Get a free buffer. */
		result = globus_i_gfs_hpss_module_get_emtpy_buffer(&buffer_handle, &buffer);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Translate the stat info to a listing. */
		globus_l_gfs_hpss_module_list_single_line(&hpss_stat_buf,
		                                          TransferInfo->pathname,
		                                          buffer,
		                                          buffer_handle.BufferLength);


		/* Release the full buffer. */
		result = globus_i_gfs_hpss_module_release_full_buffer(&buffer_handle,
		                                                       buffer,
		                                                       transfer_offset,
		                                                       strlen(buffer));

		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Release our reference to the buffer. */
		buffer = NULL;

		/*
		 * Order is important here; gridftp_write_start requires a full buffer
		 * before it is called. It will not return until it has it.
		 */

		/* Fire off the GridFTP write callbacks. */
		result = globus_i_gfs_hpss_module_gridftp_write_start(Operation, 
		                                                      &buffer_handle,
		                                                      &gridftp_request);
		goto cleanup;
	}

	/*
	 * Handle directories.
	 */
	dirfd = hpss_Opendir(TransferInfo->pathname);
	if (dirfd < 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Opendir", -dirfd);
		goto cleanup;
	}

	while (GLOBUS_TRUE)
	{
		retval = hpss_Readdir(dirfd, &dirent);
		if (retval != 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Readdir", -retval);
			goto cleanup;
		}

		if (dirent.d_namelen == 0)
			break;

		/* Construct the full path. */
		fullpath = (char *) globus_malloc(strlen(TransferInfo->pathname) + strlen(dirent.d_name) + 2);
		if (fullpath == NULL)
		{
			result = GlobusGFSErrorMemory("fullpath");
			goto cleanup;
		}

		if (TransferInfo->pathname[strlen(TransferInfo->pathname) - 1] == '/')
			sprintf(fullpath, "%s%s", TransferInfo->pathname, dirent.d_name);
		else
			sprintf(fullpath, "%s/%s", TransferInfo->pathname, dirent.d_name);

		retval = hpss_Lstat(fullpath, &hpss_stat_buf);
		if (retval != 0)
		{
			globus_free(fullpath);

			if (retval == -ENOENT)
				continue;

			result = GlobusGFSErrorSystemError("hpss_Lstat", -retval);
			goto cleanup;
		}

		/* Get a free buffer. */
		result = globus_i_gfs_hpss_module_get_emtpy_buffer(&buffer_handle, &buffer);
		if (result != GLOBUS_SUCCESS)
		{
			globus_free(fullpath);
			goto cleanup;
		}

		/* Translate the stat info to a listing. */
		globus_l_gfs_hpss_module_list_single_line(&hpss_stat_buf,
		                                          fullpath,
		                                          buffer,
		                                          buffer_handle.BufferLength);

		globus_free(fullpath);

		/* Record the length of the buffer. */
		buffer_length = strlen(buffer);

		/* Release the full buffer. */
		result = globus_i_gfs_hpss_module_release_full_buffer(&buffer_handle,
		                                                       buffer,
		                                                       transfer_offset,
		                                                       buffer_length);

		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Release our reference to the buffer. */
		buffer = NULL;

		/* Increment the transfer offset. */
		transfer_offset += buffer_length;

		/* If this is our first pass... */
		if (first_pass == GLOBUS_TRUE)
		{
			first_pass = GLOBUS_FALSE;

			/*
			 * Order is important here; gridftp_write_start requires a full buffer
			 * before it is called. It will not return until it has it.
			 */

			/* Fire off the GridFTP write callbacks. */
			result = globus_i_gfs_hpss_module_gridftp_write_start(Operation, 
			                                                      &buffer_handle,
			                                                      &gridftp_request);
			if (result != GLOBUS_SUCCESS)
				goto cleanup;
		}
	}

cleanup:
	if (dirfd >= 0)
		hpss_Closedir(dirfd);

	if (buffer != NULL)
		globus_i_gfs_hpss_module_release_empty_buffer(gridftp_request.BufferHandle, buffer);

	/* Indicate EOF to the buffer handle. */
	globus_i_gfs_hpss_module_buffer_handle_set_eof(&buffer_handle);

	/* Wait for the gridftp write callbacks to finish. */
	gridftp_result = globus_i_gfs_hpss_module_gridftp_end(&gridftp_request);

	if (result == GLOBUS_SUCCESS)
		result =  gridftp_result;

	/* Destroy the buffer handle. */
	globus_i_gfs_hpss_module_destroy_buffer_handle(&buffer_handle);

	/* Let the server know we are finished. */
	globus_gridftp_server_finished_transfer(Operation, result);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

/*
 * RETR Operation
 */
static void
globus_l_gfs_hpss_module_send(
    globus_gfs_operation_t       Operation,
    globus_gfs_transfer_info_t * TransferInfo,
    void                       * UserArg)
{
	int                     hpss_file_fd   = -1;
	globus_result_t         result         = GLOBUS_SUCCESS;
	globus_result_t         pio_result     = GLOBUS_SUCCESS;
	globus_result_t         gridftp_result = GLOBUS_SUCCESS;
	globus_off_t            file_offset    = 0;
	globus_off_t            file_length    = 0;
	globus_gfs_stat_t     * gfs_stat_array = NULL;
	int                     gfs_stat_count = 0;
	pio_request_t           pio_request;
	gridftp_request_t       gridftp_request;
	buffer_handle_t         buffer_handle;
	hpss_cos_hints_t        hints_in;
	hpss_cos_hints_t        hints_out;
	hpss_cos_priorities_t   priorities;

	GlobusGFSName(globus_l_gfs_hpss_module_send);
	GlobusGFSHpssDebugEnter();

    /* Inform the server that we are starting. */
	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	/* Initialize the free & empty buffer chains. */
	globus_i_gfs_hpss_module_init_buffer_handle(Operation, &buffer_handle);

	/* Initialize the hints in. */
	memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

	if (TransferInfo->partial_offset != -1)
		file_offset = TransferInfo->partial_offset;
	file_length = TransferInfo->partial_length;

	/* If the length wasn't given... */
	if (file_length == -1)
	{
		/* Stat the file. */
		result = globus_l_gfs_hpss_common_gfs_stat(TransferInfo->pathname,
		                                           GLOBUS_TRUE,  /* FileOnly        */
		                                           GLOBUS_FALSE, /* UseSymlinkInfo  */
		                                           GLOBUS_TRUE,  /* IncludePathStat */
		                                           &gfs_stat_array,
		                                           &gfs_stat_count);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		file_length = gfs_stat_array[0].size;

		/* Adjust for partial offset transfers. */
		file_length -= file_offset;

		globus_l_gfs_hpss_common_destroy_gfs_stat_array(gfs_stat_array, gfs_stat_count);
	}

	/* Initialize the hints out. */
	memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the priorities. */
	memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

	/* Open the HPSS file. */
	hpss_file_fd = hpss_Open(TransferInfo->pathname,
	                         O_RDONLY, /* No append, yet */
	                         0,
	                         &hints_in,    /* Hints In      */
	                         &priorities,  /* Priorities In */
	                         &hints_out);  /* Hints Out     */
	if (hpss_file_fd < 0)
	{
        result = GlobusGFSErrorSystemError("hpss_Open", -hpss_file_fd);
		goto cleanup;
	}

	/* Fire off the PIO side of the transfer. */
	result = globus_i_gfs_hpss_module_pio_begin(&pio_request,
	                                            hpss_file_fd,
	                                            HPSS_PIO_READ,
	                                            file_offset,
	                                            file_length,
	                                            buffer_handle.BufferLength,
	                                            hints_out.StripeWidth,
	                                            &buffer_handle);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Order is important here; gridftp_write_start requires a full buffer
	 * before it is called. It will not return until it has it.
	 */
	                                            
	/* Fire off the GridFTP read callbacks. */
	result = globus_i_gfs_hpss_module_gridftp_write_start(Operation, 
	                                                      &buffer_handle,
	                                                      &gridftp_request);
	if (result != GLOBUS_SUCCESS)
		goto pio_end;

	/* End the GridFTP request. */
	gridftp_result = globus_i_gfs_hpss_module_gridftp_end(&gridftp_request);

/* XXX need to process both sides here. */
/* XXX I think jumping here will hang. */
pio_end:
	/* End the PIO request. */
	pio_result = globus_i_gfs_hpss_module_pio_end(&pio_request);

	if (result == GLOBUS_SUCCESS)
	{
		if (pio_result == GLOBUS_SUCCESS)
			result = gridftp_result;
		else
			result = pio_result;
	}

cleanup:
	if (hpss_file_fd >= 0)
		hpss_Close(hpss_file_fd);

	/* Destroy the buffer handle. */
	globus_i_gfs_hpss_module_destroy_buffer_handle(&buffer_handle);

	/* Let the server know we are finished. */
	globus_gridftp_server_finished_transfer(Operation, result);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

/*
 * STOR operation
 */
static void
globus_l_gfs_hpss_module_recv(
    globus_gfs_operation_t       Operation,
    globus_gfs_transfer_info_t * TransferInfo,
    void                       * UserArg)
{
	int                   hpss_file_fd   = -1;
	globus_result_t       result         = GLOBUS_SUCCESS;
	globus_result_t       pio_result     = GLOBUS_SUCCESS;
	globus_result_t       gridftp_result = GLOBUS_SUCCESS;
	globus_off_t          file_offset    = 0;
	globus_off_t          file_length    = 0;
	pio_request_t         pio_request;
	gridftp_request_t     gridftp_request;
	buffer_handle_t       buffer_handle;
	hpss_cos_hints_t      hints_in;
	hpss_cos_hints_t      hints_out;
	hpss_cos_priorities_t priorities;

	GlobusGFSName(globus_l_gfs_hpss_module_recv);
	GlobusGFSHpssDebugEnter();

    /* Inform the server that we are starting. */
	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	/* Initialize the free & empty buffer chains. */
	globus_i_gfs_hpss_module_init_buffer_handle(Operation, &buffer_handle);

	/* Make sure we know how much data we are receiving. */
	if (TransferInfo->alloc_size == 0)
	{
		result = GlobusGFSErrorGeneric("Client must specify ALLO");
		goto cleanup;
	}

	/* We only handle truncation at this point. */
	if (TransferInfo->truncate == GLOBUS_FALSE)
	{
		result = GlobusGFSErrorGeneric("Appending is not currently allowed");
		goto cleanup;
	}

	file_offset = 0;
	file_length = TransferInfo->alloc_size;

	/* Initialize the hints in. */
	memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

	CONVERT_LONGLONG_TO_U64(file_length, hints_in.MinFileSize);
	CONVERT_LONGLONG_TO_U64(file_length, hints_in.MaxFileSize);

	/* Initialize the hints out. */
	memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the priorities. */
	memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

	priorities.MinFileSizePriority = LOWEST_PRIORITY;
	priorities.MaxFileSizePriority = LOWEST_PRIORITY;

	/* Open the HPSS file. */
	hpss_file_fd = hpss_Open(TransferInfo->pathname,
	                         O_WRONLY|O_CREAT|O_TRUNC, /* No append, yet */
	                         S_IRUSR|S_IWUSR,
	                         &hints_in,    /* Hints In      */
	                         &priorities,  /* Priorities In */
	                         &hints_out);  /* Hints Out     */
	if (hpss_file_fd < 0)
	{
        result = GlobusGFSErrorSystemError("hpss_Open", -hpss_file_fd);
		goto cleanup;
	}

	/* Fire off the PIO side of the transfer. */
	result = globus_i_gfs_hpss_module_pio_begin(&pio_request,
	                                            hpss_file_fd,
	                                            HPSS_PIO_WRITE,
	                                            file_offset,
	                                            file_length,
	                                            buffer_handle.BufferLength,
	                                            hints_out.StripeWidth,
	                                            &buffer_handle);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Fire off the GridFTP read callbacks. */
	result = globus_i_gfs_hpss_module_gridftp_read_start(Operation, 
	                                                     &buffer_handle, 
	                                                     &gridftp_request);
	if (result != GLOBUS_SUCCESS)
		goto pio_end;

/* XXX need to process both sides here. */
	/* End the GridFTP request. */
	gridftp_result = globus_i_gfs_hpss_module_gridftp_end(&gridftp_request);

/* XXX I think jumping here will hang. */
pio_end:
	/* End the PIO request. */
	pio_result = globus_i_gfs_hpss_module_pio_end(&pio_request);

	if (result == GLOBUS_SUCCESS)
	{
		if (pio_result == GLOBUS_SUCCESS)
			result = gridftp_result;
		else
			result = pio_result;
	}

cleanup:
	if (hpss_file_fd >= 0)
		hpss_Close(hpss_file_fd);

	/* Destroy the buffer handle. */
	globus_i_gfs_hpss_module_destroy_buffer_handle(&buffer_handle);

	/* Let the server know we are finished. */
	globus_gridftp_server_finished_transfer(Operation, result);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

static globus_result_t
globus_l_gfs_hpss_module_chgrp(char * Pathname,
                               char * GroupName)
{
	int                 retval = 0;
	gid_t               gid    = 0;
	globus_result_t     result = GLOBUS_SUCCESS;
    globus_gfs_stat_t * gfs_stat_array = NULL;
	int                 gfs_stat_count = 0;

	GlobusGFSName(globus_l_gfs_hpss_module_chgrp);
	GlobusGFSHpssDebugEnter();

	/* Stat it, make sure it exists. */
	result = globus_l_gfs_hpss_common_gfs_stat(Pathname,
	                                           GLOBUS_TRUE,  /* FileOnly        */
	                                           GLOBUS_FALSE, /* UseSymlinkInfo  */
	                                           GLOBUS_TRUE,  /* IncludePathStat */
	                                           &gfs_stat_array,
	                                           &gfs_stat_count);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* If the given group is not a digit... */
	if (!isdigit(*GroupName))
	{
		/* Convert to the gid. */
		result = globus_l_gfs_hpss_common_groupname_to_gid(GroupName,
		                                                   &gid);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;
	} else
	{
		/* Convert to the gid. */
		gid = atoi(GroupName);
	}

	/* Now change the group. */
	retval = hpss_Chown(Pathname, gfs_stat_array[0].uid, gid);
	if (retval != 0)
	{
       	result = GlobusGFSErrorSystemError("hpss_Chgrp", -retval);
		goto cleanup;
	}

cleanup:
	/* clean up the statbuf. */
	globus_l_gfs_hpss_common_destroy_gfs_stat_array(gfs_stat_array, gfs_stat_count);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static void
globus_l_gfs_hpss_module_command(
    globus_gfs_operation_t      Operation,
    globus_gfs_command_info_t * CommandInfo,
    void                      * UserArg)
{
	int                retval         = 0;
	char             * command_output = NULL;
	globus_result_t    result         = GLOBUS_SUCCESS;
/*	session_handle_t * session_handle = NULL; */

	GlobusGFSName(globus_l_gfs_hpss_module_command);
	GlobusGFSHpssDebugEnter();

	/* Make sure our arg was passed in. */
/*	globus_assert(UserArg != NULL); */
	/* Cast it to our session handle. */
/*	session_handle = (session_handle_t *) UserArg; */

	switch (CommandInfo->command)
	{
	case GLOBUS_GFS_CMD_DELE:
		retval = hpss_Unlink(CommandInfo->pathname);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Unlink", -retval);
		break;

	case GLOBUS_GFS_CMD_MKD:
		retval = hpss_Mkdir(CommandInfo->pathname, S_IRWXU);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Mkdir", -retval);
		break;

	case GLOBUS_GFS_CMD_RMD:
		retval = hpss_Rmdir(CommandInfo->pathname);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Rmdir", -retval);
		break;

	case GLOBUS_GFS_CMD_SITE_AUTHZ_ASSERT:
	case GLOBUS_GFS_CMD_SITE_RDEL:
		break;

	case GLOBUS_GFS_CMD_RNTO:
		retval = hpss_Rename(CommandInfo->from_pathname,
		                     CommandInfo->pathname);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Rename", -retval);
		break;

	case GLOBUS_GFS_CMD_RNFR:
		/* XXX Anything to do here? */
		break;

	case GLOBUS_GFS_CMD_SITE_CHMOD:
		retval = hpss_Chmod(CommandInfo->pathname, 
		                    CommandInfo->chmod_mode);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Chmod", -retval);
		break;

	case GLOBUS_GFS_CMD_SITE_CHGRP:
	    result = globus_l_gfs_hpss_module_chgrp(CommandInfo->pathname,
		                                        CommandInfo->chgrp_group);
		break;

	case GLOBUS_GFS_CMD_CKSM:
	case GLOBUS_GFS_CMD_SITE_DSI:
	case GLOBUS_GFS_CMD_SITE_SETNETSTACK:
	case GLOBUS_GFS_CMD_SITE_SETDISKSTACK:
	case GLOBUS_GFS_CMD_SITE_CLIENTINFO:
	case GLOBUS_GFS_CMD_DCSC:
	default:
		result = GlobusGFSErrorGeneric("Not Supported");
		break;
	}

	globus_gridftp_server_finished_command(Operation, result, command_output);

	if (command_output != NULL)
		globus_free(command_output);
	GlobusGFSHpssDebugExit();
}

/*
 * Stat just the one object pointed to by StatInfo.
 */
void
globus_l_gfs_hpss_module_stat(globus_gfs_operation_t   Operation,
                              globus_gfs_stat_info_t * StatInfo,
                              void                   * Arg)
{
	globus_result_t     result         = GLOBUS_SUCCESS;
	globus_gfs_stat_t * gfs_stat_array = NULL;
	int                 gfs_stat_count = 0;

	GlobusGFSName(globus_i_gfs_hpss_module_stat);
	GlobusGFSHpssDebugEnter();

	/* Make sure Arg was provided. */
	/* globus_assert(Arg != NULL); */


	result = globus_l_gfs_hpss_common_gfs_stat(StatInfo->pathname,
	                                           StatInfo->file_only,
	                                           StatInfo->use_symlink_info,
	                                           StatInfo->include_path_stat,
	                                           &gfs_stat_array,
	                                           &gfs_stat_count);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Inform the server that we are done. */
	globus_gridftp_server_finished_stat(Operation, 
	                                    result, 
	                                    gfs_stat_array, 
	                                    gfs_stat_count);

	/* Destroy the gfs_stat_array. */
	globus_l_gfs_hpss_common_destroy_gfs_stat_array(gfs_stat_array, gfs_stat_count);

	GlobusGFSHpssDebugExit();
	return;

cleanup:
	/* Inform the server that we completed with an error. */
	globus_gridftp_server_finished_stat(Operation, result, NULL, 0);

	GlobusGFSHpssDebugExitWithError();
}


static int globus_l_gfs_hpss_module_activate(void);
static int globus_l_gfs_hpss_module_deactivate(void);

static globus_gfs_storage_iface_t globus_l_gfs_hpss_dsi_iface = 
{
	0,
	globus_l_gfs_hpss_module_session_start,
	globus_l_gfs_hpss_module_session_end,
	globus_l_gfs_hpss_module_list,
	globus_l_gfs_hpss_module_send,
	globus_l_gfs_hpss_module_recv,
	NULL, /* globus_l_gfs_hpss_module_trev, */
	NULL, /* globus_l_gfs_hpss_module_active, */
	NULL, /* globus_l_gfs_hpss_module_passive, */
	NULL, /* globus_l_gfs_hpss_module_data_destroy, */
	globus_l_gfs_hpss_module_command,
	globus_l_gfs_hpss_module_stat,
	NULL,
	NULL
};

GlobusExtensionDefineModule(globus_gridftp_server_hpss) =
{
    "globus_gridftp_server_hpss",
    globus_l_gfs_hpss_module_activate,
    globus_l_gfs_hpss_module_deactivate,
    GLOBUS_NULL,
    GLOBUS_NULL,
    &local_version
};

static int
globus_l_gfs_hpss_module_activate(void)
{
    int rc;
    
    rc = globus_module_activate(GLOBUS_COMMON_MODULE);
    if(rc != GLOBUS_SUCCESS)
    {
        goto error;
    }
    
    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY,
        "hpss",
        GlobusExtensionMyModule(globus_gridftp_server_hpss),
        &globus_l_gfs_hpss_dsi_iface);

    GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS,
        ERROR WARNING TRACE INTERNAL_TRACE INFO STATE INFO_VERBOSE);
    
    return GLOBUS_SUCCESS;

error:
    return rc;
}

static int
globus_l_gfs_hpss_module_deactivate(void)
{
    globus_extension_registry_remove(
        GLOBUS_GFS_DSI_REGISTRY, "hpss");
        
    globus_module_deactivate(GLOBUS_COMMON_MODULE);
    
    return GLOBUS_SUCCESS;
}

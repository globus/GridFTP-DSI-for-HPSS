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
/* Temporary include to support 32 bit 7.3.3. */
#include <u_signed64.h>

/*
 * Local includes.
 */
#include "version.h"
#include "globus_gridftp_server_hpss_common.h"
#include "globus_gridftp_server_hpss_config.h"
/* include "version.h" */

typedef struct buffer {
	char          * Data;
	globus_size_t   DataLength;
	globus_off_t    TransferOffset;
	struct buffer * Next;
	struct buffer * Prev;
} buffer_t;

typedef enum {
	READ_REQUEST,
	WRITE_REQUEST,
} transfer_request_type_t;

typedef struct transfer_request {
	transfer_request_type_t      RequestType;
	globus_gfs_operation_t       Operation;
	globus_gfs_transfer_info_t * TransferInfo;
	globus_off_t                 TransferLength;

	/* This lock controls the items below it. */
	globus_mutex_t     Lock;
	/* globus_mutex_t     Cond; */
	globus_result_t    Result;

	struct {
		globus_size_t      BufferLength;
		buffer_t         * FullBufferChain;
		buffer_t         * EmptyBufferChain;

		/* Indicate that someone is waiting on a specific offset. */
		globus_cond_t      Cond;
		globus_bool_t      ValidWaiter;
		globus_off_t       TransferOffset;
	} Buffers;
} transfer_request_t;

typedef struct pio_request {
	transfer_request_t * TransferRequest;

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
/* XXX detect this */
	api_config.Flags     =! API_USE_CONFIG;
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
 * GSI authentication. Authenticate to HPSS and save off session info
 * that we'll need later.
 */
static void
globus_l_gfs_hpss_module_session_start(
    globus_gfs_operation_t      Operation,
    globus_gfs_session_info_t * SessionInfo)
{
	char              * home_directory = NULL;
    globus_result_t     result         = GLOBUS_SUCCESS;
    globus_gfs_stat_t * stat_buf_array = NULL;
	int                 stat_buf_count = 0;

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
	result = globus_l_gfs_hpss_common_stat(home_directory,
	                                       GLOBUS_TRUE,  /* FileOnly        */
	                                       GLOBUS_FALSE, /* UseSymlinkInfo  */
	                                       GLOBUS_TRUE , /* IncludePathStat */
	                                       &stat_buf_array,
	                                       &stat_buf_count);
	if (result != GLOBUS_SUCCESS)
	{
		/* Make the error message a little more obvious. */
		result = GlobusGFSErrorWrapFailed("Attempt to find home directory", result);

		goto cleanup;
	}

	/* It exists, that's good enough for us. */
	globus_l_gfs_hpss_common_destroy_stat_array(stat_buf_array, stat_buf_count);

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
	/* session_handle_t * session_handle = NULL; */

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

static void
globus_i_gfs_hpss_module_init_buffers(transfer_request_t * TransferRequest,
	                                  globus_size_t        BufferLength)
{
	GlobusGFSName(globus_i_gfs_hpss_module_init_buffers);
	GlobusGFSHpssDebugEnter();

	globus_assert(TransferRequest->Buffers.FullBufferChain  == NULL);
	globus_assert(TransferRequest->Buffers.EmptyBufferChain == NULL);
	globus_assert(TransferRequest->Buffers.ValidWaiter      == GLOBUS_FALSE);

	/* Save off the length used for each buffer. */
	TransferRequest->Buffers.BufferLength = BufferLength;

	/* Initialize the condition. */
	globus_cond_init(&TransferRequest->Buffers.Cond, NULL);

	GlobusGFSHpssDebugExit();
}

static int
globus_i_gfs_hpss_module_allocate_buffer(transfer_request_t *  TransferRequest,
                                         char               ** Buffer)
{
	int retval = 0;

	GlobusGFSName(globus_i_gfs_hpss_module_allocate_buffer);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&TransferRequest->Lock);
	{
		/* Allocate the buffer. */
		*Buffer = (char *) globus_malloc(TransferRequest->Buffers.BufferLength);
		if (*Buffer == NULL)
		{
			/* Save the error. */
			if (TransferRequest->Result == GLOBUS_SUCCESS)
				TransferRequest->Result =  GlobusGFSErrorMemory("buffer");

			retval = 1;
		}
	}
	globus_mutex_unlock(&TransferRequest->Lock);

	if (retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return retval;
	}

	GlobusGFSHpssDebugExit();
	return 0;
}

static void
globus_i_gfs_hpss_module_deallocate_buffer(void * Buffer)
{
	GlobusGFSName(globus_i_gfs_hpss_module_deallocate_buffer);
	GlobusGFSHpssDebugEnter();

	if (Buffer != NULL)
		globus_free(Buffer);

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_module_destroy_buffers(transfer_request_t * TransferRequest)
{
	buffer_t * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_destroy_buffers);
	GlobusGFSHpssDebugEnter();

	/* Free the full chain. */
	while ((buffer_s = TransferRequest->Buffers.FullBufferChain) != NULL)
	{
		TransferRequest->Buffers.FullBufferChain = buffer_s->Next;
		globus_free(buffer_s->Data);
		globus_free(buffer_s);
	}

	/* Free the empty chain. */
	while ((buffer_s = TransferRequest->Buffers.EmptyBufferChain) != NULL)
	{
		TransferRequest->Buffers.EmptyBufferChain = buffer_s->Next;
		globus_free(buffer_s->Data);
		globus_free(buffer_s);
	}

	globus_cond_destroy(&TransferRequest->Buffers.Cond);

	GlobusGFSHpssDebugExit();
}

static int
globus_i_gfs_hpss_module_release_empty_buffer(transfer_request_t * TransferRequest,
                                              void               * Buffer)
{
	int        retval   = 1;
	buffer_t * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_release_empty_buffer);
	GlobusGFSHpssDebugEnter();

	/* Safety check in case we are called with a NULL buffer. */
	if (Buffer == NULL)
		goto cleanup;

	/* Now put it on the list. */
	globus_mutex_lock(&TransferRequest->Lock);
	{
		do {
			/*
			 * Allocate the buffer list entry.
			 */
			buffer_s = (buffer_t *) globus_calloc(1, sizeof(buffer_t));
			if (buffer_s == NULL)
			{
				if (TransferRequest->Result == GLOBUS_SUCCESS)
					TransferRequest->Result = GlobusGFSErrorMemory("buffer_t");
				break;
			}

			/* Save the buffer. */
			buffer_s->Data = Buffer;

			/* Put it on the empty list. */
			buffer_s->Next = TransferRequest->Buffers.EmptyBufferChain;
			if (TransferRequest->Buffers.EmptyBufferChain != NULL)
				TransferRequest->Buffers.EmptyBufferChain->Prev = buffer_s;
			TransferRequest->Buffers.EmptyBufferChain = buffer_s;

			/* Indicate success. */
			retval = 0;
		} while (0);
	}
	globus_mutex_unlock(&TransferRequest->Lock);
	
	if (retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return retval;
	}

cleanup:
	GlobusGFSHpssDebugExit();
	return 0;
}

static int
globus_i_gfs_hpss_module_get_emtpy_buffer(transfer_request_t *  TransferRequest,
                                          char               ** Buffer)
{
	int        retval   = 0;
	buffer_t * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_get_emtpy_buffer);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Buffer = NULL;

	globus_mutex_lock(&TransferRequest->Lock);
	{
		if (TransferRequest->Buffers.EmptyBufferChain != NULL)
		{
			/* Remove the next free buffer from the list. */
			buffer_s = TransferRequest->Buffers.EmptyBufferChain;
			TransferRequest->Buffers.EmptyBufferChain = buffer_s->Next;
			if (TransferRequest->Buffers.EmptyBufferChain != NULL)
				TransferRequest->Buffers.EmptyBufferChain->Prev = NULL;

			/* Save the buffer. */
			*Buffer = buffer_s->Data;

			/* Deallocate the list structure. */
			globus_free(buffer_s);
		}
	}
	globus_mutex_unlock(&TransferRequest->Lock);

	/* Allocate a new free buffer. */
	if (*Buffer == NULL)
		retval = globus_i_gfs_hpss_module_allocate_buffer(TransferRequest, Buffer);

	if (retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return retval;
	}

	GlobusGFSHpssDebugExit();
	return 0;
}

static int
globus_i_gfs_hpss_module_get_full_buffer(transfer_request_t *  TransferRequest,
                                         globus_off_t          TransferOffset,
                                         void               ** Buffer,
                                         globus_size_t      *  BufferLength)
{
	int        retval   = 0;
	buffer_t * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_get_full_buffer);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&TransferRequest->Lock);
	{
		do
		{
			/* Check if an error has occurred. */
			if (TransferRequest->Result != GLOBUS_SUCCESS)
			{
				retval = 1;
				break;
			}

			/* Search the full list for our buffer. */
			for (buffer_s  = TransferRequest->Buffers.FullBufferChain;
			     buffer_s != NULL && buffer_s->TransferOffset != TransferOffset;
			     buffer_s  = buffer_s->Next);

/* XXX just need to wake up if error occurs while waiting on a buffer. */
			if (buffer_s == NULL)
			{
				/* Wait for it. */
				TransferRequest->Buffers.ValidWaiter = GLOBUS_TRUE;
				{
					TransferRequest->Buffers.TransferOffset = TransferOffset;
					globus_cond_wait(&TransferRequest->Buffers.Cond,
					                 &TransferRequest->Lock);
				}
				TransferRequest->Buffers.ValidWaiter = GLOBUS_FALSE;
			}
		} while (buffer_s == NULL);

		/* Either we have a buffer or an error. */
		if (buffer_s != NULL)
		{
			/* Remove it from the list. */
			if (buffer_s->Prev != NULL)
				buffer_s->Prev->Next = buffer_s->Next;
			else
				TransferRequest->Buffers.FullBufferChain = buffer_s->Next;

			if (buffer_s->Next != NULL)
				buffer_s->Next->Prev = buffer_s->Prev;

			/* Save the buffer and the number of bytes it contains. */
			*Buffer       = buffer_s->Data;
			*BufferLength = buffer_s->DataLength;

			/* Deallocate the buffer. */
			globus_free(buffer_s);
		}
	}
	globus_mutex_unlock(&TransferRequest->Lock);

	GlobusGFSHpssDebugExit();
	return retval;
}

static int
globus_i_gfs_hpss_module_release_full_buffer(transfer_request_t * TransferRequest,
                                             void               * Buffer,
                                             globus_off_t         TransferOffset,
                                             globus_size_t        BufferLength)
{
	int        retval   = 1;
	buffer_t * buffer_s = NULL;

	GlobusGFSName(globus_i_gfs_hpss_module_release_full_buffer);
	GlobusGFSHpssDebugEnter();

	/* Safety check in case we are called with a NULL buffer. */
	if (Buffer == NULL)
		goto cleanup;

	/* Now put it on the list. */
	globus_mutex_lock(&TransferRequest->Lock);
	{
		do {
			/*
			 * Allocate the buffer list entry.
			 */
			buffer_s = (buffer_t *) globus_calloc(1, sizeof(buffer_t));
			if (buffer_s == NULL)
			{
				if (TransferRequest->Result == GLOBUS_SUCCESS)
					TransferRequest->Result = GlobusGFSErrorMemory("buffer_t");
				break;
			}

			/* Save the buffer. */
			buffer_s->Data = Buffer;
			buffer_s->TransferOffset = TransferOffset;
			buffer_s->DataLength     = BufferLength;

			/* Put it on the full list. */
			buffer_s->Next = TransferRequest->Buffers.FullBufferChain;
			if (TransferRequest->Buffers.FullBufferChain != NULL)
				TransferRequest->Buffers.FullBufferChain->Prev = buffer_s;
			TransferRequest->Buffers.FullBufferChain = buffer_s;

			/* If there is a valid waiter... */
			if (TransferRequest->Buffers.ValidWaiter == GLOBUS_TRUE)
			{
				/* If they are waiting for this offset... */
				if (TransferRequest->Buffers.TransferOffset == TransferOffset)
				{
					/* Wake the waiter. */
					globus_cond_signal(&TransferRequest->Buffers.Cond);
				}
			}

			/* Indicate success. */
			retval = 0;
		} while (0);
	}
	globus_mutex_unlock(&TransferRequest->Lock);
	
	if (retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return retval;
	}

cleanup:
	GlobusGFSHpssDebugExit();
	return 0;
}


/* Start our request. */
static globus_result_t 
globus_i_gfs_hpss_module_init_transfer_request(transfer_request_type_t      RequestType,
                                               globus_gfs_operation_t       Operation,
                                               globus_gfs_transfer_info_t * TransferInfo,
                                               transfer_request_t         * TransferRequest)
{
	globus_size_t       buffer_length = 0;
	globus_result_t     result        = GLOBUS_SUCCESS;
	globus_gfs_stat_t * statbuf_array = NULL;
	int                 statbuf_count = 0;

	GlobusGFSName(globus_i_gfs_hpss_module_init_transfer_request);
	GlobusGFSHpssDebugEnter();

	memset(TransferRequest, 0, sizeof(transfer_request_t));
	TransferRequest->RequestType  = RequestType;
	TransferRequest->Operation    = Operation;
	TransferRequest->TransferInfo = TransferInfo;

	globus_mutex_init(&TransferRequest->Lock, NULL);
	globus_cond_init(&TransferRequest->Buffers.Cond, NULL);
	TransferRequest->Result = GLOBUS_SUCCESS;

	/* Determine the GridFTP blocksize (used as the server's buffer size). */
	globus_gridftp_server_get_block_size(Operation, &buffer_length);

	/* True for STOR. */
	TransferRequest->TransferLength = TransferRequest->TransferInfo->alloc_size;

	if (RequestType == READ_REQUEST)
	{
		/* Use the given length. */
		TransferRequest->TransferLength = TransferRequest->TransferInfo->partial_length;

		/* If the length wasn't given... */
		if (TransferRequest->TransferInfo->partial_length == -1)
		{
			/* Stat the file. */
			result = globus_l_gfs_hpss_common_stat(TransferRequest->TransferInfo->pathname,
			                                       GLOBUS_TRUE,  /* FileOnly        */
			                                       GLOBUS_FALSE, /* UseSymlinkInfoi */
			                                       GLOBUS_TRUE,  /* IncludePathStat */
			                                       &statbuf_array,
			                                       &statbuf_count);
			if (result != GLOBUS_SUCCESS)
				goto cleanup;

			TransferRequest->TransferLength = statbuf_array[0].size;

			/* Adjust for partial offset transfers. */
			if (TransferRequest->TransferInfo->partial_offset != -1)
				TransferRequest->TransferLength -= TransferRequest->TransferInfo->partial_offset;

			globus_l_gfs_hpss_common_destroy_stat_array(statbuf_array, statbuf_count);
		}
	}

	/* Initialize the free & empty buffer chains. */
	globus_i_gfs_hpss_module_init_buffers(TransferRequest, buffer_length);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static void
globus_i_gfs_hpss_module_destroy_transfer_request(transfer_request_t * TransferRequest)
{
	GlobusGFSName(globus_i_gfs_hpss_module_destroy_transfer_request);
	GlobusGFSHpssDebugEnter();

	globus_mutex_destroy(&TransferRequest->Lock);

	/* Destroy the buffer chains. */
	globus_i_gfs_hpss_module_destroy_buffers(TransferRequest);

	GlobusGFSHpssDebugExit();
}

static globus_result_t
globus_i_gfs_hpss_module_init_pio(pio_request_t      * PIORequest,
                                  transfer_request_t * TransferRequest)
{
	int                     retval = 0;
	int                     flags  = 0;
	void                 *  buffer = NULL;
	unsigned int            buflen = 0;
	globus_result_t         result = GLOBUS_SUCCESS;
	hpss_cos_hints_t        hints_in;
	hpss_cos_hints_t        hints_out;
	hpss_pio_params_t       pio_params;
	hpss_cos_priorities_t   priorities;

	GlobusGFSName(globus_i_gfs_hpss_module_init_pio);
	GlobusGFSHpssDebugEnter();

	/*
	 * Initialize the pio request structure. 
	 */
	memset(PIORequest, 0, sizeof(pio_request_t));

	/* Store the transfer request. */
	PIORequest->TransferRequest = TransferRequest;

	/* Initialize the lock and condition */
	globus_mutex_init(&PIORequest->Lock, NULL);
	globus_cond_init(&PIORequest->Cond, NULL);

	/* Initialize the hints in. */
	memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

	if (TransferRequest->RequestType == WRITE_REQUEST)
	{
		CONVERT_LONGLONG_TO_U64(TransferRequest->TransferInfo->alloc_size,hints_in.MinFileSize);
		CONVERT_LONGLONG_TO_U64(TransferRequest->TransferInfo->alloc_size,hints_in.MaxFileSize);
	}


/* XXX remove this for release. */
/*
strncpy(hints_in.COSName, "GRIDFTP_DSI_TEST", HPSS_MAX_OBJECT_NAME);
*/

	/* Initialize the hints out. */
	memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the priorities. */
	memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

	if (TransferRequest->RequestType == WRITE_REQUEST)
	{
		priorities.MinFileSizePriority = LOWEST_PRIORITY;
		priorities.MaxFileSizePriority = LOWEST_PRIORITY;
	}

/*
priorities.COSNamePriority = REQUIRED_PRIORITY;
*/

	/* Set the open flags. */
	flags = O_RDWR;

	if (TransferRequest->RequestType == WRITE_REQUEST)
		flags |= O_CREAT;

	if (TransferRequest->TransferInfo->truncate == GLOBUS_TRUE)
		flags |= O_TRUNC;

	/*
	 * Open the HPSS file. We need the file's stripe width before we call
	 * hpss_PIOStart().
	 */
	PIORequest->Coordinator.FileFD = hpss_Open(TransferRequest->TransferInfo->pathname,
	                                           flags,
	                                           S_IRUSR|S_IWUSR,
	                                           &hints_in,    /* Hints In      */
	                                           &priorities,  /* Priorities In */
	                                           &hints_out);  /* Hints Out     */
	if (PIORequest->Coordinator.FileFD < 0)
	{
        result = GlobusGFSErrorSystemError("hpss_Open", -PIORequest->Coordinator.FileFD);
		goto cleanup;
	}

	/*
	 * Now call hpss_PIOStart() to generate the stripe group will need for both
	 * hpss_PIOExecute() and hpss_PIORegister().
	 */
	pio_params.Operation       = HPSS_PIO_WRITE;
	if (TransferRequest->RequestType == READ_REQUEST)
		pio_params.Operation   = HPSS_PIO_READ;

	pio_params.ClntStripeWidth = 1;
	pio_params.BlockSize       = TransferRequest->Buffers.BufferLength;
	pio_params.FileStripeWidth = hints_out.StripeWidth;
	pio_params.IOTimeOutSecs   = 0;
	pio_params.Transport       = HPSS_PIO_MVR_SELECT;
	/* Don't use HPSS_PIO_HANDLE_GAP, it's bugged in HPSS 7.4. */
	pio_params.Options         = 0;

	/* Now call the start routine. */
	retval = hpss_PIOStart(&pio_params, &PIORequest->Coordinator.StripeGroup);
	if (retval != 0)
	{
        result = GlobusGFSErrorSystemError("hpss_PIOStart", -retval);
		goto cleanup;
	}

	/*
	 * Copy the stripe group for the participant.
	 */
	retval = hpss_PIOExportGrp(PIORequest->Coordinator.StripeGroup,
	                           &buffer,
	                           &buflen);
	if (retval != 0)
	{
        result = GlobusGFSErrorSystemError("hpss_PIOExportGrp", -retval);
		goto cleanup;
	}

	retval = hpss_PIOImportGrp(buffer, buflen, &PIORequest->Participant.StripeGroup);
	if (retval != 0)
	{
        result = GlobusGFSErrorSystemError("hpss_PIOImportGrp", -retval);
		goto cleanup;
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return result;
}

static void
globus_i_gfs_hpss_module_destroy_pio(pio_request_t * PIORequest)
{
    GlobusGFSName(globus_i_gfs_hpss_module_destroy_pio);
    GlobusGFSHpssDebugEnter();

	/*
	 * Perform all cleanup on this structure.
	 */
	if (PIORequest->Coordinator.FileFD >= 0)
		hpss_Close(PIORequest->Coordinator.FileFD);

	globus_mutex_destroy(&PIORequest->Lock);
	globus_cond_destroy(&PIORequest->Cond);

	/* 
	 * XXX Make sure to prevent bug 1999, nothing can
	 * prevent the seqeuence PIOImport->PIORegister->PIOEnd. Calling
	 * PIOImport->PIOEnd will cause a segfault.
	 */

	GlobusGFSHpssDebugExit();
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
		retval = globus_i_gfs_hpss_module_get_emtpy_buffer(pio_request->TransferRequest,
		                                                   &empty_buffer);
		if (retval != 0)
			goto cleanup;

		/* Copy the buffer. */
		memcpy(empty_buffer, *Buffer, *BufferLength);

		/* Release our full buffer. */
		retval = globus_i_gfs_hpss_module_release_full_buffer(pio_request->TransferRequest,
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
	int             retval          = 0;
	pio_request_t * pio_request     = NULL;
	globus_off_t    transfer_offset = 0;
	globus_size_t   buffer_length   = 0;

    GlobusGFSName(globus_i_gfs_hpss_module_pio_register_write_callback);
    GlobusGFSHpssDebugEnter();

	/* Make sure we received our arg. */
	globus_assert(UserArg != NULL);

	/* Cast our arg. */
	pio_request = (pio_request_t *) UserArg;

	/* Convert the transfer offset */
	CONVERT_U64_TO_LONGLONG(TransferOffset, transfer_offset);

	/* Release our free buffer. */
	retval = globus_i_gfs_hpss_module_release_empty_buffer(pio_request->TransferRequest, *Buffer);
	if (retval != 0)
		goto cleanup;

	/* Release our reference to the buffer. */
	*Buffer = NULL;

	/* Get the full buffer @ transfer_offset. */
	retval = globus_i_gfs_hpss_module_get_full_buffer(pio_request->TransferRequest,
	                                                  transfer_offset,
	                                                  Buffer,
	                                                  &buffer_length);

	/* Copy out the buffer length. */
	*BufferLength = buffer_length;

cleanup:
	/* On error... */
	if (retval != 0)
	{
		/* Deallocate our buffer. */
		if (*Buffer != NULL)
			globus_i_gfs_hpss_module_deallocate_buffer(*Buffer);
		*Buffer = NULL;
	}
	GlobusGFSHpssDebugExit();

	/*
	 * Notice that retval maybe non zero if we succeed but something else
	 * in the transfer failed.
	 */
	return retval;
}

static void *
globus_i_gfs_hpss_module_pio_register(void * Arg)
{
	int               retval      = 0;
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
	retval = globus_i_gfs_hpss_module_allocate_buffer(pio_request->TransferRequest, &buffer);
	if (retval != 0)
		goto cleanup;

/* BufferLength must be > (hints?) BlockSize */
	/* Now register this participant. */
	retval = hpss_PIORegister(
	             0,              /* Stripe element.      */
	             0,              /* DataNetAddr (Unused) */
	             buffer,         /* Buffer               */
	             pio_request->TransferRequest->Buffers.BufferLength,
	             pio_request->Participant.StripeGroup,
	             pio_request->TransferRequest->RequestType == WRITE_REQUEST ?
	              globus_i_gfs_hpss_module_pio_register_write_callback:
	              globus_i_gfs_hpss_module_pio_register_read_callback,
	             pio_request);

	/* If an error occurred... */
	if (retval != 0)
	{
		globus_mutex_lock(&pio_request->TransferRequest->Lock);
		{
			/* Record it. */
			if (pio_request->TransferRequest->Result == GLOBUS_SUCCESS)
			{
				pio_request->TransferRequest->Result = GlobusGFSErrorSystemError("hpss_PIORegister", -retval);
			}
		}
		globus_mutex_unlock(&pio_request->TransferRequest->Lock);
	}

cleanup:
	/*
	 * Deallocate the buffer. On a write request, PIORegister() doesn't pass the buffer to
	 * the callback so we must deallocate it. On a read operation, there's a bug in 
	 * hpss_PIORegister() which forces us to copy the data out instead of exchanging buffers.
	 */
	globus_i_gfs_hpss_module_deallocate_buffer(buffer);

	globus_mutex_lock(&pio_request->Lock);
	{
		/* Record that we have exitted. */
		pio_request->ParticHasExitted = GLOBUS_TRUE;

		/* Wake anyone that is waiting. */
		globus_cond_signal(&pio_request->Cond);
	}
	globus_mutex_unlock(&pio_request->Lock);

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

	CONVERT_LONGLONG_TO_U64(pio_request->TransferRequest->TransferInfo->partial_offset, transfer_offset);
	if (pio_request->TransferRequest->TransferInfo->partial_offset == -1)
		transfer_offset = cast64m(0);
	CONVERT_LONGLONG_TO_U64(pio_request->TransferRequest->TransferLength, transfer_length);

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

	/* If an error occurred... */
	if (retval != 0)
	{
		globus_mutex_lock(&pio_request->TransferRequest->Lock);
		{
			/* Record it. */
			if (pio_request->TransferRequest->Result == GLOBUS_SUCCESS)
			{
				pio_request->TransferRequest->Result = GlobusGFSErrorSystemError("hpss_PIOExecute", -retval);
			}
		}
		globus_mutex_unlock(&pio_request->TransferRequest->Lock);
	}

	globus_mutex_lock(&pio_request->Lock);
	{
		/* Record that we have exitted. */
		pio_request->CoordHasExitted = GLOBUS_TRUE;

		/* Wake anyone that is waiting. */
		globus_cond_signal(&pio_request->Cond);
	}
	globus_mutex_unlock(&pio_request->Lock);

	GlobusGFSHpssDebugExit();
	return NULL;
}

static globus_result_t
globus_i_gfs_hpss_module_pio_begin(pio_request_t      * PIORequest,
                                   transfer_request_t * TransferRequest)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;
    globus_thread_t thread;

    GlobusGFSName(globus_i_gfs_hpss_module_pio_begin);
    GlobusGFSHpssDebugEnter();

	/*
	 * Initialize the PIO structures.
	 */
	result = globus_i_gfs_hpss_module_init_pio(PIORequest, TransferRequest);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Launch the pio register thread. */
	retval = globus_thread_create(&thread,
	                              NULL,
	                              globus_i_gfs_hpss_module_pio_register,
	                              PIORequest);
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
	                              PIORequest);
	if (retval != 0)
	{
		/* Translate the error. */
        result = GlobusGFSErrorSystemError("globus_thread_create", retval);
        goto cleanup;
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

    GlobusGFSHpssDebugExit();
    return result;
}

static void
globus_i_gfs_hpss_module_pio_end(pio_request_t * PIORequest)
{
	GlobusGFSName(globus_gfs_operation_t);
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
	}
	globus_mutex_unlock(&PIORequest->Lock);

	globus_i_gfs_hpss_module_destroy_pio(PIORequest);

	GlobusGFSHpssDebugExit();
}

typedef struct gridftp_request {
	transfer_request_t * TransferRequest;

	/* This lock controls access to the variables below it. */
	globus_mutex_t Lock;
	globus_cond_t  Cond;
	globus_bool_t  Eof;
	int            OpCount; /* Number of outstanding read/writes */
} gridftp_request_t;

static globus_result_t
globus_i_gfs_hpss_module_gridftp_init_request(gridftp_request_t  * GridFTPRequest,
                                              transfer_request_t * TransferRequest)
{
	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_init_request);
	GlobusGFSHpssDebugEnter();

	/* Initialize our request structure. */
	memset(GridFTPRequest, 0, sizeof(gridftp_request_t));

	GridFTPRequest->TransferRequest = TransferRequest;
	globus_mutex_init(&GridFTPRequest->Lock, NULL);
	globus_cond_init(&GridFTPRequest->Cond, NULL);
	GridFTPRequest->Eof     = GLOBUS_FALSE;
	GridFTPRequest->OpCount = 0;

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
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
	int                 retval          = 0;
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
		if (Length == 0)
			retval = globus_i_gfs_hpss_module_release_empty_buffer(
			                                  gridftp_request->TransferRequest,
		                                      Buffer);
		else
			retval = globus_i_gfs_hpss_module_release_full_buffer(
		                                  	gridftp_request->TransferRequest,
	                                      	Buffer,
	                                      	TransferOffset,
	                                      	Length);
	}

	/* On error, deallocate the buffer */
	if (Result != GLOBUS_SUCCESS || retval != 0)
		globus_i_gfs_hpss_module_deallocate_buffer(Buffer);

	globus_mutex_lock(&gridftp_request->TransferRequest->Lock);
	{
		if (Result != GLOBUS_SUCCESS)
		{
			/* Record the error */
			if (gridftp_request->TransferRequest->Result == GLOBUS_SUCCESS)
				gridftp_request->TransferRequest->Result = Result;
		}
	}
	globus_mutex_unlock(&gridftp_request->TransferRequest->Lock);

	globus_mutex_lock(&gridftp_request->Lock);
	{
		/* Record EOF. */
		if (Eof == GLOBUS_TRUE)
			gridftp_request->Eof = Eof;

		/* Decrement the op count. */
		gridftp_request->OpCount--;

		/* Wake anyone that may be waiting. */
		if (gridftp_request->OpCount == 0)
			globus_cond_signal(&gridftp_request->Cond);
	}
	globus_mutex_unlock(&gridftp_request->Lock);

	if (Result != GLOBUS_SUCCESS || retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

/* Don't return the error, we will post it. */
static void
globus_i_gfs_hpss_module_gridftp_read(transfer_request_t * TransferRequest)
{
	int                 retval         = 0;
	int                 ops_needed     = 0;
	char              * buffer         = NULL;
	globus_bool_t       eof            = GLOBUS_FALSE;
	globus_bool_t       error_occurred = GLOBUS_FALSE;
	globus_result_t     result         = GLOBUS_SUCCESS;
	gridftp_request_t   gridftp_request;

	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_read);
	GlobusGFSHpssDebugEnter();

	/* Initialize our request structure. */
	result = globus_i_gfs_hpss_module_gridftp_init_request(&gridftp_request, TransferRequest);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	while (result == GLOBUS_SUCCESS && retval == 0 && eof == GLOBUS_FALSE)
	{
		/* Check if an error has occurred. */
		globus_mutex_lock(&TransferRequest->Lock);
		{
			error_occurred = (TransferRequest->Result != GLOBUS_SUCCESS);
		}
		globus_mutex_unlock(&TransferRequest->Lock);

		if (error_occurred == GLOBUS_TRUE)
			break;

		globus_mutex_lock(&gridftp_request.Lock);
		{
			/* Record whether we have received EOF. */
			eof = gridftp_request.Eof;

			if (eof == GLOBUS_FALSE)
			{
				do
				{
					/*
					 * Calculate the number of read operations to submit.
					 */

					/* Start with the optimal number. */
					globus_gridftp_server_get_optimal_concurrency(TransferRequest->Operation, &ops_needed);

					/* Subtract the number of current operations. */
					ops_needed -= gridftp_request.OpCount;

/* XXX need throttle control on buffer allocations. */
					/* Submit the buffers. */
					for (; ops_needed > 0; ops_needed--)
					{
						retval = globus_i_gfs_hpss_module_get_emtpy_buffer(TransferRequest, &buffer);
						if (retval != 0)
							break;

						/* Now register the read operation. */
						result = globus_gridftp_server_register_read(
						             TransferRequest->Operation,
						             (globus_byte_t *)buffer,
						             TransferRequest->Buffers.BufferLength,
						             globus_i_gfs_hpss_module_gridftp_read_callback,
						             &gridftp_request);
						if (result != GLOBUS_SUCCESS)
							break;

						/* Release our buffer handle. */
						buffer = NULL;

						/* Increment OpCount. */
						gridftp_request.OpCount++;
					}

					/* If no errors occurred... */
					if (retval == 0 && result == GLOBUS_SUCCESS)
					{
						/* Wait for one of the callbacks to wake us. */
						globus_cond_wait(&gridftp_request.Cond, &gridftp_request.Lock);
					}
				} while (0);
			}
		}
		globus_mutex_unlock(&gridftp_request.Lock);
	}

	globus_mutex_lock(&gridftp_request.Lock);
	{
		/* Wait for all operations to complete. */
		while (gridftp_request.OpCount != 0)
		{
			globus_cond_wait(&gridftp_request.Cond, &gridftp_request.Lock);
		}
	}
	globus_mutex_unlock(&gridftp_request.Lock);

	/* Clean up the request memory. */
	globus_i_gfs_hpss_module_gridftp_destroy_request(&gridftp_request);

cleanup:
	/* If we still holding a buffer... */
	if (buffer != NULL)
		globus_i_gfs_hpss_module_deallocate_buffer(buffer);

	if (result != GLOBUS_SUCCESS)
	{
		/* Post the error so others know to shutdown. */
		globus_mutex_lock(&TransferRequest->Lock);
		{
			if (TransferRequest->Result == GLOBUS_SUCCESS)
				TransferRequest->Result = result;
		}
		globus_mutex_unlock(&TransferRequest->Lock);
	}

	if (result != GLOBUS_SUCCESS || retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
	return;
}

static void
globus_i_gfs_hpss_module_gridftp_write_callback(
    globus_gfs_operation_t   Operation,
    globus_result_t          Result,
    globus_byte_t          * Buffer,
    globus_size_t            Length,
    void                   * UserArg)
{
	int                 retval          = 0;
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
		retval = globus_i_gfs_hpss_module_release_empty_buffer(
		                                  gridftp_request->TransferRequest,
	                                      Buffer);
	}

	/* On error, deallocate the buffer */
	if (Result != GLOBUS_SUCCESS || retval != 0)
		globus_i_gfs_hpss_module_deallocate_buffer(Buffer);

	globus_mutex_lock(&gridftp_request->TransferRequest->Lock);
	{
		if (Result != GLOBUS_SUCCESS)
		{
			/* Record the error */
			if (gridftp_request->TransferRequest->Result == GLOBUS_SUCCESS)
				gridftp_request->TransferRequest->Result = Result;
		}
	}
	globus_mutex_unlock(&gridftp_request->TransferRequest->Lock);

	globus_mutex_lock(&gridftp_request->Lock);
	{
		/* Decrement the op count. */
		gridftp_request->OpCount--;

		/* Wake anyone that may be waiting. */
		if (gridftp_request->OpCount == 0)
			globus_cond_signal(&gridftp_request->Cond);
	}
	globus_mutex_unlock(&gridftp_request->Lock);

	if (Result != GLOBUS_SUCCESS || retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}
static void
globus_i_gfs_hpss_module_gridftp_write(transfer_request_t * TransferRequest)
{
	int                 retval          = 0;
	int                 ops_needed      = 0;
	char              * buffer          = NULL;
	globus_bool_t       eof             = GLOBUS_FALSE;
	globus_bool_t       error_occurred  = GLOBUS_FALSE;
	globus_result_t     result          = GLOBUS_SUCCESS;
	gridftp_request_t   gridftp_request;
	globus_off_t        transfer_offset = 0;
	globus_size_t       buffer_length   = 0;

	GlobusGFSName(globus_i_gfs_hpss_module_gridftp_write);
	GlobusGFSHpssDebugEnter();

	/* Initialize our request structure. */
	result = globus_i_gfs_hpss_module_gridftp_init_request(&gridftp_request, TransferRequest);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	while (result == GLOBUS_SUCCESS && retval == 0 && eof == GLOBUS_FALSE)
	{
		/* Check if an error has occurred. */
		globus_mutex_lock(&TransferRequest->Lock);
		{
			error_occurred = (TransferRequest->Result != GLOBUS_SUCCESS);
		}
		globus_mutex_unlock(&TransferRequest->Lock);

		if (error_occurred == GLOBUS_TRUE)
			break;

		globus_mutex_lock(&gridftp_request.Lock);
		{
			do
			{
				/*
				 * Calculate the max number of write operations to submit.
				 */

				/* Start with the optimal number. */
				globus_gridftp_server_get_optimal_concurrency(TransferRequest->Operation, &ops_needed);

				/* Subtract the number of current operations. */
				ops_needed -= gridftp_request.OpCount;

/* XXX need throttle control on buffer allocations. */
				/* Submit the buffers. */
				for (; ops_needed > 0; ops_needed--)
				{
					/* Get the buffer at the current offset. */
					retval = globus_i_gfs_hpss_module_get_full_buffer(TransferRequest, 
					                                                  transfer_offset,
					                                                  (void **)&buffer,
					                                                  &buffer_length);
					if (retval != 0)
						break;

					/* Now register the write operation. */
					result = globus_gridftp_server_register_write(
					             TransferRequest->Operation,
					             (globus_byte_t *)buffer,
					             buffer_length,
					             transfer_offset,
					             0,
					             globus_i_gfs_hpss_module_gridftp_write_callback,
					             &gridftp_request);

					if (result != GLOBUS_SUCCESS)
						break;

					/* Release our buffer handle. */
					buffer = NULL;

					/* Increment OpCount. */
					gridftp_request.OpCount++;

					/* Increment the transfer offset. */
					transfer_offset += buffer_length;

					/* Check if the transfer is done. */
					if (transfer_offset == TransferRequest->TransferLength)
					{
						eof = GLOBUS_TRUE;
						break;
					}
				}

				if (eof == GLOBUS_TRUE)
					break;

				/* If no errors occurred... */
				if (retval == 0 && result == GLOBUS_SUCCESS)
				{
					/* Wait for one of the callbacks to wake us. */
					globus_cond_wait(&gridftp_request.Cond, &gridftp_request.Lock);
				}
			} while (0);
		}
		globus_mutex_unlock(&gridftp_request.Lock);
	}

	globus_mutex_lock(&gridftp_request.Lock);
	{
		/* Wait for all operations to complete. */
		while (gridftp_request.OpCount != 0)
		{
			globus_cond_wait(&gridftp_request.Cond, &gridftp_request.Lock);
		}
	}
	globus_mutex_unlock(&gridftp_request.Lock);

	/* Clean up the request memory. */
	globus_i_gfs_hpss_module_gridftp_destroy_request(&gridftp_request);

cleanup:
	/* If we still holding a buffer... */
	if (buffer != NULL)
		globus_i_gfs_hpss_module_deallocate_buffer(buffer);

	if (result != GLOBUS_SUCCESS)
	{
		/* Post the error so others know to shutdown. */
		globus_mutex_lock(&TransferRequest->Lock);
		{
			if (TransferRequest->Result == GLOBUS_SUCCESS)
				TransferRequest->Result = result;
		}
		globus_mutex_unlock(&TransferRequest->Lock);
	}

	if (result != GLOBUS_SUCCESS || retval != 0)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
	return;
}

/*
 * RETR
 */
static void
globus_l_gfs_hpss_module_send(
    globus_gfs_operation_t       Operation,
    globus_gfs_transfer_info_t * TransferInfo,
    void                       * UserArg)
{
	globus_result_t    result      = GLOBUS_SUCCESS;
	pio_request_t      pio_request;
	transfer_request_t transfer_request;

	GlobusGFSName(globus_l_gfs_hpss_module_send);
	GlobusGFSHpssDebugEnter();

    /* Inform the server that we are starting. */
	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	/* Initialize our transfer request structure. */
	result = globus_i_gfs_hpss_module_init_transfer_request(READ_REQUEST,
	                                                        Operation, 
	                                                        TransferInfo,
	                                                        &transfer_request);
	if (result != GLOBUS_SUCCESS)
		goto finish_transfer;

	/* Launch the PIO transfer. */
	result = globus_i_gfs_hpss_module_pio_begin(&pio_request, &transfer_request);
	if (result != GLOBUS_SUCCESS)
		goto destroy_transfer_request;

	/* Fire off the GridFTP write callbacks. */
	globus_i_gfs_hpss_module_gridftp_write(&transfer_request);

	/* End the PIO request. */
	globus_i_gfs_hpss_module_pio_end(&pio_request);

destroy_transfer_request:

	if (result == GLOBUS_SUCCESS)
		result = transfer_request.Result;

	/* Destroy our request. */
	globus_i_gfs_hpss_module_destroy_transfer_request(&transfer_request);

finish_transfer:
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
	globus_result_t    result      = GLOBUS_SUCCESS;
	pio_request_t      pio_request;
	transfer_request_t transfer_request;

	GlobusGFSName(globus_l_gfs_hpss_module_recv);
	GlobusGFSHpssDebugEnter();

    /* Inform the server that we are starting. */
	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	/* Initialize our transfer request structure. */
	result = globus_i_gfs_hpss_module_init_transfer_request(WRITE_REQUEST,
	                                                        Operation, 
	                                                        TransferInfo,
	                                                        &transfer_request);
	if (result != GLOBUS_SUCCESS)
		goto finish_transfer;

	/* Launch the PIO transfer. */
	result = globus_i_gfs_hpss_module_pio_begin(&pio_request, &transfer_request);
	if (result != GLOBUS_SUCCESS)
		goto destroy_transfer_request;

	/* Fire off the GridFTP read callbacks. */
	globus_i_gfs_hpss_module_gridftp_read(&transfer_request);

	/* End the PIO request. */
	globus_i_gfs_hpss_module_pio_end(&pio_request);

destroy_transfer_request:

	if (result == GLOBUS_SUCCESS)
		result = transfer_request.Result;

	/* Destroy our request. */
	globus_i_gfs_hpss_module_destroy_transfer_request(&transfer_request);

finish_transfer:
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
    globus_gfs_stat_t * stat_buf_array = NULL;
	int                 stat_buf_count = 0;

	GlobusGFSName(globus_l_gfs_hpss_module_chgrp);
	GlobusGFSHpssDebugEnter();

	/* Stat it, make sure it exists. */
	result = globus_l_gfs_hpss_common_stat(Pathname,
	                                       GLOBUS_TRUE,  /* FileOnly        */
	                                       GLOBUS_FALSE, /* UseSymlinkInfo  */
	                                       GLOBUS_TRUE,  /* IncludePathStat */
	                                       &stat_buf_array,
	                                       &stat_buf_count);
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
	retval = hpss_Chown(Pathname, stat_buf_array[0].uid, gid);
	if (retval != 0)
	{
       	result = GlobusGFSErrorSystemError("hpss_Chgrp", -retval);
		goto cleanup;
	}

cleanup:
	/* clean up the statbuf. */
	globus_l_gfs_hpss_common_destroy_stat_array(stat_buf_array, stat_buf_count);

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
	globus_result_t     result        = GLOBUS_SUCCESS;
	globus_gfs_stat_t * statbuf_array = NULL;
	int                 statbuf_count = 0;

	GlobusGFSName(globus_i_gfs_hpss_module_stat);
	GlobusGFSHpssDebugEnter();

	/* Make sure Arg was provided. */
	/* globus_assert(Arg != NULL); */


	result = globus_l_gfs_hpss_common_stat(StatInfo->pathname,
	                                       StatInfo->file_only,
	                                       StatInfo->use_symlink_info,
	                                       StatInfo->include_path_stat,
	                                       &statbuf_array,
	                                       &statbuf_count);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Inform the server that we are done. */
	globus_gridftp_server_finished_stat(Operation, 
	                                    result, 
	                                    statbuf_array, 
	                                    statbuf_count);

	/* Destroy the statbuf_array. */
	globus_l_gfs_hpss_common_destroy_stat_array(statbuf_array, statbuf_count);

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
	NULL, /* globus_l_gfs_hpss_module_list, */
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

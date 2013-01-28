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
#include <openssl/md5.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_hash.h>
#include <hpss_api.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_checksum.h"
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_misc.h"

struct checksum {
	MD5_CTX                   MD5Context;
	buffer_handle_t         * BufferHandle;
	buffer_priv_id_t          PrivateBufferID;
	checksum_eof_callback_t   EofCallbackFunc;
	void                    * EofCallbackArg;
	checksum_buffer_pass_t    BufferPassFunc;
	void                    * BufferPassArg;

	globus_mutex_t            Lock;
	globus_cond_t             Cond;
	globus_result_t           Result;
	globus_bool_t             Stop;
	globus_off_t              NextOffset;
	int                       ThreadCount;
};

globus_result_t
checksum_init(buffer_handle_t        *  BufferHandle,
              checksum_eof_callback_t   EofCallbackFunc,
              void                   *  EofCallbackArg,
              checksum_t             ** Checksum)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*Checksum = (checksum_t *) globus_calloc(1, sizeof(checksum_t));
	if (*Checksum == NULL)
	{
		result = GlobusGFSErrorMemory("checksum_t");
		goto cleanup;
	}

	retval = MD5_Init(&(*Checksum)->MD5Context);
	if (retval == 0)
	{
		result = GlobusGFSErrorGeneric("Failed to create MD5 context");
		goto cleanup;
	}

	(*Checksum)->BufferHandle    = BufferHandle;
	(*Checksum)->EofCallbackFunc = EofCallbackFunc;
	(*Checksum)->EofCallbackArg  = EofCallbackArg;
	(*Checksum)->Result          = GLOBUS_SUCCESS;
	(*Checksum)->Stop            = GLOBUS_FALSE;
	(*Checksum)->NextOffset      = 0;
	(*Checksum)->ThreadCount     = 0;
	(*Checksum)->PrivateBufferID = buffer_create_private_list(BufferHandle);

	globus_mutex_init(&(*Checksum)->Lock, NULL);
	globus_cond_init(&(*Checksum)->Cond, NULL);
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
checksum_set_buffer_pass_func(checksum_t            * Checksum,
                              checksum_buffer_pass_t  BufferPassFunc,
                              void                  * BufferPassArg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	Checksum->BufferPassFunc = BufferPassFunc;
	Checksum->BufferPassArg  = BufferPassArg;

	GlobusGFSHpssDebugExit();
}

void
checksum_destroy(checksum_t * Checksum)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Checksum != NULL)
	{
		globus_mutex_destroy(&Checksum->Lock);
		globus_cond_destroy(&Checksum->Cond);
		globus_free(Checksum);
	}

	GlobusGFSHpssDebugExit();
}

void
checksum_buffer(void         * CallbackArg,
                char         * Buffer,
                globus_off_t   Offset,
                globus_off_t   Length)
{
	int             retval        = 0;
	globus_bool_t   call_callback = GLOBUS_FALSE;
	globus_bool_t   stop          = GLOBUS_FALSE;
	globus_result_t result        = GLOBUS_SUCCESS;
	checksum_t    * checksum      = (checksum_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Put the buffer on our the ready list. */
	buffer_store_ready_buffer(checksum->BufferHandle,
	                          checksum->PrivateBufferID,
	                          Buffer,
	                          Offset,
	                          Length);

	globus_mutex_lock(&checksum->Lock);
	{
		/* Indicate that we are here. */
		checksum->ThreadCount++;

		/* Save the error. */
		result = checksum->Result;

		/* Save the stop flag. */
		stop = checksum->Stop;

		/* Save the next offset. */
		Offset = checksum->NextOffset;
	}
	globus_mutex_unlock(&checksum->Lock);

	while (GLOBUS_TRUE)
	{
		if (result != GLOBUS_SUCCESS)
			break;

		if (stop == GLOBUS_TRUE)
			break;

		Buffer = NULL;

		/* Try to get the next ready buffer. */
		buffer_get_ready_buffer(checksum->BufferHandle,
		                        checksum->PrivateBufferID,
		                        &Buffer, 
		                        Offset, 
		                        &Length);

		if (Buffer == NULL)
			break;

		retval = MD5_Update(&checksum->MD5Context, Buffer, Length);
		if (retval == 0)
		{
			globus_mutex_lock(&checksum->Lock);
			{
				if (checksum->Result == GLOBUS_SUCCESS)
				{
					result = GlobusGFSErrorSystemError("hpss_HashAppendBuf", -retval);
					checksum->Result = result;
					call_callback = GLOBUS_TRUE;
				}
			}
			globus_mutex_unlock(&checksum->Lock);

			/* Inform the upper layer. */
			if (call_callback == GLOBUS_TRUE)
				checksum->EofCallbackFunc(checksum->EofCallbackArg, result);
				                          
			break;
		}

		/* Pass the buffer forward. */
		checksum->BufferPassFunc(checksum->BufferPassArg, Buffer, Offset, Length);

		globus_mutex_lock(&checksum->Lock);
		{
			/* Move the next offset forward. */
			Offset = checksum->NextOffset += Length;
		}
		globus_mutex_unlock(&checksum->Lock);
	}

	globus_mutex_lock(&checksum->Lock);
	{
		/* Indicate that we are leaving. */
		checksum->ThreadCount--;

		/* Check if we should wake someone. */
		if (checksum->ThreadCount == 0)
		{
			globus_cond_signal(&checksum->Cond);
		}
	}
	globus_mutex_unlock(&checksum->Lock);

	GlobusGFSHpssDebugExit();
}

/* May need to call the hashing function ourselves b/c of gaps on 
   error after flusing started.
 */
/*
 * Called after lower layers have been flushed. We should not be
 * recieving any new buffers.
 */
void
checksum_flush(checksum_t * Checksum)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Checksum != NULL)
	{
		globus_mutex_lock(&Checksum->Lock);
		{
			while (buffer_get_ready_buffer_count(Checksum->BufferHandle,
			                                     Checksum->PrivateBufferID) > 0)
			{
				if (Checksum->Result != GLOBUS_SUCCESS)
					break;

				globus_cond_wait(&Checksum->Cond, &Checksum->Lock);
			}
		}
		globus_mutex_unlock(&Checksum->Lock);
	}

	GlobusGFSHpssDebugExit();
/* If we are here w/o error, we have a valid checksum. */
}

/*
 * The important thing is that once we set the stop flag, our buffer
 * pass function should not be passing buffers, although it may
 * continue to receive buffers.
 */
void
checksum_stop(checksum_t * Checksum)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Checksum != NULL)
	{
		globus_mutex_lock(&Checksum->Lock);
		{
			Checksum->Stop = GLOBUS_TRUE;

			while (Checksum->ThreadCount > 0)
			{
				globus_cond_wait(&Checksum->Cond, &Checksum->Lock);
			}
		}
		globus_mutex_unlock(&Checksum->Lock);
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
checksum_get_sum(checksum_t * Checksum, char ** ChecksumString)
{
	int             index  = 0;
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;
	unsigned char   md5_digest[MD5_DIGEST_LENGTH];

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	*ChecksumString = (char *) malloc(2*MD5_DIGEST_LENGTH + 1);

	retval = MD5_Final(md5_digest, &Checksum->MD5Context);
globus_assert(retval == 1);
	for (index = 0; index < MD5_DIGEST_LENGTH; index++)
	{
		sprintf(&((*ChecksumString)[index*2]), "%02x", (unsigned int)md5_digest[index]);
	}

	GlobusGFSHpssDebugExit();
	return result;
}

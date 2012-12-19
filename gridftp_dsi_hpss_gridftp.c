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
 * Globus includes.
 */
#include <globus_gridftp_server.h>
#include <globus_thread.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_transfer_data.h"
#include "gridftp_dsi_hpss_gridftp.h"
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_msg.h"

#ifdef DMALLOC
/*
 * Dmalloc
 */
#include <dmalloc.h>
#endif /* DMALLOC */

struct gridftp {
	gridftp_op_type_t        OpType;
	globus_gfs_operation_t   Operation;
	buffer_handle_t        * BufferHandle;
	buffer_priv_id_t         PrivateBufferID;
	gridftp_eof_callback_t   EofCallbackFunc;
	void                   * EofCallbackArg;
	gridftp_buffer_pass_t    BufferPassFunc;
	void                   * BufferPassArg;
	msg_handle_t           * MsgHandle;
	globus_off_t             OffsetAdjustment;

	globus_mutex_t           Lock;
	globus_cond_t            Cond;
	globus_bool_t            Eof;
	globus_bool_t            Stop;
	globus_bool_t            EofCallbackCalled;
	globus_result_t          Result;
	int                      OpCount;
};

static void
gridftp_register_read(gridftp_t     * GridFTP,
                      globus_bool_t   FromCallback);

static void
gridftp_register_write(gridftp_t * GridFTP);

static void
gridftp_record_error(gridftp_t * GridFTP, globus_result_t Result);

static void
gridftp_decrement_op_count(gridftp_t * GridFTP);

static void
gridftp_record_eof(gridftp_t * GridFTP, globus_bool_t Eof);

globus_result_t
gridftp_init(gridftp_op_type_t             OpType,
	         globus_gfs_operation_t        Operation,
	         globus_gfs_transfer_info_t *  TransferInfo,
             buffer_handle_t            *  BufferHandle,
             msg_handle_t               *  MsgHandle,
             gridftp_eof_callback_t        EofCallbackFunc,
             void                       *  EofCallbackArg,
             gridftp_t                  ** GridFTP)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*GridFTP = (gridftp_t *) globus_calloc(1, sizeof(gridftp_t));
	if (*GridFTP == NULL)
	{
		result = GlobusGFSErrorMemory("gridftp_t");
		goto cleanup;
	}

	/* Initialize the entries. */
	(*GridFTP)->OpType            = OpType;
	(*GridFTP)->Operation         = Operation;
	(*GridFTP)->BufferHandle      = BufferHandle;
	(*GridFTP)->MsgHandle         = MsgHandle;
	(*GridFTP)->OffsetAdjustment  = TransferInfo->partial_offset;
	(*GridFTP)->EofCallbackFunc   = EofCallbackFunc;
	(*GridFTP)->EofCallbackArg    = EofCallbackArg;
	(*GridFTP)->EofCallbackCalled = GLOBUS_FALSE;
	(*GridFTP)->Eof               = GLOBUS_FALSE;
	(*GridFTP)->Stop              = GLOBUS_FALSE;
	(*GridFTP)->Result            = GLOBUS_SUCCESS;
	(*GridFTP)->OpCount           = 0;
	(*GridFTP)->PrivateBufferID   = buffer_create_private_list(BufferHandle);

	globus_mutex_init(&(*GridFTP)->Lock, NULL);
	globus_cond_init(&(*GridFTP)->Cond, NULL);


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
gridftp_set_buffer_pass_func(gridftp_t              * GridFTP,
                              gridftp_buffer_pass_t   BufferPassFunc,
                              void                  * BufferPassArg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	GridFTP->BufferPassFunc = BufferPassFunc;
	GridFTP->BufferPassArg  = BufferPassArg;

	GlobusGFSHpssDebugExit();
}

void
gridftp_destroy(gridftp_t * GridFTP)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (GridFTP != NULL)
	{
		globus_mutex_destroy(&GridFTP->Lock);
		globus_cond_destroy(&GridFTP->Cond);
		globus_free(GridFTP);
	}

	GlobusGFSHpssDebugExit();
}

void
gridftp_buffer(void         * CallbackArg,
               char         * Buffer,
               globus_off_t   Offset,
               globus_off_t   Length)
{
	gridftp_t * gridftp = (gridftp_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Mark this buffer as ours. */
	buffer_set_private_id(gridftp->BufferHandle, 
	                      gridftp->PrivateBufferID, 
	                      Buffer);

	switch (gridftp->OpType)
	{
	case GRIDFTP_OP_TYPE_STOR:
		/* Store this free buffer. */
		buffer_store_free_buffer(gridftp->BufferHandle,
		                         gridftp->PrivateBufferID,
		                         Buffer);

		/* Fire off more reads. */
		gridftp_register_read(gridftp, GLOBUS_FALSE);

		break;

	case GRIDFTP_OP_TYPE_RETR:
		/* Store this ready buffer. */
		buffer_store_ready_buffer(gridftp->BufferHandle,
		                          gridftp->PrivateBufferID,
		                          Buffer,
		                          Offset,
		                          Length);

		/* Fire off more writes. */
		gridftp_register_write(gridftp);

		break;
	}

	GlobusGFSHpssDebugExit();
}

void
gridftp_flush(gridftp_t * GridFTP)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (GridFTP == NULL)
		goto cleanup;

	globus_mutex_lock(&GridFTP->Lock);
	{
		/*
		 * On STOR, we were the ones that kicked out EOF so we
		 * have nothing to do. On RETR, we need to send all
		 * ready buffers.
		 */

		/* Set Eof so (for RETR) so that we will get signaled. */
		GridFTP->Eof = GLOBUS_TRUE;

		while (buffer_get_ready_buffer_count(GridFTP->BufferHandle,
		                                     GridFTP->PrivateBufferID) > 0)
		{
			globus_cond_wait(&GridFTP->Cond, &GridFTP->Lock);
		}

		/* Wait for all ops to complete. */
		while (GridFTP->OpCount != 0)
		{
			globus_cond_wait(&GridFTP->Cond, &GridFTP->Lock);
		}
	}
	globus_mutex_unlock(&GridFTP->Lock);

cleanup:
	GlobusGFSHpssDebugExit();
}

void
gridftp_stop(gridftp_t * GridFTP)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (GridFTP == NULL)
		goto cleanup;

	globus_mutex_lock(&GridFTP->Lock);
	{
		/* Indicate full stop. */
		GridFTP->Stop = GLOBUS_TRUE;

		/* Wait for all ops to complete. */
		while (GridFTP->OpCount != 0)
		{
			globus_cond_wait(&GridFTP->Cond, &GridFTP->Lock);
		}
	}
	globus_mutex_unlock(&GridFTP->Lock);

cleanup:
	GlobusGFSHpssDebugExit();
}

static void
gridftp_register_read_callback(globus_gfs_operation_t   Operation,
                               globus_result_t          Result,
                               globus_byte_t *          Buffer,
                               globus_size_t            BytesRead,
                               globus_off_t             TransferOffset,
                               globus_bool_t            Eof,
                               void                   * CallbackArg)
{
	gridftp_t * gridftp = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Make sure we got our callback arg. */
	globus_assert(CallbackArg != NULL);
	/* Cast to our handle. */
	gridftp = (gridftp_t *) CallbackArg;

	/* Pass the buffer forward. */
	if (Result == GLOBUS_SUCCESS && BytesRead > 0)
	{
		/* Mark the buffer as ready. */
		buffer_set_buffer_ready(gridftp->BufferHandle,
		                        gridftp->PrivateBufferID,
		                        (char *)Buffer,
		                        TransferOffset + gridftp->OffsetAdjustment,
		                        BytesRead);

		/* Now pass the buffer forward. */
		gridftp->BufferPassFunc(gridftp->BufferPassArg, 
		                       (char*)Buffer, 
		                       TransferOffset + gridftp->OffsetAdjustment, 
		                       BytesRead);
	}

	/* Record any error we might have had. */
	gridftp_record_error(gridftp, Result);

	/* Record eof. */
	gridftp_record_eof(gridftp, Eof);

	/* Fire off more reads. */
	gridftp_register_read(gridftp, GLOBUS_TRUE);

	/* Decrement the current op count. */
	gridftp_decrement_op_count(gridftp);

	GlobusGFSHpssDebugExit();
}

static void
gridftp_register_read(gridftp_t     * GridFTP,
                      globus_bool_t   FromCallback)
{
	int             opt_count = 0;
	char          * buffer    = NULL;
	globus_off_t    length    = 0;
	globus_result_t result    = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&GridFTP->Lock);
	{
		if (GridFTP->Result != GLOBUS_SUCCESS)
			goto unlock;
		if (GridFTP->Eof == GLOBUS_TRUE)
			goto unlock;
		if (GridFTP->Stop == GLOBUS_TRUE)
			goto unlock;

		/* Until we can find a real fix... */
		if (FromCallback == GLOBUS_FALSE && GridFTP->OpCount > 0)
			goto unlock;

		if (FromCallback == GLOBUS_TRUE && GridFTP->OpCount > 1)
			goto unlock;

		/* Get the optimal concurrency count. */
		globus_gridftp_server_get_optimal_concurrency(GridFTP->Operation, &opt_count);

		while (opt_count >= GridFTP->OpCount)
		{
			/* Get a free buffer. */
			buffer_get_free_buffer(GridFTP->BufferHandle,
			                       GridFTP->PrivateBufferID,
			                       &buffer,
			                       &length);
			if (buffer == NULL)
				break;

			/* Register this read. */
			result = globus_gridftp_server_register_read(GridFTP->Operation,
			                                             (globus_byte_t*)buffer,
			                                             length,
			                                             gridftp_register_read_callback,
			                                             GridFTP);
			if (result != GLOBUS_SUCCESS)
				break;

			/* Success! Increment the op count. */
			GridFTP->OpCount++;
		}
	}
unlock:
	globus_mutex_unlock(&GridFTP->Lock);

	/* Record any error we might have had. */
	gridftp_record_error(GridFTP, result);

	GlobusGFSHpssDebugExit();
}

void
gridftp_register_write_callback(globus_gfs_operation_t   Operation,
                                globus_result_t          Result,
                                globus_byte_t          * Buffer,
                                globus_size_t            BytesWritten,
                                void                   * CallbackArg)
{
	gridftp_t * gridftp = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Make sure we got our callback arg. */
	globus_assert(CallbackArg != NULL);
	/* Cast to our handle. */
	gridftp = (gridftp_t *) CallbackArg;

	/* Pass the buffer forward. */
	if (Result == GLOBUS_SUCCESS)
		gridftp->BufferPassFunc(gridftp->BufferPassArg, (char *)Buffer, 0, 0);

	/* Record any error we might have had. */
	gridftp_record_error(gridftp, Result);

	/* Register more writes. */
	gridftp_register_write(gridftp);

	/* Decrement the current op count. */
	gridftp_decrement_op_count(gridftp);

	GlobusGFSHpssDebugExit();
}

static void
gridftp_register_write(gridftp_t * GridFTP)
{
	int               opt_count = 0;
	char            * buffer    = NULL;
	globus_off_t      offset    = 0;
	globus_off_t      length    = 0;
	globus_result_t   result    = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&GridFTP->Lock);
	{
		/*
		 * Don't check EOF here, just keep sending until we run out of
		 * free buffers.
		 */
		if (GridFTP->Result != GLOBUS_SUCCESS)
			goto unlock;
		if (GridFTP->Stop == GLOBUS_TRUE)
			goto unlock;

		/* Get the optimal concurrency count. */
		globus_gridftp_server_get_optimal_concurrency(GridFTP->Operation, &opt_count);

		while (opt_count >= GridFTP->OpCount)
		{
			/* Get the next ready buffer. */
			buffer_get_next_ready_buffer(GridFTP->BufferHandle,
			                             GridFTP->PrivateBufferID,
			                             &buffer,
			                             &offset,
			                             &length);

			if (buffer == NULL)
				break;

			/* Register this write. */
			result = globus_gridftp_server_register_write(GridFTP->Operation,
			                                              (globus_byte_t*)buffer,
			                                              length,
			                                              offset-GridFTP->OffsetAdjustment,
			                                              0, /* Stripe index. */
			                                              gridftp_register_write_callback,
			                                              GridFTP);

			if (result != GLOBUS_SUCCESS)
				break;

			/* Success! Increment the op count. */
			GridFTP->OpCount++;
		}
	}
unlock:
	globus_mutex_unlock(&GridFTP->Lock);

	/* Record any error we might have had. */
	gridftp_record_error(GridFTP, result);

	GlobusGFSHpssDebugExit();
}

static void
gridftp_record_error(gridftp_t       * GridFTP, 
                     globus_result_t   Result)
{
	globus_bool_t   call_callback = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Result != GLOBUS_SUCCESS)
	{
		globus_mutex_lock(&GridFTP->Lock);
		{
			if (GridFTP->Result == GLOBUS_SUCCESS)
			{
				/* Record it. */
				GridFTP->Result = Result;

				/* Call the callback. */
				if (GridFTP->EofCallbackCalled == GLOBUS_FALSE)
					call_callback = GLOBUS_TRUE;

				/* Indicate that we have/will called the eof callback. */
				GridFTP->EofCallbackCalled = GLOBUS_TRUE;

				/* On error we should wake people. */
				globus_cond_broadcast(&GridFTP->Cond);
			}
		}
		globus_mutex_unlock(&GridFTP->Lock);
	}

	/* Inform the caller. */
	if (call_callback == GLOBUS_TRUE)
		GridFTP->EofCallbackFunc(GridFTP->EofCallbackArg, Result);

	GlobusGFSHpssDebugExit();
}

static void
gridftp_decrement_op_count(gridftp_t * GridFTP)
{
	globus_bool_t call_callback = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&GridFTP->Lock);
	{
		if (GridFTP->OpCount > 1)
		{
			/*
			 * We are not even the last current thread.
			 */

			/* Decrement the op count. */
			GridFTP->OpCount--;
		} else if (GridFTP->Stop   != GLOBUS_TRUE    &&
			       GridFTP->Result == GLOBUS_SUCCESS &&
			       GridFTP->Eof    != GLOBUS_TRUE)
		{
			/*
			 * We are the last current thread but we expect more.
			 */

			/* Decrement the op count. */
			GridFTP->OpCount--;
		} else if (GridFTP->EofCallbackCalled == GLOBUS_TRUE)
		{
			/*
			 * We are the absolute last thread but someone already
			 * called the callback.
			 */

			/* Decrement the op count. */
			GridFTP->OpCount--;

			/* Wake anyone waiting on our op count. */
			globus_cond_broadcast(&GridFTP->Cond);
		} else
		{
			/*
			 * We are the absolute last thread and no one has
			 * called the callback.
			 */

			/* Call the callback. */
			call_callback = GLOBUS_TRUE;

			/* Make sure no one else calls. */
			GridFTP->EofCallbackCalled = GLOBUS_TRUE;
		}
	}
	globus_mutex_unlock(&GridFTP->Lock);

	if (call_callback == GLOBUS_TRUE)
	{
		/* Call the callback. */
		GridFTP->EofCallbackFunc(GridFTP->EofCallbackArg, GLOBUS_SUCCESS);

		globus_mutex_lock(&GridFTP->Lock);
		{
			/* Decrement the op count. */
			GridFTP->OpCount--;

			/* Wake anyone waiting on our op count. */
			globus_cond_broadcast(&GridFTP->Cond);
		}
		globus_mutex_unlock(&GridFTP->Lock);
	}

	GlobusGFSHpssDebugExit();
}

static void
gridftp_record_eof(gridftp_t     * GridFTP,
                   globus_bool_t   Eof)
{
	globus_bool_t call_callback = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Eof == GLOBUS_TRUE)
	{
		globus_mutex_lock(&GridFTP->Lock);
		{
			if (GridFTP->Eof == GLOBUS_FALSE)
			{
				/* Record it. */
				GridFTP->Eof = GLOBUS_TRUE;

				/* Call the callback. */
				if (GridFTP->EofCallbackCalled == GLOBUS_FALSE)
					call_callback = GLOBUS_TRUE;

				/* Indicate that we will call the eof callback. */
				GridFTP->EofCallbackCalled = GLOBUS_TRUE;

				/* On error we should wake people. */
				globus_cond_broadcast(&GridFTP->Cond);
			}
		}
		globus_mutex_unlock(&GridFTP->Lock);
	}

	/* Inform the caller. */
	if (call_callback == GLOBUS_TRUE)
		GridFTP->EofCallbackFunc(GridFTP->EofCallbackArg, GLOBUS_SUCCESS);

	GlobusGFSHpssDebugExit();
}

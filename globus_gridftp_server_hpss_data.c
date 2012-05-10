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
/* include <globus_debug.h> */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_api.h>
#include <u_signed64.h>
/*include <hpss_net.h> */

/*
 * Local includes.
 */
#include "globus_gridftp_server_hpss_common.h"
#include "version.h"

/*
 * This is used to define the debug print statements.
 */
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);

typedef struct buffer {
	uint64_t         TransferOffset;
	uint32_t         BufferLength;
	char           * Data;
	struct buffer  * Next;
	struct buffer  * Prev;
	struct request * Request;
} buffer_t;

typedef struct {
	struct request {
		globus_result_t          Result;
		globus_mutex_t           Mutex;
		globus_cond_t            Cond;
		globus_bool_t            Eof;
		globus_size_t            BlockSize;
		globus_gfs_operation_t   Operation;
		int                      MaxBufferCount;
		int                      CurrentBufferCount;
		int                      CurrentGridFtpRequests;

		globus_cond_t            FreeListCond;
		buffer_t               * FreeList;
		globus_cond_t            ReadyListCond;
		buffer_t               * ReadyList;

		/*
		 * Request-specific elements.
		 */
		/* This is maintained by the stat callback */
		struct {
			char         * ReceivedBuffer;
			unsigned int   BufferLength;
		} StatArray;

		/* This is maintained by the send/recv callbacks */
		struct {
			char         * ReceivedBuffer;
			unsigned int   BufferLength;
		} PioStripeGroup;

		/*
		 * Info passed for all PIORegister().
		 */
		struct {
			globus_bool_t Exitted;
		    int (*Callback) (void         *  UserArg,
		                     u_signed64      TransferOffset,
		                     unsigned int *  BufferLength,
		                     void         ** DataBuffer);
		} PioRegister;

	} Request;
} session_handle_t;

static globus_result_t
globus_i_gfs_hpss_data_allocate_buffer(struct request *  Request,
                                       buffer_t       ** Buffer)
{
	GlobusGFSName(globus_i_gfs_hpss_data_allocate_buffer);
	GlobusGFSHpssDebugEnter();

	*Buffer = (buffer_t *) globus_calloc(1, sizeof(buffer_t));
	if (*Buffer == NULL)
		return GlobusGFSErrorMemory("buffer_t");

	(*Buffer)->Data = globus_calloc(1, Request->BlockSize);
	if ((*Buffer)->Data == NULL)
	{
		globus_free(*Buffer);
		return GlobusGFSErrorMemory("buffer_t");
	}

	(*Buffer)->TransferOffset = 0;
	(*Buffer)->BufferLength   = Request->BlockSize;
	(*Buffer)->Next           = NULL;
	(*Buffer)->Prev           = NULL;

	/* Add a pointer to the request. */
	(*Buffer)->Request = Request;

    GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static void
globus_i_gfs_hpss_data_destroy_buffer(buffer_t * Buffer)
{
	GlobusGFSName(globus_i_gfs_hpss_data_destroy_buffer);
	GlobusGFSHpssDebugEnter();

	if (Buffer == NULL)
		return;

	if (Buffer->Data != NULL)
		globus_free(Buffer->Data);

	globus_free(Buffer);

    GlobusGFSHpssDebugExit();
}

/*
 * Called locked.
 */

static void
globus_i_gfs_hpss_data_get_ready_buffer(struct request *  Request,
                                        struct buffer  ** Buffer,
                                        uint64_t          TransferOffset)
{
	GlobusGFSName(globus_i_gfs_hpss_data_get_ready_buffer);
	GlobusGFSHpssDebugEnter();

	/* Initialize our return value. */
	*Buffer = NULL;

	/* Look for a match on the ready list. */
	for (*Buffer  = Request->ReadyList;
	     *Buffer != NULL;
	     *Buffer  = (*Buffer)->Next)
	{
		if ((*Buffer)->TransferOffset == TransferOffset)
		{
			/* Remove it from the ready list. */
			if ((*Buffer)->Next != NULL)
				(*Buffer)->Next->Prev = (*Buffer)->Prev;
			if ((*Buffer)->Prev != NULL)
				(*Buffer)->Prev->Next = (*Buffer)->Next;
			else
				Request->ReadyList = (*Buffer)->Next;

			(*Buffer)->Next = NULL;
			(*Buffer)->Prev = NULL;

			/* Add a pointer to the request. */
			(*Buffer)->Request = Request;

			/* Break out of the loop. */
			break;
		}
	}

    GlobusGFSHpssDebugExit();
}


/*
 * Called locked.
 */
static void
globus_i_gfs_hpss_data_get_any_ready_buffer(struct request *  Request,
                                            struct buffer  ** Buffer)
{
	GlobusGFSName(globus_i_gfs_hpss_data_get_any_ready_buffer);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Buffer = NULL;

	/* Take the first request from the ready list. */
	if (Request->ReadyList != NULL)
	{
		*Buffer = Request->ReadyList;
		Request->ReadyList = Request->ReadyList->Next;
		if (Request->ReadyList != NULL)
			Request->ReadyList->Prev = NULL;

		(*Buffer)->Next = NULL;
		(*Buffer)->Prev = NULL;

		/* Add a pointer to the request. */
		(*Buffer)->Request = Request;

	}

    GlobusGFSHpssDebugExit();
}

/*
 * Called locked. Put this request on the ready list.
 */
static void
globus_i_gfs_hpss_data_release_ready_buffer(struct request * Request,
                                            struct buffer  * Buffer,
                                            uint64_t         TransferOffset,
                                            uint32_t         BufferLength)
{
	GlobusGFSName(globus_i_gfs_hpss_data_release_ready_buffer);
	GlobusGFSHpssDebugEnter();

	/* Initialize this buffer. */
	Buffer->TransferOffset = TransferOffset;
	Buffer->BufferLength   = BufferLength;

	/* Put it on the front of the ready list. */
	Buffer->Prev = NULL;
	Buffer->Next = Request->ReadyList;
	if (Request->ReadyList != NULL)
		Request->ReadyList->Prev = Buffer;
	Request->ReadyList = Buffer;

	/* Delete the pointer to the request. */
	Buffer->Request = NULL;

    GlobusGFSHpssDebugExit();
}

/*
 * Request may be NULL on return. Called locked
 */
static globus_result_t
globus_i_gfs_hpss_data_get_free_buffer(struct request *  Request,
                                       struct buffer  ** Buffer)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(globus_i_gfs_hpss_data_get_free_buffer);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return. */
	*Buffer = NULL;

	/* If there are requests on the free list... */
	if (Request->FreeList != NULL)
	{
		/* Get the head of the free list. */
		*Buffer = Request->FreeList;
		Request->FreeList = Request->FreeList->Next;
		if (Request->FreeList != NULL)
			Request->FreeList->Prev = NULL;

		(*Buffer)->Next = NULL;
		(*Buffer)->Prev = NULL;
	}

	/* If the free list was empty. */
	if (*Buffer == NULL)
	{
		/* If we haven't hit our request limit... */
		if (Request->MaxBufferCount > Request->CurrentBufferCount)
		{
			result = globus_i_gfs_hpss_data_allocate_buffer(Request, Buffer);
			if (result != GLOBUS_SUCCESS)
				goto cleanup;

			/* Incrementn the current buffer count. */
			Request->CurrentBufferCount++;
		}
	}

	/* If we have a request... */
	if (*Buffer != NULL)
	{
		/* Initialize it. */
		(*Buffer)->TransferOffset = 0;
		(*Buffer)->BufferLength   = Request->BlockSize;
		(*Buffer)->Request        = Request;
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
 * Called locked
 */
static void
globus_i_gfs_hpss_data_release_free_buffer(struct request * Request,
                                           struct buffer  * Buffer)
{
	GlobusGFSName(globus_i_gfs_hpss_data_release_free_buffer);
	GlobusGFSHpssDebugEnter();

	/* Put this buffer on the free list. */
	Buffer->Prev = NULL;
	Buffer->Next = Request->FreeList;
	if (Request->FreeList != NULL)
		Request->FreeList->Prev = Buffer;
	Request->FreeList = Buffer;

	/* Delete the pointer to the request. */
	Buffer->Request = NULL;

	GlobusGFSHpssDebugExit();
}


static void
globus_i_gfs_hpss_data_init_request(struct request * Request)
{
	GlobusGFSName(globus_i_gfs_hpss_data_init_request);
	GlobusGFSHpssDebugEnter();

	memset(Request, 0, sizeof(struct request));

	globus_mutex_init(&Request->Mutex, NULL);
	globus_cond_init(&Request->Cond, NULL);

    GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_data_start_request(struct request         * Request,
                                     globus_gfs_operation_t   Operation)
{
	GlobusGFSName(globus_i_gfs_hpss_data_start_request);
	GlobusGFSHpssDebugEnter();

	Request->Result                 = GLOBUS_SUCCESS;
	Request->Eof                    = FALSE;
	Request->Operation              = Operation;
	Request->CurrentBufferCount     = 0;
	Request->CurrentGridFtpRequests = 0;
	Request->FreeList               = NULL;
	Request->ReadyList              = NULL;

	/*
	 * The buffer's size is based on this BlockSize value. When we call
	 * hpss_PIOStart(), we tell it the size of the blocks each data node
	 * is responsible for. Our BlockSize here must match that value. So if
	 * you change the calculation of BlockSize here, change it in the
	 * control
	 * DSI too in the call to hpss_PIOStart().
	 */
	globus_gridftp_server_get_stripe_block_size(Operation,
	                                            &Request->BlockSize);
	globus_gridftp_server_get_optimal_concurrency(Operation,
	                                              &Request->MaxBufferCount);

	/* Reset the HPSS PIO entries. */
	Request->PioRegister.Exitted  = GLOBUS_FALSE;
	Request->PioRegister.Callback = NULL;

    GlobusGFSHpssDebugExit();
}

/*
 * Destroy the buffers between transfer requests because the buffer
 * sizes may be different between requests.
 */
static void
globus_i_gfs_hpss_data_end_request(struct request * Request)
{
	struct buffer * buffer = NULL;

	GlobusGFSName(globus_i_gfs_hpss_data_end_request);
	GlobusGFSHpssDebugEnter();

	/* Free the free list */
	while ((buffer = Request->FreeList) != NULL)
	{
		/* Unlink the buffer. */
		Request->FreeList = buffer->Next;

		/* Deallocate its memory. */
		globus_i_gfs_hpss_data_destroy_buffer(buffer);
	}

	/* Free the ready list */
	while ((buffer = Request->ReadyList) != NULL)
	{
		/* Unlink the buffer. */
		Request->ReadyList = buffer->Next;

		/* Deallocate its memory. */
		globus_i_gfs_hpss_data_destroy_buffer(buffer);
	}

	/* Delete the received buffer. */
/*
	if (Request->U.Send.ReceivedBuffer != NULL)
		globus_free(Request->U.Send.ReceivedBuffer);
	Request->U.Send.ReceivedBuffer = NULL;
	Request->U.Send.BufferLength   = 0;
XXX
*/

	Request->Result                 = GLOBUS_SUCCESS;
	Request->Eof                    = FALSE;
	Request->Operation              = NULL;
	Request->CurrentBufferCount     = 0;
	Request->CurrentGridFtpRequests = 0;
	Request->FreeList               = NULL;
	Request->ReadyList              = NULL;

    GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_data_destroy_request(struct request * Request)
{
	GlobusGFSName(globus_i_gfs_hpss_data_destroy_request);
	GlobusGFSHpssDebugEnter();

	/* Make sure the request was ended. */
	globus_i_gfs_hpss_data_end_request(Request);

	globus_mutex_destroy(&Request->Mutex);
	globus_cond_destroy(&Request->Cond);

    GlobusGFSHpssDebugExit();
}

static int
globus_i_gfs_hpss_data_hpss_read_callback(
    void         *  UserArg,
    u_signed64      TransferOffset,
    unsigned int *  BufferLength,
    void         ** DataBuffer)
{
	int               retval       = 0;
	globus_result_t   result       = GLOBUS_SUCCESS;
	struct request  * request      = NULL;
	struct buffer   * free_buffer  = NULL;
	struct buffer   * ready_buffer = NULL;
uint64_t transfer_offset = 0;

	GlobusGFSName(globus_i_gfs_hpss_data_hpss_read_callback);
	GlobusGFSHpssDebugEnter();

	/* Make sure we received our user arg. */
	globus_assert(UserArg != NULL);
	/* Cast to our ready buffer. */
	ready_buffer = (struct buffer *) UserArg;
	/* Get a shortcut to our request */
	request = ready_buffer->Request;

	/* Check this assumption */
	globus_assert(*DataBuffer == ready_buffer->Data);

	/*
	 * hpss_PIORegister() has a bug, if you call hpss_PIOExecute() more
	 * than once, the same buffer pointer is returned to this callback on
	 * the first call after each hpss_PIOExecute(). But after the very
	 * first time this callback is called, that buffer can not be trusted.
	 * So we do not change out buffers, instead we copy between buffers.
	 */

	globus_mutex_lock(&request->Mutex);
	{
		while (free_buffer == NULL)
		{
			/* Check if an error has occurred. */
			if (request->Result != GLOBUS_SUCCESS)
			{
				/* Change our return value to indicate failure. */
				retval = -1;

				/* Break out of the loop. */
				break;
			}

			/* Get a free buffer. */
			result = globus_i_gfs_hpss_data_get_free_buffer(request,
			                                                &free_buffer);
			if (result != GLOBUS_SUCCESS)
			{
				/* Save our error. */
				if (request->Result == GLOBUS_SUCCESS)
					request->Result = result;

				/* Change our return value to indicate failure. */
				retval = -1;

				/* Break out of the loop. */
				break;
			}

			if (free_buffer != NULL)
			{
				/* Copy out of our buffer. */
				memcpy(free_buffer->Data, 
				       ready_buffer->Data, 
				       *BufferLength);

				/* Release this 'ready' buffer. */
				/* Temporary 32bit fix. */
				CONVERT_U64_TO_LONGLONG(TransferOffset, transfer_offset);
				globus_i_gfs_hpss_data_release_ready_buffer(request,
				                                            free_buffer,
				                                            transfer_offset,
				                                            *BufferLength);

				/* Delete our reference to that buffer. */
				free_buffer = NULL;

				break;
			}

			/* Wait for a buffer to arrive. */
			globus_cond_wait(&request->Cond, &request->Mutex);
		}

		/* Wake anyone that is waiting. */
		globus_cond_broadcast(&request->Cond);
	}
	globus_mutex_unlock(&request->Mutex);

	if (retval != 0)
	{
    	GlobusGFSHpssDebugExitWithError();
		return retval;
	}

    GlobusGFSHpssDebugExit();
	return 0;
}

/* On first call, *DataBuffer is NULL */
static int
globus_i_gfs_hpss_data_hpss_write_callback(
    void         *  UserArg,
    u_signed64      TransferOffset,
    unsigned int *  BufferLength,
    void         ** DataBuffer)
{
	int               retval       = 0;
	struct request  * request      = NULL;
	struct buffer   * free_buffer  = NULL;
	struct buffer   * ready_buffer = NULL;
uint64_t transfer_offset = 0;

	GlobusGFSName(globus_i_gfs_hpss_data_hpss_write_callback);
	GlobusGFSHpssDebugEnter();

	/* Make sure we received our user arg. */
	globus_assert(UserArg != NULL);
	/* Cast to our free buffer. */
	free_buffer = (struct buffer *) UserArg;
	/* Get a shortcut to our request. */
	request = free_buffer->Request;

	/* Check this assumption. */
	globus_assert(*DataBuffer == NULL || free_buffer->Data == *DataBuffer);

	/*
	 * hpss_PIORegister() has a bug, if you call hpss_PIOExecute() more
	 * than once, the same buffer pointer is returned to this callback on
	 * the first call after each hpss_PIOExecute(). But after the very
	 * first time this callback is called, that buffer can not be trusted.
	 * So we do not change out buffers, instead we copy between buffers.
	 */
	globus_mutex_lock(&request->Mutex);
	{
		while (TRUE)
		{
			/* Check if an error has occurred. */
			if (request->Result != GLOBUS_SUCCESS)
			{
				/* Change our return value to indicate failure. */
				retval = -1;

				/* Break out of the loop. */
				break;
			}

			/* Get a ready request @ this offset. */
			CONVERT_U64_TO_LONGLONG(TransferOffset, transfer_offset);
			globus_i_gfs_hpss_data_get_ready_buffer(request,
			                                        &ready_buffer,
			                                        transfer_offset);

			if (ready_buffer != NULL)
			{
				/* Copy into our free buffer. */
				memcpy(free_buffer->Data, 
				       ready_buffer->Data, 
				       ready_buffer->BufferLength);

				/* Record the length of this buffer. */
				*BufferLength = ready_buffer->BufferLength;

				/* Release the 'ready' buffer. */
				globus_i_gfs_hpss_data_release_free_buffer(request,
				                                           ready_buffer);

				/* Delete our pointer to the ready buffer. */
				ready_buffer = NULL;

				/* Point DataBuffer to our buffer. */
				*DataBuffer = free_buffer->Data;

				break;
			}

			/*
			 * If there are no ready buffers and EOF has occurred,
			 * then we have a serious problem.
			 */
			if (request->Eof == GLOBUS_TRUE)
			{
				/*
				 * Record the error, we know we are not over riding
				 * a previous error because of the check at the top
				 * of this loop.
				 */
				request->Result = GlobusGFSErrorGeneric(
				                    "Hpss Pio Write Callback after EOF");
				/* Indicate failure */
				retval = -1;

				/* Breakout of the loop. */
				break;
			}

			/* Wait. */
			globus_cond_wait(&request->Cond, &request->Mutex);
		}

		/* Wake anyone that is waiting. */
		globus_cond_broadcast(&request->Cond);
	}
	globus_mutex_unlock(&request->Mutex);

	if (retval != 0)
	{
    	GlobusGFSHpssDebugExitWithError();
		return retval;
	}

    GlobusGFSHpssDebugExit();
	return 0;
}

/*
 * New thread.
 */
static void *
globus_i_gfs_hpss_data_pio_register(void * UserArg)
{
	int               retval       = 0;
	buffer_t        * buffer       = NULL;
	struct request  * request      = NULL;
	hpss_pio_grp_t    stripe_group = NULL;
	globus_result_t   result       = GLOBUS_SUCCESS;

	GlobusGFSName(globus_i_gfs_hpss_data_pio_register);
	GlobusGFSHpssDebugEnter();

	/* Make sure we got our callback arg. */
	globus_assert(UserArg != NULL);
	/* Cast it to our request. */
	request = (struct request *) UserArg;

	/* Allocate a new buffer for PIORegister. */
	result = globus_i_gfs_hpss_data_allocate_buffer(request, &buffer);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Import the stripe group. */
	retval = hpss_PIOImportGrp(request->PioStripeGroup.ReceivedBuffer,
	                           request->PioStripeGroup.BufferLength,
	                           &stripe_group);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_PIOImportGrp", -retval);
		goto cleanup;
	}

	/*
	 * Now fire off PIORegister. All the magic happens in its callback.
	 *
	 * PIORegister requires a data sock address BUT it doesn't use it!
	 * Instead, it uses the value of HPSS_API_HOSTNAME in the
	 * environment (or set in /var/hpss/etc/env.conf) which defaults to
	 * the systems name.
	 */
	retval = hpss_PIORegister(
	             0,                    /* Stripe element.      */
	             0,                    /* DataNetAddr (Unused) */
	             buffer->Data,         /* Buffer               */
	             buffer->BufferLength, /* Buffer Length        */
	             stripe_group,
	             request->PioRegister.Callback,
	             buffer);

	if (retval != 0)
		result = GlobusGFSErrorSystemError("hpss_PIORegister", -retval);

cleanup:
	/* Deallocate the buffer. */
	globus_i_gfs_hpss_data_destroy_buffer(buffer);

	if (stripe_group != NULL)
	{
/* It is not always safe to call this. Depends on where PIORegister fails */
		retval = hpss_PIOEnd(stripe_group);
		if (retval != 0 && result == GLOBUS_SUCCESS)
			result = GlobusGFSErrorSystemError("hpss_PIOEnd", -retval);
	}

	globus_mutex_lock(&request->Mutex);
	{
		/* Inform the caller that we are exitting. */
		request->PioRegister.Exitted = GLOBUS_TRUE;

		/* Save our error. */
		if (request->Result == GLOBUS_SUCCESS)
			request->Result = result;

		/* Wake the caller. */
		globus_cond_broadcast(&request->Cond);
	}
	globus_mutex_unlock(&request->Mutex);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return NULL;
	}

	GlobusGFSHpssDebugExit();
	return NULL;
}

static void
globus_i_gfs_hpss_data_pio_begin(
    struct request * Request,
    int (*Callback) (void         *  UserArg,
                     u_signed64      TransferOffset,
                     unsigned int *  BufferLength,
                     void         ** DataBuffer))
{
	globus_thread_t thread;

	GlobusGFSName(globus_i_gfs_hpss_data_pio_begin);
	GlobusGFSHpssDebugEnter();

	/* Initialize the exitted variable. */
	Request->PioRegister.Exitted = GLOBUS_FALSE;

	/* Save our callback. */
	Request->PioRegister.Callback = Callback;

	globus_thread_create(&thread,
	                     NULL,
	                     globus_i_gfs_hpss_data_pio_register,
	                     Request);

	GlobusGFSHpssDebugExit();
	return;
}

static void
globus_i_gfs_hpss_data_pio_end(struct request * Request)
{
	GlobusGFSName(globus_i_gfs_hpss_data_pio_end);
	GlobusGFSHpssDebugEnter();

	/* Wait for the pio register thread to exit. */
	globus_mutex_lock(&Request->Mutex);
	{
		while (Request->PioRegister.Exitted == GLOBUS_FALSE)
		{
			globus_cond_wait(&Request->Cond, &Request->Mutex);
		}
	}
	globus_mutex_unlock(&Request->Mutex);

	GlobusGFSHpssDebugExit();
	return;
}

static void
globus_i_gfs_hpss_data_wait_on_gridftp (struct request * Request)
{
	GlobusGFSName(globus_i_gfs_hpss_data_wait_on_gridftp);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&Request->Mutex);
	{
		/* While we have outstanding GridFtp requests... */
		while (Request->CurrentGridFtpRequests > 0)
		{
			/* Wait */
			globus_cond_wait(&Request->Cond, &Request->Mutex);
		}
	}
	globus_mutex_unlock(&Request->Mutex);

	GlobusGFSHpssDebugExit();
}

/*
 * Right now, all read requests in the data dsi(s) are sending ranges.
 * This could be more efficient if:
 *   1) We told the data dsi(s) when to send ranges and
 *   2) The data dsi(s) held ranges and sent them in bulk
 *
 * Unfortunately, when I try to use globus_gridftp_server_transfer_event()
 * to tell the data dsi(s) to send ranges, the control dsi ceases to
 * receive events. Maybe a bug, maybe I'm calling it incorrectly. Also,
 * I do not have a good algorithm yet as to how to 'bulk' the ranges.
 */

static void
globus_i_gfs_hpss_data_send_range(struct request * Request,
                                  uint64_t         Offset, 
                                  uint64_t         Length)
{
	globus_gfs_event_info_t event_info;

	GlobusGFSName(globus_i_gfs_hpss_data_send_range);
	GlobusGFSHpssDebugEnter();

	/* Initialize the event info. */
	event_info.type       = GLOBUS_GFS_EVENT_RANGES_RECVD;
	event_info.event_arg  = NULL;
	event_info.node_ndx   = 0;
	event_info.id         = 0xDEADBEEF;
	event_info.event_mask = 0;

	/* Create the range list. */
	globus_range_list_init(&event_info.recvd_ranges);

	/* Insert our range. */
	globus_range_list_insert(event_info.recvd_ranges, Offset, Length);

	/* Send the range. */
	globus_gridftp_server_operation_event(Request->Operation,
	                                      GLOBUS_SUCCESS,
	                                      &event_info);

	/* Destroy the range list. */
	globus_range_list_destroy(event_info.recvd_ranges);

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_data_gridftp_read_callback(
    globus_gfs_operation_t   Operation,
    globus_result_t          Result,
    globus_byte_t          * Buffer,
    globus_size_t            Length,
    globus_off_t             TransferOffset,
    globus_bool_t            Eof,
    void                   * UserArg)
{
	struct request * request = NULL;
	buffer_t       * buffer  = NULL;

	GlobusGFSName(globus_i_gfs_hpss_data_gridftp_read_callback);
	GlobusGFSHpssDebugEnter();

	/* Make sure we received our callback arg. */
	globus_assert(UserArg != NULL);
	/* Cast our callback arg. */
	buffer = (struct buffer *) UserArg;
	/* Get a pointer to the request. */
	request = buffer->Request;

	/* Make sure our buffer pointers are sane. */
	globus_assert((char *)Buffer == buffer->Data);

	globus_mutex_lock(&request->Mutex);
	{
		/* Release this ready request. */
		globus_i_gfs_hpss_data_release_ready_buffer(request, 
		                                            buffer,
		                                            TransferOffset,
		                                            Length);

		/* Delete our reference to the buffer. */
		buffer = NULL;
		
		/* Save our error value if another error has not occurred. */
		if (request->Result == GLOBUS_SUCCESS)
			request->Result = Result;

		if (Length != 0 && request->Result == GLOBUS_SUCCESS)
		{
			/*
			 * Check our assumption that data doesn't arrive after the first EOF
			 * although it may arrive w/ the EOF. There is a comment at the end
			 * of globus_i_gfs_hpss_data_gridftp_read() that explains the need
			 * for this.
			 */
			globus_assert(request->Eof == GLOBUS_FALSE);

			/*
			 * Send the range. We will call it locked so that the EOF/Result
			 * status doesn't change underneath us. 
			 */
			globus_i_gfs_hpss_data_send_range(request, TransferOffset, Length);
		}


		/* Indicate if we received EOF. */
		if (Eof == GLOBUS_TRUE)
			request->Eof = Eof;

		/* Decrement the number of outstanding GridFTP requests. */
		request->CurrentGridFtpRequests--;

		/* Wake any waiters. */
		globus_cond_broadcast(&request->Cond);
	}
	globus_mutex_unlock(&request->Mutex);

	GlobusGFSHpssDebugExit();
}

/*
 * begin/end transfer calls are here in this function outside of
 * the PIO logic so that the control DSI will receive begin/end
 * events while it may be waiting in hpss_PIOExecute(). The control
 * DSI may be waiting for us to send it our EOF notice before it's
 * final hpss_PIOExecute() call which causes us to hang in
 * hpss_PIORegister(). If we instead tried to call end transfer after
 * PIORegister() exits, we would hang.
 */
static void
globus_i_gfs_hpss_data_gridftp_read(struct request         * Request,
                                    globus_gfs_operation_t   Operation)
{
	buffer_t        * buffer = NULL;
	globus_result_t   result = GLOBUS_SUCCESS;

	GlobusGFSName(globus_i_gfs_hpss_data_register_gridftp_read);
	GlobusGFSHpssDebugEnter();

	/* Inform the server that we are starting. */
	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	globus_mutex_lock(&Request->Mutex);
	{
		/* Until we receive EOF... */
		while (Request->Eof == GLOBUS_FALSE)
		{
			/* Check if an error occurred. */
			if (Request->Result != GLOBUS_SUCCESS)
				break;

			/* Request a free request. */
			result = globus_i_gfs_hpss_data_get_free_buffer(Request,
			                                                &buffer);
			if (result != GLOBUS_SUCCESS)
				break;

			/* If there are no free buffers... */
			if (buffer == NULL)
			{
				/* Sleep */
				globus_cond_wait(&Request->Cond, &Request->Mutex);

				/* Start over. */
				continue;
			}

			/* Submit the write request. */
			result = globus_gridftp_server_register_read(
			             Operation,
			             (globus_byte_t *)buffer->Data,
			             buffer->BufferLength,
			             globus_i_gfs_hpss_data_gridftp_read_callback,
			             buffer);

			if (result != GLOBUS_SUCCESS)
			{
				result = GlobusGFSErrorWrapFailed(
				             "globus_gridftp_server_register_read",
				             result);
				break;
			}

			/* Increment the number of outstanding GridFTP requests. */
			Request->CurrentGridFtpRequests++;
		}


		/* If we had an error. */
		if (result != GLOBUS_SUCCESS)
		{
			/* Save it. */
			if (Request->Result == GLOBUS_SUCCESS)
				Request->Result = result;
		}
	}
	globus_mutex_unlock(&Request->Mutex);

	/*
	 * Notify the server that we are done with the transfer. If we are
	 * done because of an error, this will cause the control DSI to
	 * call PIOEnd() which will kick us out of PIORegister().
	 */
	globus_gridftp_server_finished_transfer(Operation, result);

/*
 * It may be the case that, on success, we need to call wait_on_gridftp()
 * then finished_transfer() AND on success, call finished_transfer()
 * then wait_on_gridftp(). This is because, on error, we need to call
 * finished_transfer() to cause the registered reads to kickout otherwise
 * wait_on_gridftp() would hang. But on success, it may be possible (not sure)
 * that we receive multiple EOFs and some of those EOFs other than the first
 * carry data. This is a problem since the read callback needs to send ranges
 * but can only do so if Request->Operation->ipc_handle->state is in REPLY_WAIT
 * but once I call finished_transfer(), the ipc_handle's state goes to OPEN.
 * I'm going to leave it as is for now with the hope that we do not need to
 * send ranges after the first EOF (or error?) is received. I've added an
 * assertion to the read callback to make sure this assumption holds. If that
 * assert is tripped, then we can swap calls here.
 */
/* 
 * Anyone making IPC calls, like sending ranges, should do so with the request
 * locked and checking Request->Result == GLOBUS_SUCCESS so that we do not have
 * a race condition between the IPC call and our finished_transfer() call.
 */

	/* Wait for all gridftp requests to complete. */
	globus_i_gfs_hpss_data_wait_on_gridftp(Request);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_data_gridftp_write_callback(
    globus_gfs_operation_t   Operation,
    globus_result_t          Result,
    globus_byte_t          * Buffer,
    globus_size_t            BytesWritten,
    void                   * UserArg)
{
	struct buffer  * buffer  = NULL;
	struct request * request = NULL;

	GlobusGFSName(globus_i_gfs_hpss_data_gridftp_write_callback);
	GlobusGFSHpssDebugEnter();

	/* Make sure we received our callback arg. */
	globus_assert(UserArg != NULL);
	/* Cast our buffer. */
	buffer = (struct buffer *) UserArg;
	/* Get a pointer to our request. */
	request = buffer->Request;

	/* On success... */
	if (Result == GLOBUS_SUCCESS)
		/* Check the assumption that we wrote the entire buffer. */
		globus_assert(buffer->BufferLength == BytesWritten);

	/* Make sure our buffer pointers are sane. */
	globus_assert((char *)Buffer == buffer->Data);

	globus_mutex_lock(&request->Mutex);
	{
		/* Release this free buffer. */
		globus_i_gfs_hpss_data_release_free_buffer(request, buffer);

		/* Delete our pointer to the buffer. */
		buffer = NULL;
		
		/* If an error has not yet occurred... */
		if (request->Result == GLOBUS_SUCCESS)
			/* Register our return value. */
			request->Result = Result;

		/* Decrement the number of outstanding GridFTP requests. */
		request->CurrentGridFtpRequests--;

		/* Wake any waiters. */
		globus_cond_broadcast(&request->Cond);
	}
	globus_mutex_unlock(&request->Mutex);

	GlobusGFSHpssDebugExit();
}

/*
 * Fire off a gridftp write request for every free buffer.
 */
static void
globus_i_gfs_hpss_data_gridftp_write(struct request         * Request,
                                     globus_gfs_operation_t   Operation)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	struct buffer   * buffer = NULL;

	GlobusGFSName(globus_i_gfs_hpss_data_gridftp_write);
	GlobusGFSHpssDebugEnter();

	/* Inform the server that we are starting. */
	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	globus_mutex_lock(&Request->Mutex);
	{
		while (TRUE)
		{
			/* Check if an error occurred. */
			if (Request->Result != GLOBUS_SUCCESS)
				break;

			/* Request any ready buffer. */
			globus_i_gfs_hpss_data_get_any_ready_buffer(Request, &buffer);
			if (buffer == NULL)
			{
				/*
				 * Since all ready buffers have been sent, we can break
				 * out if PIORegister() has exitted. If we didn't check
				 * for ready buffers, we may have left some unsent.
				 */
				if (Request->PioRegister.Exitted == GLOBUS_TRUE)
					break;

				/* Wait for a buffer. */
				globus_cond_wait(&Request->Cond, &Request->Mutex);

				/* Start over. */
				continue;
			}

			/* Submit the write request. */
			result = globus_gridftp_server_register_write(
			           Operation,
			           (globus_byte_t *)buffer->Data,
			           buffer->BufferLength,
			           buffer->TransferOffset,
			           -1,
			           globus_i_gfs_hpss_data_gridftp_write_callback,
			           buffer);

			if (result != GLOBUS_SUCCESS)
			{
				result = GlobusGFSErrorWrapFailed(
				             "globus_gridftp_server_register_write",
				             result);
				break;
			}

			/* Increment the number of outstanding GridFTP buffers. */
			Request->CurrentGridFtpRequests++;
		}

		/* If we had an error. */
		if (result != GLOBUS_SUCCESS)
		{
			/* Save it. */
			if (Request->Result == GLOBUS_SUCCESS)
				Request->Result = result;
		}
	}
	globus_mutex_unlock(&Request->Mutex);

	/*
	 * Notify the server that we are done with the transfer. If we are
	 * done because of an error, this will cause the control DSI to
	 * call PIOEnd() which will kick us out of PIORegister().
	 */
	globus_gridftp_server_finished_transfer(Operation, result);

	/* Wait for all gridftp requests to complete. */
	globus_i_gfs_hpss_data_wait_on_gridftp(Request);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_data_destroy_session_handle(
    session_handle_t * SessionHandle)
{
    GlobusGFSName(globus_i_gfs_hpss_data_destroy_session_handle);
    GlobusGFSHpssDebugEnter();

	if (SessionHandle == NULL)
		return;

	/* Destroy the request. */
	globus_i_gfs_hpss_data_destroy_request(&SessionHandle->Request);

	globus_free(SessionHandle);
    GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_data_session_start(
    globus_gfs_operation_t      Operation,
    globus_gfs_session_info_t * SessionInfo)
{
    globus_result_t    result         = GLOBUS_SUCCESS;
	session_handle_t * session_handle = NULL;

    GlobusGFSName(globus_i_gfs_hpss_data_session_start);
    GlobusGFSHpssDebugEnter();

	/* Allocate the session handle. */
	session_handle = (session_handle_t *) globus_calloc(
	                                          1, 
	                                          sizeof(session_handle_t));
	if (session_handle == NULL)
	{
        result = GlobusGFSErrorMemory("session_handle");
		goto cleanup;
	}

	/* Initialize the request. */
	globus_i_gfs_hpss_data_init_request(&session_handle->Request);

	/* Inform the server that we are done. */
	globus_gridftp_server_finished_session_start(
	    Operation,
	    GLOBUS_SUCCESS,
	    session_handle, /* session handle */
	    "USERNAME",
	    "HOMEDIRECTORY");
    GlobusGFSHpssDebugExit();
	return;

cleanup:
	/* Destroy the session handle. */
	globus_i_gfs_hpss_data_destroy_session_handle(session_handle);

	/* Inform the server that we are done. */
	globus_gridftp_server_finished_session_start(Operation,
	                                             result,
	                                             NULL,
	                                             NULL,
	                                             NULL);

	/* Debug output w/ error. */
	GlobusGFSHpssDebugExitWithError();
}

static void
globus_i_gfs_hpss_data_session_stop(void * UserArg)
{
	session_handle_t * session_handle = NULL;

    GlobusGFSName(globus_i_gfs_hpss_data_session_stop);
    GlobusGFSHpssDebugEnter();

	if (UserArg != NULL)
	{
		/* Cast to our session handle. */
		session_handle = (session_handle_t *) UserArg;

		/* Destroy the session handle. */
		globus_i_gfs_hpss_data_destroy_session_handle(session_handle);
	}

    GlobusGFSHpssDebugExit();
}

/*
 * RETR operation
 */
static void
globus_i_gfs_hpss_data_send(
    globus_gfs_operation_t       Operation,
    globus_gfs_transfer_info_t * TransferInfo,
    void                       * UserArg)
{
	session_handle_t * session_handle = NULL;

	GlobusGFSName(globus_i_gfs_hpss_data_send);
	GlobusGFSHpssDebugEnter();

	/* Make sure we received something. */
	globus_assert(UserArg != NULL);
	/* Cast to our session handle. */
	session_handle = (session_handle_t *) UserArg;

	/*
	 * Make sure we received our stripe group buffer from the
	 * control server.
	 */
	globus_assert(session_handle->Request.PioStripeGroup.ReceivedBuffer != NULL);
	globus_assert(session_handle->Request.PioStripeGroup.BufferLength   != 0);

	/* Start our request. */
	globus_i_gfs_hpss_data_start_request(&session_handle->Request,
	                                     Operation);

	/* Launch the PIO transfer. */
	globus_i_gfs_hpss_data_pio_begin(
	             &session_handle->Request,
	             globus_i_gfs_hpss_data_hpss_read_callback);

	globus_i_gfs_hpss_data_gridftp_write(&session_handle->Request,
	                                     Operation);

	/* End the PIO request. */
	globus_i_gfs_hpss_data_pio_end(&session_handle->Request);

	/* End our request. */
	globus_i_gfs_hpss_data_end_request(&session_handle->Request);

	GlobusGFSHpssDebugExit();
}

/*
 * STOR operation
 */
static void
globus_i_gfs_hpss_data_recv(
    globus_gfs_operation_t       Operation,
    globus_gfs_transfer_info_t * TransferInfo,
    void                       * UserArg)
{
	session_handle_t * session_handle = NULL;

	GlobusGFSName(globus_i_gfs_hpss_data_recv);
	GlobusGFSHpssDebugEnter();

	/* Make sure we received something. */
	globus_assert(UserArg != NULL);
	/* Cast to our session handle. */
	session_handle = (session_handle_t *) UserArg;

	/*
	 * Make sure we received our stripe group buffer from the
	 * control server.
	 */
	globus_assert(session_handle->Request.PioStripeGroup.ReceivedBuffer != NULL);
	globus_assert(session_handle->Request.PioStripeGroup.BufferLength   != 0);

	/* Start our request. */
	globus_i_gfs_hpss_data_start_request(&session_handle->Request,
	                                     Operation);

	/* Launch the PIO transfer. */
	globus_i_gfs_hpss_data_pio_begin(
	             &session_handle->Request,
	             globus_i_gfs_hpss_data_hpss_write_callback);

	globus_i_gfs_hpss_data_gridftp_read(&session_handle->Request,
	                                    Operation);

	/* End the PIO request. */
	globus_i_gfs_hpss_data_pio_end(&session_handle->Request);

	/* End our request. */
	globus_i_gfs_hpss_data_end_request(&session_handle->Request);

	GlobusGFSHpssDebugExit();
}

/*
 * This function receives events from our server and from the control
 * server sent via globus_gfs_ipc_request_transfer_event().  The event
 * types received are set by the mask to
 * globus_gridftp_server_begin_transfer().
 *
 * Currently none.
 */
static void
globus_i_gfs_hpss_data_trev(
    globus_gfs_event_info_t * EventInfo,
    void                    * UserArg)
{
	GlobusGFSName(globus_i_gfs_hpss_data_trev);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();
}

static void
globus_i_gfs_hpss_data_stat(
    globus_gfs_operation_t   Operation,
    globus_gfs_stat_info_t * StatInfo,
    void *                   UserArg)
{
	session_handle_t  * session_handle = NULL;
	globus_gfs_stat_t * stat_array     = NULL;
	globus_result_t     result         = GLOBUS_SUCCESS;
	int                 stat_count     = 0;

	GlobusGFSName(globus_i_gfs_hpss_data_stat);
	GlobusGFSHpssDebugEnter();

	/* Make sure we were passed something. */
	globus_assert(UserArg != NULL);
	/* Cast to our session handle. */
	session_handle = (session_handle_t *) UserArg;

	/*
	 * Sometimes the server will request a stat for internal purposes (like
	 * just before a STOR operation). The control DSI sends us this info but
	 * sometimes (like if the file does not already exist) there is not stat
	 * info to send us. In this case, we just return a generic error and the
	 * operation (like STOR) will proceed.
	 */
	if (StatInfo->internal)
	{
		if (session_handle->Request.StatArray.ReceivedBuffer == NULL)
		{
			result = GlobusGFSErrorGeneric("Stat info not available");
			goto cleanup;
		}
	}

	/* Make sure we have our buffer from the control DSI. */
	globus_assert(session_handle->Request.StatArray.ReceivedBuffer != NULL);
	globus_assert(session_handle->Request.StatArray.BufferLength   != 0);

	/* Decode the stat array. */
	result = globus_i_gfs_hpss_common_decode_stat_array(
	             &stat_array,
	             &stat_count,
	             (globus_byte_t *)session_handle->Request.StatArray.ReceivedBuffer,
	             session_handle->Request.StatArray.BufferLength);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		/* Inform the server of our results. */
		globus_gridftp_server_finished_stat(Operation, result, NULL, 0);
		GlobusGFSHpssDebugExitWithError();
	} else
	{
		/* Inform the server of our results. */
		globus_gridftp_server_finished_stat(Operation,
		                                    GLOBUS_SUCCESS,
		                                    stat_array,
		                                    stat_count);
		GlobusGFSHpssDebugExit();
	}

	/* Destroy our stat array. */
	globus_l_gfs_hpss_common_destroy_stat_array(stat_array, stat_count);

	/* Reset our received buffer. */
	if (session_handle->Request.StatArray.ReceivedBuffer != NULL)
		globus_free(session_handle->Request.StatArray.ReceivedBuffer);
	session_handle->Request.StatArray.ReceivedBuffer = NULL;
	session_handle->Request.StatArray.BufferLength   = 0;
}


/*
 * This function receives buffers from the control server. We
 * received buffers for:
 *  1) List operations
 *  2) File send/receive
 *
 */
static void
globus_i_gfs_hpss_data_buffer_recv(
    int             BufferType,
    globus_byte_t * Buffer,
    globus_size_t   BufferLength,
    void          * UserArg)
{
	session_handle_t * session_handle = NULL;

	GlobusGFSName(globus_i_gfs_hpss_data_buffer_recv);
	GlobusGFSHpssDebugEnter();

	/* Make sure we were passed something. */
	globus_assert(UserArg != NULL);
	globus_assert(Buffer  != NULL);

	/* Cast to our session handle. */
	session_handle = (session_handle_t *) UserArg;

	switch (BufferType)
	{
	case GLOBUS_HPSS_BUFFER_TYPE_STRIPE_GROUP:
		globus_assert(session_handle->Request.PioStripeGroup.ReceivedBuffer == NULL);
		globus_assert(session_handle->Request.PioStripeGroup.BufferLength   == 0);

		/*
		 * Copy out the listing/stripe group.
		 */
		session_handle->Request.PioStripeGroup.ReceivedBuffer = (char *) globus_malloc(BufferLength);

		/* How can we handle errors in this call? */
		globus_assert(session_handle->Request.PioStripeGroup.ReceivedBuffer != NULL);

		/* Copy in the buffer. */
		memcpy(session_handle->Request.PioStripeGroup.ReceivedBuffer, Buffer, BufferLength);

		/* Record the length. */
		session_handle->Request.PioStripeGroup.BufferLength = BufferLength;

		break;

	case GLOBUS_HPSS_BUFFER_TYPE_STAT_ARRAY:
		globus_assert(session_handle->Request.StatArray.ReceivedBuffer == NULL);
		globus_assert(session_handle->Request.StatArray.BufferLength   == 0);

		/*
		 * Copy out the listing/stripe group.
		 */
		session_handle->Request.StatArray.ReceivedBuffer = (char *) globus_malloc(BufferLength);

		/* How can we handle errors in this call? */
		globus_assert(session_handle->Request.StatArray.ReceivedBuffer != NULL);

		/* Copy in the buffer. */
		memcpy(session_handle->Request.StatArray.ReceivedBuffer, Buffer, BufferLength);

		/* Record the length. */
		session_handle->Request.StatArray.BufferLength = BufferLength;

		break;

	default:
		globus_assert(0);
	}

	GlobusGFSHpssDebugExit();
}

static int globus_i_gfs_hpss_data_activate(void);
static int globus_i_gfs_hpss_data_deactivate(void);

static globus_gfs_storage_iface_t       globus_i_gfs_hpss_data_dsi_iface =
{
	GLOBUS_GFS_DSI_DESCRIPTOR_SENDER,
	globus_i_gfs_hpss_data_session_start,  /* start */
	globus_i_gfs_hpss_data_session_stop,   /* stop  */
	NULL,                                  /* list  */
	globus_i_gfs_hpss_data_send,           /* send  */
	globus_i_gfs_hpss_data_recv,           /* recv  */
	globus_i_gfs_hpss_data_trev,           /* trev  */
	NULL,                                  /* active */
	NULL,                                  /* passive */
	NULL,                                  /* data destroy */
	NULL,                                  /* command */
	globus_i_gfs_hpss_data_stat,           /* stat */
	NULL,                                  /* cred */
	globus_i_gfs_hpss_data_buffer_recv,    /* buffer */
};

GlobusExtensionDefineModule(globus_gridftp_server_hpss_data) =
{
    "globus_gridftp_server_hpss_data",
    globus_i_gfs_hpss_data_activate,
    globus_i_gfs_hpss_data_deactivate,
    NULL,
    NULL,
    &local_version
};

static int
globus_i_gfs_hpss_data_activate(void)
{
    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY,
        "hpss_data",
        GlobusExtensionMyModule(globus_gridftp_server_hpss_data),
        &globus_i_gfs_hpss_data_dsi_iface);

    GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS,
        ERROR WARNING TRACE INTERNAL_TRACE INFO STATE INFO_VERBOSE);
    
    return GLOBUS_SUCCESS;
}

static int
globus_i_gfs_hpss_data_deactivate(void)
{
    globus_extension_registry_remove(
        GLOBUS_GFS_DSI_REGISTRY, "hpss_data");

    return GLOBUS_SUCCESS;
}

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
#include <string.h>
#include <stdio.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_transfer_data.h"
#include "gridftp_dsi_hpss_pio_control.h"
#include "gridftp_dsi_hpss_range_list.h"
#include "gridftp_dsi_hpss_pio_data.h"
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_msg.h"

typedef enum {
	PIO_DATA_OP_TYPE_RETR,
	PIO_DATA_OP_TYPE_STOR,
	PIO_DATA_OP_TYPE_CKSM,
} pio_data_op_type_t;

struct pio_data {
	pio_data_op_type_t        OpType;
	buffer_handle_t         * BufferHandle;
	buffer_priv_id_t          PrivateBufferID;
	pio_data_eof_callback_t   EofCallbackFunc;
	void                    * EofCallbackArg;
	pio_data_buffer_pass_t    BufferPassFunc;
	void                    * BufferPassArg;
	msg_handle_t            * MsgHandle;
	globus_off_t              BufferSize;
	msg_register_id_t         MsgRegisterID;
	range_list_t            * RangeList;

	globus_mutex_t            Lock;
	globus_cond_t             Cond;
	globus_bool_t             Eof;
	globus_bool_t             PioRegisterRunning;
	globus_bool_t             Stop;
	hpss_pio_grp_t            StripeGroup;
	int                       StripeIndex;
	globus_result_t           Result;
};

static void *
pio_data_register_thread(void * Arg);

static int
pio_data_register_read_callback(void         *  Arg,
                                u_signed64      FileOffset,
                                unsigned int *  ReadyBufferLength,
                                void         ** ReadyBuffer);

static int
pio_data_register_write_callback(void         *  Arg,
                                 u_signed64      FileOffset,
                                 unsigned int *  BufferLength,
                                 void         ** Buffer);

static void
pio_data_msg_recv(void          * CallbackArg,
                  msg_comp_id_t   DstMsgCompID,
                  msg_comp_id_t   SrcMsgCompID,
                  int             MsgType,
                  int             MsgLen,
                  void          * Msg);

static globus_result_t
pio_data_common_init(pio_data_op_type_t         OpType,
                     buffer_handle_t         *  BufferHandle,
                     msg_handle_t            *  MsgHandle,
                     pio_data_eof_callback_t    EofCallbackFunc,
                     void                    *  EofCallbackArg,
                     pio_data_t              ** PioData)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*PioData = (pio_data_t *) globus_calloc(1, sizeof(pio_data_t));
	if (*PioData == NULL)
	{
		result = GlobusGFSErrorMemory("pio_data_t");
		goto cleanup;
	}

	/* Initialize the entries. */
	(*PioData)->OpType             = OpType;
	(*PioData)->BufferHandle       = BufferHandle;
	(*PioData)->EofCallbackFunc    = EofCallbackFunc;
	(*PioData)->EofCallbackArg     = EofCallbackArg;
	(*PioData)->PioRegisterRunning = GLOBUS_FALSE;
	(*PioData)->Stop               = GLOBUS_FALSE;
	(*PioData)->PrivateBufferID    = buffer_create_private_list(BufferHandle);
	(*PioData)->MsgHandle          = MsgHandle;
	(*PioData)->BufferSize         = buffer_get_alloc_size(BufferHandle);
	(*PioData)->MsgRegisterID      = MSG_REGISTER_ID_NONE;

	/* Register to receive messages. */
	result = msg_register(MsgHandle,
	                      MSG_COMP_ID_NONE,
	                      MSG_COMP_ID_TRANSFER_DATA_PIO,
	                      pio_data_msg_recv,
	                      *PioData,
	                      &(*PioData)->MsgRegisterID);

	globus_mutex_init(&(*PioData)->Lock, NULL);
	globus_cond_init(&(*PioData)->Cond, NULL);


cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

globus_result_t
pio_data_stor_init(buffer_handle_t         *  BufferHandle,
                   msg_handle_t            *  MsgHandle,
                   pio_data_eof_callback_t    EofCallbackFunc,
                   void                    *  EofCallbackArg,
                   pio_data_t              ** PioData)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = pio_data_common_init(PIO_DATA_OP_TYPE_STOR,
	                              BufferHandle,
	                              MsgHandle,
	                              EofCallbackFunc,
	                              EofCallbackArg,
	                              PioData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

globus_result_t
pio_data_retr_init(buffer_handle_t            *  BufferHandle,
                   msg_handle_t               *  MsgHandle,
                   globus_gfs_transfer_info_t *  TransferInfo,
                   pio_data_eof_callback_t       EofCallbackFunc,
                   void                       *  EofCallbackArg,
                   pio_data_t                 ** PioData)

{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = pio_data_common_init(PIO_DATA_OP_TYPE_RETR,
	                              BufferHandle,
	                              MsgHandle,
	                              EofCallbackFunc,
	                              EofCallbackArg,
	                              PioData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Initialize our range list. */
	result = range_list_init(&(*PioData)->RangeList);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Populate the range list with our transfer ranges. */
	result = range_list_fill_retr_range((*PioData)->RangeList, TransferInfo);

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

globus_result_t
pio_data_cksm_init(buffer_handle_t            *  BufferHandle,
                   msg_handle_t               *  MsgHandle,
                   globus_gfs_command_info_t  *  CommandInfo,
                   pio_data_eof_callback_t       EofCallbackFunc,
                   void                       *  EofCallbackArg,
                   pio_data_t                 ** PioData)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = pio_data_common_init(PIO_DATA_OP_TYPE_CKSM,
	                              BufferHandle,
	                              MsgHandle,
	                              EofCallbackFunc,
	                              EofCallbackArg,
	                              PioData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Initialize our range list. */
	result = range_list_init(&(*PioData)->RangeList);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Populate the range list with our transfer ranges. */
	result = range_list_fill_cksm_range((*PioData)->RangeList, CommandInfo);

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}


void
pio_data_set_buffer_pass_func(pio_data_t             *  PioData,
                              pio_data_buffer_pass_t    BufferPassFunc,
                              void                   *  BufferPassArg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	PioData->BufferPassFunc = BufferPassFunc;
	PioData->BufferPassArg  = BufferPassArg;

	GlobusGFSHpssDebugExit();
}

void
pio_data_destroy(pio_data_t * PioData)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (PioData != NULL)
	{
		/* Unregister to receive messages. */
		msg_unregister(PioData->MsgHandle, PioData->MsgRegisterID);

		/* Destroy our range list. */
		range_list_destroy(PioData->RangeList);

		globus_mutex_destroy(&PioData->Lock);
		globus_cond_destroy(&PioData->Cond);
		globus_free(PioData);
	}

	GlobusGFSHpssDebugExit();
}

static void
pio_data_stripe_group_msg(pio_data_t * PioData,
                          void       * StripeGroupBuffer,
                          int          BufferLength)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;
	globus_thread_t thread;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Import the stripe group. */
	retval = hpss_PIOImportGrp(StripeGroupBuffer, 
	                           BufferLength, 
	                           &PioData->StripeGroup);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_PIOImportGrp", retval);
		goto cleanup;
	}

	/* Indicate that pio register is running. */
	PioData->PioRegisterRunning = GLOBUS_TRUE;

	/* Launch pio register. */
	retval = globus_thread_create(&thread, NULL, pio_data_register_thread, PioData);
	if (retval != 0)
	{
		PioData->PioRegisterRunning = GLOBUS_FALSE;
		result = GlobusGFSErrorSystemError("globus_thread_create", retval);
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
		PioData->EofCallbackFunc(PioData->EofCallbackArg, result);

	GlobusGFSHpssDebugExit();
}

static void
pio_data_stripe_index_msg(pio_data_t * PioData,
                          void       * Msg,
                          int          MsgLen)
{
	char * int_buf = (char *) Msg;
	int    index   = 0;
	int    retcnt  = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Decode our new stripe index. */
	retcnt = sscanf(int_buf, "%u", &index);

	globus_assert(retcnt == 1);

	PioData->StripeIndex = index;

	GlobusGFSHpssDebugExit();
}

/*
 * Message handle callback function. 
 */
static void
pio_data_msg_recv(void          * CallbackArg,
                  msg_comp_id_t   DstMsgCompID,
                  msg_comp_id_t   SrcMsgCompID,
                  int             MsgType,
                  int             MsgLen,
                  void          * Msg)
{
	pio_data_t * pio_data = (pio_data_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	switch (SrcMsgCompID)
	{
	case MSG_COMP_ID_TRANSFER_CONTROL_PIO:
		switch (MsgType)
		{
		case PIO_CONTROL_MSG_TYPE_STRIPE_GROUP:
			pio_data_stripe_group_msg(pio_data, Msg, MsgLen);
			break;

		case PIO_CONTROL_MSG_TYPE_STRIPE_INDEX:
			pio_data_stripe_index_msg(pio_data, Msg, MsgLen);
			break;

		default:
			globus_assert(0);
		}
		break;

	default:
		globus_assert(0);
	}

	GlobusGFSHpssDebugExit();
}

void
pio_data_buffer(void         * CallbackArg,
                char         * Buffer,
                globus_off_t   Offset,
                globus_off_t   Length)
{
	pio_data_t * pio_data = (pio_data_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	switch (pio_data->OpType)
	{
	case PIO_DATA_OP_TYPE_STOR:
		/* Put this buffer on our ready list */
		buffer_store_ready_buffer(pio_data->BufferHandle,
		                          pio_data->PrivateBufferID,
		                          Buffer,
		                          Offset,
		                          Length);
		break;

	case PIO_DATA_OP_TYPE_RETR:
	case PIO_DATA_OP_TYPE_CKSM:
		/* Put this buffer on our free list */
		buffer_store_free_buffer(pio_data->BufferHandle,
		                         pio_data->PrivateBufferID,
		                         Buffer);
		break;
	}

	/* Wake any waiters. */
	globus_mutex_lock(&pio_data->Lock);
	{
		globus_cond_broadcast(&pio_data->Cond);
	}
	globus_mutex_unlock(&pio_data->Lock);

	GlobusGFSHpssDebugExit();
}

void
pio_data_flush(pio_data_t * PioData)
{
	int ready_buffer_count = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&PioData->Lock);
	{
		/* Indicate that we have all the buffers we will ever have. */
		PioData->Eof = GLOBUS_TRUE;

		/*
		 * Make sure the thread we are waiting to exit is not waiting
		 * on us.
		 */
		globus_cond_signal(&PioData->Cond);

		/*
		 * Make sure that all ready buffers have been processed.
		 */
		while (1)
		{
			ready_buffer_count = buffer_get_ready_buffer_count(PioData->BufferHandle,
			                                                   PioData->PrivateBufferID);

			if (ready_buffer_count == 0)
				break;

			/* Bail on error. */
			if (PioData->Result != GLOBUS_SUCCESS)
				break;

			globus_cond_wait(&PioData->Cond, &PioData->Lock);
		}

		/*
		 * Make sure the register thread has completed.
		 */
		while (PioData->PioRegisterRunning == GLOBUS_TRUE)
		{
			globus_cond_wait(&PioData->Cond, &PioData->Lock);
		}
	}
	globus_mutex_unlock(&PioData->Lock);

	GlobusGFSHpssDebugExit();
}

void
pio_data_stop(pio_data_t * PioData)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&PioData->Lock);
	{
		/* Indicate full stop. */
		PioData->Stop = GLOBUS_TRUE;

		/* Wake anyone waiting. */
		globus_cond_broadcast(&PioData->Cond);

		while (PioData->PioRegisterRunning == GLOBUS_TRUE)
		{
			globus_cond_wait(&PioData->Cond, &PioData->Lock);
		}
	}
	globus_mutex_unlock(&PioData->Lock);

	GlobusGFSHpssDebugExit();
}

static void *
pio_data_register_thread(void * Arg)
{
	int             retval   = 0;
	char          * buffer   = NULL;
	pio_data_t    * pio_data = (pio_data_t *) Arg;
	globus_off_t    length   = 0;
	globus_result_t result   = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Get a free buffer. */
	result = buffer_allocate_buffer(pio_data->BufferHandle,
	                                pio_data->PrivateBufferID,
	                                &buffer,
	                                &length);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * hpss_PIORegister() can exit early w/o error when hpss_PIOExecute()
	 * encounters an error and calls hpss_PIOEnd().
	 */

	/* Call PIO register. */
	retval = hpss_PIORegister(pio_data->StripeIndex,
	                          NULL, /* DataNetSockAddr */
	                          buffer,
	                          length,
	                          pio_data->StripeGroup,
	                          pio_data->OpType == PIO_DATA_OP_TYPE_STOR?
	                             pio_data_register_write_callback :
	                             pio_data_register_read_callback,
	                          pio_data);

	/* Save the result from other threads. */
	result = pio_data->Result;

	if (retval != 0 && result == GLOBUS_SUCCESS)
		result = GlobusGFSErrorSystemError("hpss_PIORegister", -retval);

	retval = hpss_PIOEnd(pio_data->StripeGroup);
	if (retval != 0 && result == GLOBUS_SUCCESS)
		result = GlobusGFSErrorSystemError("hpss_PIOEnd", -retval);

cleanup:
	/* Release our buffer. */
	if (buffer != NULL)
		buffer_store_free_buffer(pio_data->BufferHandle,
	                             pio_data->PrivateBufferID,
	                             buffer);

	/*
	 * We only notify the caller of EOF on RETR/CKSM. On STOR, GridFTP must
	 * signal EOF. We always call the callback when an error occurs.
	 */
	if (result != GLOBUS_SUCCESS || pio_data->OpType != PIO_DATA_OP_TYPE_STOR)
		pio_data->EofCallbackFunc(pio_data->EofCallbackArg, result);

	globus_mutex_lock(&pio_data->Lock);
	{
		/* Indicate that we are exitting. */
		pio_data->PioRegisterRunning = GLOBUS_FALSE;

		/* Wake up anyone waiting. */
		globus_cond_signal(&pio_data->Cond);
	}
	globus_mutex_unlock(&pio_data->Lock);

	GlobusGFSHpssDebugExit();
	return NULL;
}

/*
 * Due to a bug in hpss_PIORegister(), each time we call hpss_PIOExecute(), this callback
 * receives the buffer passed to hpss_PIORegister(). To avoid this bug, we must copy the
 * buffer instead of exchanging it. This is HPSS bug 1660.
 */
static int
pio_data_register_read_callback(void         *  Arg,
                                u_signed64      FileOffset,
                                unsigned int *  ReadyBufferLength,
                                void         ** ReadyBuffer)
{
	int             retval        = 0;
	pio_data_t    * pio_data      = (pio_data_t *) Arg;
	char          * free_buffer   = NULL;
	int             bytes_to_copy = 0;
	globus_off_t    file_offset   = 0;
	globus_off_t    free_length   = 0;
	globus_off_t    free_offset   = 0;
	globus_off_t    ready_offset  = 0;
	globus_off_t    range_offset  = 0;
	globus_off_t    range_length  = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Convert from u64 to globus_off_t */
	CONVERT_U64_TO_LONGLONG(FileOffset, file_offset);

	/* While we still have new data... */
	while (ready_offset < *ReadyBufferLength)
	{
		/* Peek at the range we expect to get. */
		range_list_peek(pio_data->RangeList, &range_offset, &range_length);

		/*  Make sure something hasn't gone wrong. */
		globus_assert(range_offset <= file_offset);

		globus_mutex_lock(&pio_data->Lock);
		{
			while (free_buffer == NULL)
			{
				if (pio_data->Stop == GLOBUS_TRUE)
				{
					/* Set our retval so we stop. */
					retval = 1;
					goto unlock;
				}

				/* Get a free buffer. */
				buffer_get_free_buffer(pio_data->BufferHandle,
				                       pio_data->PrivateBufferID,
				                       &free_buffer,
				                       &free_length);

				if (free_buffer == NULL)
					globus_cond_wait(&pio_data->Cond, &pio_data->Lock);
			}
		}
unlock:
		globus_mutex_unlock(&pio_data->Lock);

		/* Bail on shutdown. */
		if (retval == 1)
			break;

		/* Reset the free offset. */
		free_offset = 0;
		/* Reset bytes_to_copy. */
		bytes_to_copy = 0;
	
		/* If we have a gap between what we expected and what we got... */
		if (range_offset < file_offset)
		{
			/* We'll write zeroes for the difference. */
			bytes_to_copy = file_offset - range_offset;

			/* Truncate for the length of the free buffer. */
			if (bytes_to_copy > free_length)
				bytes_to_copy = free_length;

			/* Truncate for a short 'expected range'. */
			if (bytes_to_copy > range_length)
				bytes_to_copy = range_length;

			/* Include the zeroes. */
			memset(free_buffer, 0, bytes_to_copy);

			/* Remove this range from our range list. */
			range_list_delete(pio_data->RangeList, range_offset, bytes_to_copy);

			/* Increment free_offset. */
			free_offset += bytes_to_copy;

			/* If we filled the buffer or wrote the whole range... */
			if (bytes_to_copy == free_length || bytes_to_copy == range_length)
			{
				/* Now send this buffer up stream. */
				pio_data->BufferPassFunc(pio_data->BufferPassArg,
				                         free_buffer,
				                         range_offset,
				                         bytes_to_copy);

				/* Release our reference on the free buffer. */
				free_buffer = NULL;

				/* Start over. */
				continue;
			}

			/* Increment range offset, range length */
			range_offset += bytes_to_copy;
			range_length -= bytes_to_copy;
		}

		/*
		 * Copy out real data.
		 */

		/*  Make sure something hasn't gone wrong. */
		globus_assert(file_offset == range_offset);

		/* Try to copy all remaining data. */
		bytes_to_copy = *ReadyBufferLength - ready_offset;

		/* Truncate for the length of the free buffer. */
		if (bytes_to_copy > (free_length - free_offset))
			bytes_to_copy = free_length - free_offset;

		/* Copy in real data. */
		memcpy(free_buffer  + free_offset, 
		       *ReadyBuffer + ready_offset, 
		       bytes_to_copy);

		/* Remove this range from our range list. */
		range_list_delete(pio_data->RangeList, range_offset, bytes_to_copy);

		/* Now send this buffer up stream. */
		pio_data->BufferPassFunc(pio_data->BufferPassArg,
		                         free_buffer,
		                         file_offset,
		                         bytes_to_copy + free_offset);

		/* Release our reference on the free buffer. */
		free_buffer = NULL;

		/* Increment our file offset. */
		file_offset += bytes_to_copy;

		/* Increment our ready buffer offset. */
		ready_offset += bytes_to_copy;
	}

	/* Update our ready buffer length. */
	*ReadyBufferLength = free_length;

	GlobusGFSHpssDebugExit();
	return retval;
}


static int
pio_data_register_write_callback(void         *  Arg,
                                 u_signed64      FileOffset,
                                 unsigned int *  ReadyLength,
                                 void         ** ReadyBuffer)
{
	int                      retval              = 0;
	pio_data_t             * pio_data            = (pio_data_t *) Arg;
	char                   * ready_buffer        = NULL;
	globus_off_t             ready_length        = 0;
	globus_off_t             ready_offset        = 0;
	char                   * flagged_buffer      = NULL;
	globus_off_t             flagged_length      = 0;
	globus_off_t             flagged_offset      = 0;
	globus_off_t             stored_ready_length = 0;
	globus_off_t             stored_ready_offset = 0;
	globus_result_t          result              = GLOBUS_SUCCESS;
	pio_data_bytes_written_t bytes_written;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/*
	 * See if we have a flagged buffer. This is our free buffer from a previous
	 * call.
	 */
	buffer_get_flagged_buffer(pio_data->BufferHandle,
	                          pio_data->PrivateBufferID,
	                          &flagged_buffer,
	                          &flagged_offset,
	                          &flagged_length);

	/* If we have one... */
	if (flagged_buffer != NULL)
	{
		/*
		 * Send the bytes written message based on the actual
		 * offset/length of this write. 
		 */
		bytes_written.Offset = flagged_offset;
		bytes_written.Length = flagged_length;

		result = msg_send(pio_data->MsgHandle,
		                  MSG_COMP_ID_ANY,
		                  MSG_COMP_ID_TRANSFER_DATA_PIO,
		                  PIO_DATA_MSG_TYPE_BYTES_WRITTEN,
		                  sizeof(bytes_written),
		                  &bytes_written);

		if (result != GLOBUS_SUCCESS)
		{
			globus_mutex_lock(&pio_data->Lock);
			{
				/* Save our error. */
				pio_data->Result = result;
			}
			globus_mutex_unlock(&pio_data->Lock);

			/* Indicate error. */
			retval = 1;

			/* Bail. */
			goto cleanup;
		}
	}

	/* If we do not have one... */
	if (flagged_buffer == NULL)
	{
		/* Create one. */
		result = buffer_allocate_buffer(pio_data->BufferHandle,
		                                pio_data->PrivateBufferID,
		                                &flagged_buffer,
		                                &flagged_length);
		if (result != GLOBUS_SUCCESS)
		{
			globus_mutex_lock(&pio_data->Lock);
			{
				/* Save our error. */
				pio_data->Result = result;
			}
			globus_mutex_unlock(&pio_data->Lock);

			/* Indicate error. */
			retval = 2;

			/* Bail. */
			goto cleanup;
		}

		globus_assert(flagged_buffer != NULL);

		/* Flag this buffer. */
		buffer_flag_buffer(pio_data->BufferHandle,
                           pio_data->PrivateBufferID,
                           flagged_buffer);
	}

	/* Convert the next needed offset from u64 to globus_off_t */
	CONVERT_U64_TO_LONGLONG(FileOffset, flagged_offset);

	/* Reset the flagged buffer's length. */
	flagged_length = 0;

	globus_mutex_lock(&pio_data->Lock);
	{
		/* Until this block is full... */
		while (flagged_length < pio_data->BufferSize)
		{
			/* Calculate the offset of the ready buffer we need. */
			ready_offset = flagged_offset + flagged_length;

			/* Keep trying to get the next ready buffer. */
			while (ready_buffer == NULL)
			{
				if (pio_data->Stop == GLOBUS_TRUE)
				{
					/* Set our retval so we stop. */
					retval = 3;
					goto unlock;
				}

				/* Get the ready buffer. */
				buffer_get_ready_buffer(pio_data->BufferHandle,
				                        pio_data->PrivateBufferID,
				                        &ready_buffer,
				                        ready_offset,
				                        &ready_length);

				/* If the buffer was not available... */
				if (ready_buffer == NULL)
				{
					/* Check if we are all out of ready buffers. */
					if (pio_data->Eof)
						break;

					/* Wait for more buffers. */
					globus_cond_wait(&pio_data->Cond, &pio_data->Lock);
				}
			}

			/* If we have no buffer, we must be at EOF, break out. */
			if (ready_buffer == NULL)
				break;

			/* Get the original offset/length of this bufer. */
			buffer_get_stored_offset_length(pio_data->BufferHandle,
			                                pio_data->PrivateBufferID,
			                                ready_buffer,
			                                &stored_ready_offset,
			                                &stored_ready_length);

			/* If there was no stored offset... */
			if (stored_ready_offset == (globus_off_t)-1)
			{
				stored_ready_offset = ready_offset;
				stored_ready_length = ready_length;
			}

			/*
			 * If this ready buffer is small enough to fit in the flagged
			 * buffer.
			 */
			if (ready_length <= (pio_data->BufferSize - flagged_length))
			{
				/* Copy it all in... */
				memmove(flagged_buffer + flagged_length,
				        ready_buffer + (ready_offset - stored_ready_offset),
				        ready_length);

				/* Update the flagged buffer's length. */
				flagged_length += ready_length;

				/* Reset the origianl offset/range. */
				buffer_set_offset_length(pio_data->BufferHandle,
				                         pio_data->PrivateBufferID,
				                         ready_buffer,
				                         stored_ready_offset,
				                         stored_ready_length);

				/* Clear and stored offset/range. */
				buffer_clear_stored_offset_length(pio_data->BufferHandle,
				                                  pio_data->PrivateBufferID,
				                                  ready_buffer);

				/* Release the lock for one moment. */
				globus_mutex_unlock(&pio_data->Lock);
				{
					/* Pass the buffer on. */
					pio_data->BufferPassFunc(pio_data->BufferPassArg,
					                         ready_buffer,
					                         stored_ready_offset,
					                         stored_ready_length);
				}
				globus_mutex_lock(&pio_data->Lock);

				/* Release our reference on ready buffer. */
				ready_buffer = NULL;
			}
			else
			{
				/* Copy what fits. */
				memmove(flagged_buffer + flagged_length,
				        ready_buffer + (ready_offset - stored_ready_offset),
				        pio_data->BufferSize - flagged_length);

				/* Store the original offset/length of this buffer. */
				buffer_store_offset_length(pio_data->BufferHandle,
				                           pio_data->PrivateBufferID,
				                           ready_buffer,
				                           stored_ready_offset,
				                           stored_ready_length);

				/* Put this buffer back into storaged w/ new offset/length. */
				buffer_store_ready_buffer(pio_data->BufferHandle,
				                          pio_data->PrivateBufferID,
				                          ready_buffer,
				                          ready_offset + (pio_data->BufferSize - flagged_length),
				                          ready_length - (pio_data->BufferSize - flagged_length));

				/* Release our reference on ready buffer. */
				ready_buffer = NULL;

				/* Update the flagged buffer's length. */
				flagged_length += (pio_data->BufferSize - flagged_length);
			}
		}

		/*
		 * If the client aborts the transfer, we get EOF earlier than
		 * we expected. In this case we just need to break out.
		 */
		if (flagged_length == 0)
		{
			/* This should only happen on early EOF. */
			globus_assert(pio_data->Eof == GLOBUS_TRUE);

			/* Save our error. */
			if (pio_data->Result == GLOBUS_SUCCESS)
				pio_data->Result = GlobusGFSErrorGeneric("Transfer ended prematurely");

			/* Indicate an error. */
			retval = 4;
			goto unlock;
		}

		/* Store the offset and length of the flagged buffer. */
		buffer_set_offset_length(pio_data->BufferHandle,
		                         pio_data->PrivateBufferID,
		                         flagged_buffer,
		                         flagged_offset,
		                         flagged_length);
		
		/* Pass these back to the caller. */
		*ReadyBuffer = flagged_buffer;
		*ReadyLength = flagged_length;
	}
unlock:
	globus_mutex_unlock(&pio_data->Lock);

cleanup:
	GlobusGFSHpssDebugExit();
	return retval;
}

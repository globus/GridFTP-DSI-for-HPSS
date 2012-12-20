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

/*
 * HPSS includes.
 */
#include <hpss_api.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_transfer_control.h"
#include "gridftp_dsi_hpss_transfer_data.h"
#include "gridftp_dsi_hpss_pio_control.h"
#include "gridftp_dsi_hpss_data_ranges.h"
#include "gridftp_dsi_hpss_range_list.h"
#include "gridftp_dsi_hpss_checksum.h"
#include "gridftp_dsi_hpss_pio_data.h"
#include "gridftp_dsi_hpss_session.h"
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

typedef enum {
	TRANSFER_DATA_OP_TYPE_STOR,
	TRANSFER_DATA_OP_TYPE_RETR,
	TRANSFER_DATA_OP_TYPE_CKSM,
} transfer_data_op_type_t;

typedef struct range_msg_list {
	data_ranges_msg_range_received_t   Msg;
	struct range_msg_list            * Next;
} range_msg_list_t;

struct transfer_data {
	transfer_data_op_type_t   OpType;
	pio_data_t              * PioData;
	gridftp_t               * GridFTP;
	checksum_t              * Checksum;
	buffer_handle_t         * BufferHandle;
	msg_handle_t            * MsgHandle;
	data_ranges_t           * DataRanges;
	msg_register_id_t         MsgRegisterID;
	buffer_priv_id_t          PrivateBufID;

	/*
	 * The lock protects entries below it.
	 */
	globus_mutex_t            Lock;
	globus_cond_t             Cond;
	globus_result_t           Result;
	globus_bool_t             Eof;
	globus_bool_t             ControlComplete;
	globus_off_t              TotalBufferCount;
	globus_off_t              DeadLockBufferCount;
	range_list_t            * StorRangeList;
	range_msg_list_t        * MsgRangeList;
};

/*
 * Only the source will call us on success.
 */
static void
transfer_data_eof_callback(void          * CallbackArg,
                           globus_result_t Result)
{
	transfer_data_t * transfer_data = (transfer_data_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&transfer_data->Lock);
	{
		/* Post our message. */
		if (Result != GLOBUS_SUCCESS)
		{
			if (transfer_data->Result == GLOBUS_SUCCESS)
				transfer_data->Result = Result;
		} else
			transfer_data->Eof = GLOBUS_TRUE;

		/* Wait the main thread. */
		globus_cond_signal(&transfer_data->Cond);
	}
	globus_mutex_unlock(&transfer_data->Lock);

	GlobusGFSHpssDebugExit();
}

static void
transfer_data_msg_range_received(transfer_data_t * TransferData,
                                 void            * Msg)
{
	char                              * buffer             = NULL;
	globus_off_t                        offset             = 0;
	globus_off_t                        length             = 0;
	range_msg_list_t                  * new_range_msg      = NULL;
	range_msg_list_t                  * range_msg          = NULL;
	data_ranges_msg_range_received_t  * msg_range_received = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Cast to our message. */
	msg_range_received = (data_ranges_msg_range_received_t *) Msg;

	globus_mutex_lock(&TransferData->Lock);
	{
		/* Peek at the next range. */
		range_list_peek(TransferData->StorRangeList, &offset, &length);

		/* If this is the range we have been waiting for... */
		if (offset == msg_range_received->Offset)
		{
			/* Delete this range. */
			range_list_delete(TransferData->StorRangeList,
			                  msg_range_received->Offset,
			                  msg_range_received->Length);

			
			while (TransferData->MsgRangeList != NULL)
			{
				/* Peek at the next range. */
				range_list_peek(TransferData->StorRangeList, &offset, &length);

				/* If we don't have a thread out with that offset, we are done. */
				if (offset != TransferData->MsgRangeList->Msg.Offset)
					break;

				/* Delete this range. */
				range_list_delete(TransferData->StorRangeList,
				                  TransferData->MsgRangeList->Msg.Offset,
				                  TransferData->MsgRangeList->Msg.Length);

				/* Delete this msg. */
				range_msg = TransferData->MsgRangeList;
				TransferData->MsgRangeList = TransferData->MsgRangeList->Next;
				globus_free(range_msg);

				/* Decrease the dead lock count. */
				TransferData->DeadLockBufferCount--;
			}
		} else
		{
			/* Push this message on our list. */
			new_range_msg = (range_msg_list_t *) globus_calloc(1, sizeof(range_msg_list_t));

			new_range_msg->Msg.Offset = msg_range_received->Offset;
			new_range_msg->Msg.Length = msg_range_received->Length;

			if (TransferData->MsgRangeList == NULL)
			{
				TransferData->MsgRangeList = new_range_msg;
			} else if (TransferData->MsgRangeList->Msg.Offset > new_range_msg->Msg.Offset)
			{
				new_range_msg->Next = TransferData->MsgRangeList;
				TransferData->MsgRangeList = new_range_msg;
			} else
			{
				/* Find where to put the new message. */
				for (range_msg  = TransferData->MsgRangeList; 
				     range_msg->Next != NULL && range_msg->Next->Msg.Offset < new_range_msg->Msg.Offset;
				     range_msg  = range_msg->Next);

				new_range_msg->Next = range_msg->Next;
				range_msg->Next     = new_range_msg;
			}

			/* Increase the dead lock count. */
			TransferData->DeadLockBufferCount++;

			if (TransferData->TotalBufferCount == TransferData->DeadLockBufferCount)
			{
				/* Allocate a new buffer. */
				buffer_allocate_buffer(TransferData->BufferHandle,
				                       TransferData->PrivateBufID,
				                       &buffer,
				                       &length);

				/* Send this buffer to GridFTP. */
				gridftp_buffer(TransferData->GridFTP,
				               buffer,
				               0,
				               length);

				/* Increase the total buffer count. */
				TransferData->TotalBufferCount++;
			}
		}
	}
	globus_mutex_unlock(&TransferData->Lock);

	GlobusGFSHpssDebugExit();
}

static void
transfer_data_msg_recv(void          * CallbackArg,
                       msg_comp_id_t   DstMsgCompID,
                       msg_comp_id_t   SrcMsgCompID,
                       int             MsgType,
                       int             MsgLen,
                       void          * Msg)
{
	transfer_data_t * transfer_data = (transfer_data_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	switch (SrcMsgCompID)
	{
	case MSG_COMP_ID_TRANSFER_CONTROL:

		switch (MsgType)
		{
		case TRANSFER_CONTROL_MSG_TYPE_SHUTDOWN:

			globus_mutex_lock(&transfer_data->Lock);
			{
				/* Indicate that the control says we are done. */
				transfer_data->ControlComplete = GLOBUS_TRUE;
				/* Wake anyone that is waiting. */
				globus_cond_signal(&transfer_data->Cond);
			}
			globus_mutex_unlock(&transfer_data->Lock);

			break;
		default:
			globus_assert(0);
		}
		break;

	case MSG_COMP_ID_TRANSFER_DATA_RANGES:
		switch (MsgType)
		{
		case DATA_RANGES_MSG_TYPE_RANGE_RECEIVED:
			transfer_data_msg_range_received(transfer_data, Msg);
			break;
		default:
			break;
		}
		break;
	default:
		globus_assert(0);
	}

	GlobusGFSHpssDebugExit();
}

static globus_result_t
transfer_data_common_init(transfer_data_op_type_t    OpType,
                          msg_handle_t            *  MsgHandle,
                          globus_gfs_operation_t     Operation,
                          transfer_data_t         ** TransferData)
{
	globus_result_t result     = GLOBUS_SUCCESS;
	globus_size_t   block_size = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*TransferData = (transfer_data_t *) globus_calloc(1, sizeof(transfer_data_t));
	if (*TransferData == NULL)
	{
		result = GlobusGFSErrorMemory("transfer_data_t");
		goto cleanup;
	}

	/* Initialize the entries. */
	globus_mutex_init(&(*TransferData)->Lock, NULL);
	globus_cond_init(&(*TransferData)->Cond, NULL);
	(*TransferData)->OpType        = OpType;
	(*TransferData)->MsgHandle     = MsgHandle;
	(*TransferData)->MsgRegisterID = MSG_REGISTER_ID_NONE;

	/* Register to receive messages. */
	result = msg_register(MsgHandle,
	                      MSG_COMP_ID_TRANSFER_DATA_RANGES,
	                      MSG_COMP_ID_TRANSFER_DATA,
	                      transfer_data_msg_recv,
	                      *TransferData,
	                      &(*TransferData)->MsgRegisterID);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Get the server's block size. */
/* XXX this should be globus_gridftp_server_get_block_size()
 * but PIO requies that the block(buffer) size and stripe block size
 * be the same. We'll figure this out later.
 */
	globus_gridftp_server_get_stripe_block_size(Operation, &block_size);

	/* Allocate the buffer handle. */
	result = buffer_init(block_size, &(*TransferData)->BufferHandle);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Create a private handle. */
	(*TransferData)->PrivateBufID = buffer_create_private_list((*TransferData)->BufferHandle);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static globus_result_t
transfer_data_stor_init_modules(transfer_data_t            * TransferData,
                                globus_gfs_operation_t       Operation,
                                globus_gfs_transfer_info_t * TransferInfo)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the GridFTP handle. */
	result = gridftp_init(GRIDFTP_OP_TYPE_STOR,
	                      Operation,
	                      TransferInfo,
	                      TransferData->BufferHandle,
	                      TransferData->MsgHandle,
	                      transfer_data_eof_callback,
	                      TransferData,
	                      &TransferData->GridFTP);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Allocate the data_ranges object. */
	result = data_ranges_init(DATA_RANGE_MODE_RANGE_RECEIVED,
	                          TransferData->MsgHandle, 
	                          &TransferData->DataRanges);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Allocate the PIO data handle. */
	result = pio_data_init(PIO_DATA_OP_TYPE_STOR,
	                       TransferData->BufferHandle,
	                       TransferData->MsgHandle,
	                       transfer_data_eof_callback,
	                       TransferData,
	                       &TransferData->PioData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Set the buffer passing functions.
	 */

	/* Send buffers from GridFTP to the data ranges module. */
	gridftp_set_buffer_pass_func(TransferData->GridFTP,
	                             data_ranges_buffer,
	                             TransferData->DataRanges);

	/* Send buffers to Pio Data. */
	data_ranges_set_buffer_pass_func(TransferData->DataRanges,
	                                 pio_data_buffer,
	                                 TransferData->PioData);

	/* Send buffers back to the start. */
	pio_data_set_buffer_pass_func(TransferData->PioData,
	                              gridftp_buffer,
	                              TransferData->GridFTP);

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
transfer_data_stor_init(msg_handle_t               *  MsgHandle,
                        globus_gfs_operation_t        Operation,
                        globus_gfs_transfer_info_t *  TransferInfo,
                        transfer_data_t            ** TransferData)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = transfer_data_common_init(TRANSFER_DATA_OP_TYPE_STOR,
	                                   MsgHandle,
	                                   Operation,
	                                   TransferData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Create the range list. */
	result = range_list_init(&(*TransferData)->StorRangeList);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Populate the range list. */
	result = range_list_fill_stor_range((*TransferData)->StorRangeList, TransferInfo);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Create the modules. */
	result = transfer_data_stor_init_modules(*TransferData, Operation, TransferInfo);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Tell the control side that we are ready. */
	result = msg_send(MsgHandle,
	                  MSG_COMP_ID_TRANSFER_CONTROL,
	                  MSG_COMP_ID_TRANSFER_DATA,
	                  TRANSFER_DATA_MSG_TYPE_READY,
	                  0,
	                  NULL);

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

static globus_result_t
transfer_data_retr_init_modules(transfer_data_t            * TransferData,
                                globus_gfs_operation_t       Operation,
                                globus_gfs_transfer_info_t * TransferInfo)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the PIO data handle. */
	result = pio_data_init(PIO_DATA_OP_TYPE_RETR,
	                       TransferData->BufferHandle,
	                       TransferData->MsgHandle,
	                       transfer_data_eof_callback,
	                       TransferData,
	                       &TransferData->PioData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Allocate the GridFTP handle. */
	result = gridftp_init(GRIDFTP_OP_TYPE_RETR,
	                      Operation,
	                      TransferInfo,
	                      TransferData->BufferHandle,
	                      TransferData->MsgHandle,
	                      transfer_data_eof_callback,
	                      TransferData,
	                      &TransferData->GridFTP);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Set the buffer passing functions. */
	pio_data_set_buffer_pass_func(TransferData->PioData,
	                              gridftp_buffer,
	                              TransferData->GridFTP);

	gridftp_set_buffer_pass_func(TransferData->GridFTP,
	                             pio_data_buffer,
	                             TransferData->PioData);

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
transfer_data_retr_init(msg_handle_t               *  MsgHandle,
                        globus_gfs_operation_t        Operation,
                        globus_gfs_transfer_info_t *  TransferInfo,
                        transfer_data_t            ** TransferData)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = transfer_data_common_init(TRANSFER_DATA_OP_TYPE_RETR,
	                                   MsgHandle,
	                                   Operation,
	                                   TransferData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Create the modules. */
	result = transfer_data_retr_init_modules(*TransferData, Operation, TransferInfo);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Tell the control side that we are ready. */
	result = msg_send(MsgHandle,
	                  MSG_COMP_ID_TRANSFER_CONTROL,
	                  MSG_COMP_ID_TRANSFER_DATA,
	                  TRANSFER_DATA_MSG_TYPE_READY,
	                  0,
	                  NULL);

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

static globus_result_t
transfer_data_cksm_init_modules(transfer_data_t * TransferData)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the PIO data handle. */
	result = pio_data_init(PIO_DATA_OP_TYPE_RETR,
	                       TransferData->BufferHandle,
	                       TransferData->MsgHandle,
	                       transfer_data_eof_callback,
	                       TransferData,
	                       &TransferData->PioData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Allocate the checksum handle. */
	result = checksum_init(TransferData->BufferHandle,
	                       transfer_data_eof_callback,
	                       TransferData,
	                       &TransferData->Checksum);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Allocate the data_ranges object. */
	result = data_ranges_init(DATA_RANGE_MODE_RANGE_COMPLETE,
	                          TransferData->MsgHandle, 
	                          &TransferData->DataRanges);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Set the buffer passing functions. */
	pio_data_set_buffer_pass_func(TransferData->PioData,
	                              checksum_buffer,
	                              TransferData->Checksum);
	checksum_set_buffer_pass_func(TransferData->Checksum,
	                              data_ranges_buffer,
	                              TransferData->DataRanges);
	data_ranges_set_buffer_pass_func(TransferData->DataRanges,
	                                 pio_data_buffer,
	                                 TransferData->PioData);

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
transfer_data_cksm_init(msg_handle_t               *  MsgHandle,
                        globus_gfs_operation_t        Operation,
                        globus_gfs_command_info_t  *  CommandInfo,
                        transfer_data_t            ** TransferData)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = transfer_data_common_init(TRANSFER_DATA_OP_TYPE_CKSM,
	                                   MsgHandle,
	                                   Operation,
	                                   TransferData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Create the modules. */
	result = transfer_data_cksm_init_modules(*TransferData);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Tell the control side that we are ready. */
	result = msg_send(MsgHandle,
	                  MSG_COMP_ID_TRANSFER_CONTROL,
	                  MSG_COMP_ID_TRANSFER_DATA,
	                  TRANSFER_DATA_MSG_TYPE_READY,
	                  0,
	                  NULL);

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

void
transfer_data_destroy(transfer_data_t * TransferData)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (TransferData != NULL)
	{
		/* Unregister to reveive messages. */
		msg_unregister(TransferData->MsgHandle, TransferData->MsgRegisterID);

		/* Destroy the buffer handle. */
		buffer_destroy(TransferData->BufferHandle);
		/* Destroy the pio data handle. */
		pio_data_destroy(TransferData->PioData);
		/* Destroy the GridFTP handle. */
		gridftp_destroy(TransferData->GridFTP);
		/* Destroy the checksum handle. */
		checksum_destroy(TransferData->Checksum);
		/* Destroy the data_ranges handle. */
		data_ranges_destroy(TransferData->DataRanges);
		/* Destroy the lock. */
		globus_mutex_destroy(&TransferData->Lock);
		/* Destroy the condition. */
		globus_cond_destroy(&TransferData->Cond);
		/* Free the handle. */
		globus_free(TransferData);
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
transfer_data_run(transfer_data_t * TransferData)
{
	int               x             = 0;
	char            * buffer        = NULL;
	globus_off_t      buffer_length = 0;
	globus_bool_t     should_flush  = GLOBUS_FALSE;
	globus_result_t   result        = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/*
	 * We must supply the initial buffers.
	 */
/* XXX clean this up so the # of buffers to create depends explicitly
 * on the modules added else where.
 */

	/* For each module. */
	for (x = 0; x < 2; x++)
	{
		/* Allocate the buffer. */
		result = buffer_allocate_buffer(TransferData->BufferHandle,
		                                -1,
		                                &buffer,
		                                &buffer_length);
		if (result != GLOBUS_SUCCESS)
			break;

		switch (TransferData->OpType)
		{
		case TRANSFER_DATA_OP_TYPE_STOR:
			/* Send it to the first module. */
			gridftp_buffer(TransferData->GridFTP, buffer, 0, buffer_length);
			break;
		case TRANSFER_DATA_OP_TYPE_RETR:
		case TRANSFER_DATA_OP_TYPE_CKSM:
			/* Send it to the first module. */
			pio_data_buffer(TransferData->PioData,
			                buffer,
			                0,
			                buffer_length);
			break;
		}

		/* Increment the total buffer count. */
		TransferData->TotalBufferCount++;
	}

	if (result == GLOBUS_SUCCESS)
	{
		globus_mutex_lock(&TransferData->Lock);
		{
			while (GLOBUS_TRUE)
			{
				if (TransferData->Eof == GLOBUS_TRUE)
					break;
				if (TransferData->Result != GLOBUS_SUCCESS)
					break;
				if (TransferData->ControlComplete == GLOBUS_TRUE)
					break;

				/* Wait for an event. */
				globus_cond_wait(&TransferData->Cond, &TransferData->Lock);
			}

			if (TransferData->Result == GLOBUS_SUCCESS)
				should_flush = GLOBUS_TRUE;
		}
		globus_mutex_unlock(&TransferData->Lock);

		/* Flush the handles. */
		if (should_flush == GLOBUS_TRUE)
		{
			switch (TransferData->OpType)
			{
			case TRANSFER_DATA_OP_TYPE_STOR:
				gridftp_flush(TransferData->GridFTP);
				data_ranges_flush(TransferData->DataRanges);
				pio_data_flush(TransferData->PioData);
				break;
			case TRANSFER_DATA_OP_TYPE_RETR:
				pio_data_flush(TransferData->PioData);
				gridftp_flush(TransferData->GridFTP);
				break;
			case TRANSFER_DATA_OP_TYPE_CKSM:
				pio_data_flush(TransferData->PioData);
				checksum_flush(TransferData->Checksum);
				data_ranges_flush(TransferData->DataRanges);
				break;
			}
		}
	}

	/* Now make sure they are completely stopped. */
	if (TransferData->PioData != NULL)
		pio_data_stop(TransferData->PioData);

	if (TransferData->Checksum != NULL)
		checksum_stop(TransferData->Checksum);

	if (TransferData->GridFTP != NULL)
		gridftp_stop(TransferData->GridFTP);

	if (TransferData->DataRanges != NULL)
		data_ranges_stop(TransferData->DataRanges);

	GlobusGFSHpssDebugExit();
	return TransferData->Result;
}

globus_result_t
transfer_data_checksum(transfer_data_t *  TransferData,
                       char            ** Checksum)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	result = checksum_get_sum(TransferData->Checksum, Checksum);

	GlobusGFSHpssDebugExit();
	return result;
}


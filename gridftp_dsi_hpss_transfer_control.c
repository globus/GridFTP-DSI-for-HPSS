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
 * Local includes.
 */
#include "gridftp_dsi_hpss_transfer_control.h"
#include "gridftp_dsi_hpss_transfer_data.h"
#include "gridftp_dsi_hpss_pio_control.h"
#ifdef REMOTE
#include "gridftp_dsi_hpss_ipc_control.h"
#endif /* REMOTE */
#include "gridftp_dsi_hpss_range_list.h"
#include "gridftp_dsi_hpss_gridftp.h"
#include "gridftp_dsi_hpss_session.h"
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_msg.h"

typedef enum {
	TRANSFER_CONTROL_WAIT_FOR_DATA_READY    = 0x01,
	TRANSFER_CONTROL_TRANSFER_RUNNING       = 0x02,
	TRANSFER_CONTROL_WAIT_FOR_DATA_COMPLETE = 0x04,
	TRANSFER_CONTROL_WAIT_FOR_PIO_COMPLETE  = 0x08,
	TRANSFER_CONTROL_COMPLETE               = 0x10,
} transfer_control_state_t;

typedef enum {
	TRANSFER_CONTROL_OP_TYPE_STOR,
	TRANSFER_CONTROL_OP_TYPE_RETR,
	TRANSFER_CONTROL_OP_TYPE_CKSM,
} transfer_control_op_type_t;

struct transfer_control {
	transfer_control_op_type_t   OpType;
	globus_size_t                StripeBlockSize;
	msg_handle_t               * MsgHandle;
	range_list_t               * TransferRangeList;
	pio_control_t              * PioControl;
	globus_result_t              Result;
	transfer_control_state_t     State;
	globus_mutex_t               Lock;
	msg_register_id_t            MsgRegisterID;
};

static void
transfer_control_event_range_complete(void            * CallbackArg,
                                      globus_result_t   Result)
{
	globus_result_t      result           = GLOBUS_SUCCESS;
	transfer_control_t * transfer_control = NULL;
	transfer_control_complete_msg_t complete_msg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Cast to our handle. */
	transfer_control = (transfer_control_t *) CallbackArg;

	globus_mutex_lock(&transfer_control->Lock);
	{
		/* Save the error. */
		if (transfer_control->Result == GLOBUS_SUCCESS)
			transfer_control->Result = Result;

		/* If the data side has not reported in. */
		if (transfer_control->State == TRANSFER_CONTROL_TRANSFER_RUNNING)
		{
			/* Update our state. */
			transfer_control->State = TRANSFER_CONTROL_WAIT_FOR_DATA_COMPLETE;
			goto unlock;
		}

		/* Update our state. */
		transfer_control->State = TRANSFER_CONTROL_COMPLETE;

		/* Prepare the completion message. */
		complete_msg.Result = transfer_control->Result;

		/* Send the message. */
		result = msg_send(transfer_control->MsgHandle,
		                  MSG_COMP_ID_ANY,
		                  MSG_COMP_ID_TRANSFER_CONTROL,
		                  TRANSFER_CONTROL_MSG_TYPE_COMPLETE,
		                  sizeof(transfer_control_complete_msg_t),
		                  &complete_msg);

		/* XXX */
		globus_assert(result == GLOBUS_SUCCESS);
	}
unlock:
	globus_mutex_unlock(&transfer_control->Lock);

	GlobusGFSHpssDebugExit();
}

static void
transfer_control_event_data_ready(transfer_control_t * TransferControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();


	/*
	 * If there is nothing to transfer...
	 */
	if (range_list_empty(TransferControl->TransferRangeList))
	{
		/* Update our state. */
		TransferControl->State = TRANSFER_CONTROL_WAIT_FOR_DATA_COMPLETE;

		/* Tell the data side to shutdown. */
		result = msg_send(TransferControl->MsgHandle,
		                  MSG_COMP_ID_TRANSFER_DATA,
		                  MSG_COMP_ID_TRANSFER_CONTROL,
		                  TRANSFER_CONTROL_MSG_TYPE_SHUTDOWN,
		                  0,
		                  NULL);

		/* XXX */
		globus_assert(result == GLOBUS_SUCCESS);

		goto cleanup;
	}

	/* Update our state. */
	TransferControl->State = TRANSFER_CONTROL_TRANSFER_RUNNING;

	/* Tell PIO to perform the transfer. */
	pio_control_transfer_ranges(TransferControl->PioControl,
	                            1,
	                            TransferControl->StripeBlockSize,
	                            TransferControl->TransferRangeList,
	                            transfer_control_event_range_complete,
	                            TransferControl);
cleanup:
	GlobusGFSHpssDebugExit();
}

static void
transfer_control_event_data_complete(transfer_control_t * TransferControl,
                                     globus_result_t      Result)
{
	globus_result_t                 result = GLOBUS_SUCCESS;
	transfer_control_complete_msg_t complete_msg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&TransferControl->Lock);
	{
		/* Save the error. */
		if (TransferControl->Result == GLOBUS_SUCCESS)
			TransferControl->Result = Result;

		/* If PIO is still running... */
		if (TransferControl->State == TRANSFER_CONTROL_TRANSFER_RUNNING)
		{
			/* Update the state. */
			TransferControl->State = TRANSFER_CONTROL_WAIT_FOR_PIO_COMPLETE;

			goto unlock;
		}

		/* Update the state. */
		TransferControl->State = TRANSFER_CONTROL_COMPLETE;

		/* Prepare the completion message. */
		complete_msg.Result = TransferControl->Result;

		/* Send the message. */
		result = msg_send(TransferControl->MsgHandle,
		                  MSG_COMP_ID_ANY,
		                  MSG_COMP_ID_TRANSFER_CONTROL,
		                  TRANSFER_CONTROL_MSG_TYPE_COMPLETE,
		                  sizeof(transfer_control_complete_msg_t),
		                  &complete_msg);

		/* XXX */
		globus_assert(result == GLOBUS_SUCCESS);
	}
unlock:
	globus_mutex_unlock(&TransferControl->Lock);

	GlobusGFSHpssDebugExit();
}

/*
 * Special single-node transfer completion path.
 * XXX move this to a message.
 */
void
transfer_control_data_complete(transfer_control_t * TransferControl,
                               globus_result_t      Result)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	transfer_control_event_data_complete(TransferControl, Result);

	GlobusGFSHpssDebugExit();
}

/*
 * Messages from Transfer Data.
 */
static void
transfer_control_transfer_data_msg(transfer_control_t * TransferControl,
                                   int                  MsgType,
                                   int                  MsgLen,
                                   void               * Msg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	switch (MsgType)
	{
	case TRANSFER_DATA_MSG_TYPE_READY:
		transfer_control_event_data_ready(TransferControl);
		break;
	default:
		globus_assert(0);
	}

	GlobusGFSHpssDebugExit();
}

#ifdef REMOTE
/*
 * On data process segfault, this is called with Result set.
 *
(gdb) print *Reply
$1 = {type = GLOBUS_GFS_OP_RECV, id = 1, code = 500, msg = 0x0, result = 32, info = {session = {
      session_arg = 0x0, username = 0x0, home_dir = 0x0}, data = {data_arg = 0x0, bi_directional = 0, ipv6 = 0,
      cs_count = 0, contact_strings = 0x0}, command = {command = 0, checksum = 0x0, created_dir = 0x0}, stat = {
      uid = 0, gid_count = 0, gid_array = 0x0, stat_count = 0, stat_array = 0x0}, transfer = {
      bytes_transferred = 0}}, op_info = 0x0}
*/
/*
 * IPC_CONTROL_MSG_TYPE_TRANSFER_EVENT are events that we registered for
 * that have been sent by the data node.IPC_CONTROL_MSG_TYPE_TRANSFER_COMPLETE
 * comes from data nodes regardless of whether we asked for it or not.
 * See gridftp_hpss_dsi_ipc_control.c ipc_control_event_callback() for more
 * details.
 */
static void
transfer_control_ipc_msg(transfer_control_t * TransferControl,
/*                         int                  NodeIndex, */
                         int                  MsgType,
                         int                  MsgLen,
                         void               * Msg)
{
	ipc_control_transfer_event_t    * transfer_event    = NULL;
	ipc_control_transfer_complete_t * transfer_complete = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

#ifdef NOT
	globus_assert(NodeIndex >= 0 && NodeIndex < TransferControl->DataNodeCount);
#endif /* NOT */

	switch (MsgType)
	{
	case IPC_CONTROL_MSG_TYPE_TRANSFER_EVENT:
/* Incoming ranges from data node(s). */
		transfer_event = (ipc_control_transfer_event_t *) Msg;
		break;

	case IPC_CONTROL_MSG_TYPE_TRANSFER_COMPLETE:
		transfer_complete = (ipc_control_transfer_complete_t *) Msg;

		transfer_control_data_complete(TransferControl, transfer_complete->Result);
#ifdef NOT
		globus_mutex_lock(&TransferControl->Lock);
		{
			/* Increase the thread count so cleanup doesn't happen before we exit. */
			TransferControl->ThreadCount++;

			/* Indicate that this node has completed. */
			TransferControl->DataNodes[0].Complete = GLOBUS_TRUE;
			TransferControl->DataNodes[0].Result = transfer_complete->Result;
			if (TransferControl->DataNodes[0].Result == GLOBUS_SUCCESS)
				TransferControl->DataNodes[0].Result = transfer_complete->Reply->result;

			/* Increment the completion count. */
			TransferControl->NodesDone++;
		}
		globus_mutex_unlock(&TransferControl->Lock);

		/* Update our state. */
		transfer_control_update_state(TransferControl);
#endif /* NOT */

		break;
	default:
		globus_assert(0);
	}
	GlobusGFSHpssDebugExit();
}
#endif /* REMOTE */

/*
 * All incoming messages.
 */
static void
transfer_control_msg_recv(void          * CallbackArg,
                          msg_comp_id_t   DstMsgCompID,
                          msg_comp_id_t   SrcMsgCompID,
                          int             MsgType,
                          int             MsgLen,
                          void          * Msg)
{
	transfer_control_t * transfer_control = (transfer_control_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	switch (SrcMsgCompID)
	{
	case MSG_COMP_ID_TRANSFER_DATA:
		transfer_control_transfer_data_msg(transfer_control, MsgType, MsgLen, Msg);
		break;
	case MSG_COMP_ID_IPC_CONTROL:
		transfer_control_ipc_msg(transfer_control, /*NodeIndex,*/ MsgType, MsgLen, Msg);
		break;
	default:
		globus_assert(0);
	}

	GlobusGFSHpssDebugExit();
}

static globus_result_t
transfer_control_common_init(transfer_control_msg_type_t    OpType,
                             msg_handle_t                *  MsgHandle,
                             transfer_control_t          ** TransferControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*TransferControl = (transfer_control_t *) globus_calloc(1, sizeof(transfer_control_t));
	if (*TransferControl == NULL)
	{
		result = GlobusGFSErrorMemory("transfer_control_t");
		goto cleanup;
	}

	/* Initialize the entries. */
	globus_mutex_init(&(*TransferControl)->Lock, NULL);
	(*TransferControl)->OpType        = OpType;
	(*TransferControl)->MsgHandle     = MsgHandle;
	(*TransferControl)->State         = TRANSFER_CONTROL_WAIT_FOR_DATA_READY;
	(*TransferControl)->MsgRegisterID = MSG_REGISTER_ID_NONE;

	/* Intialize the range list. Fill it later. */
	result = range_list_init(&(*TransferControl)->TransferRangeList);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Register to receive messages. */
	result = msg_register(MsgHandle,
	                      MSG_COMP_ID_IPC_CONTROL,
	                      MSG_COMP_ID_TRANSFER_CONTROL,
	                      transfer_control_msg_recv,
	                      *TransferControl,
	                      &(*TransferControl)->MsgRegisterID);

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
transfer_control_stor_init(msg_handle_t               *  MsgHandle,
                           session_handle_t           *  Session,
                           globus_gfs_operation_t        Operation,
                           globus_gfs_transfer_info_t *  TransferInfo,
                           transfer_control_t         ** TransferControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform all common initialization. */
	result = transfer_control_common_init(TRANSFER_CONTROL_OP_TYPE_STOR,
	                                      MsgHandle,
	                                      TransferControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Initialize the PIO control handle. */
	result = pio_control_stor_init(MsgHandle,
	                               Session,
	                               TransferInfo,
	                               &(*TransferControl)->PioControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Get the stripe block size. */
	globus_gridftp_server_get_stripe_block_size(Operation, 
	                                            &(*TransferControl)->StripeBlockSize);

	/* Fill the transfer range list. */
	result = range_list_fill_stor_range((*TransferControl)->TransferRangeList,
	                                      TransferInfo);

cleanup:
    GlobusGFSHpssDebugExit();
    return result;
}

globus_result_t
transfer_control_retr_init(msg_handle_t               *  MsgHandle,
                           globus_gfs_operation_t        Operation,
                           globus_gfs_transfer_info_t *  TransferInfo,
                           transfer_control_t         ** TransferControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform all common initialization. */
	result = transfer_control_common_init(TRANSFER_CONTROL_OP_TYPE_RETR,
	                                      MsgHandle,
	                                      TransferControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Initialize the PIO control handle. */
	result = pio_control_retr_init(MsgHandle,
	                               TransferInfo,
	                               &(*TransferControl)->PioControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Get the stripe block size. */
	globus_gridftp_server_get_stripe_block_size(Operation, 
	                                            &(*TransferControl)->StripeBlockSize);

	/* Fill the transfer range list. */
	result = range_list_fill_retr_range((*TransferControl)->TransferRangeList,
	                                      TransferInfo);

cleanup:
    GlobusGFSHpssDebugExit();
    return result;
}

globus_result_t
transfer_control_cksm_init(msg_handle_t              *  MsgHandle,
                           globus_gfs_operation_t       Operation,
                           globus_gfs_command_info_t *  CommandInfo,
                           transfer_control_t        ** TransferControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform all common initialization. */
	result = transfer_control_common_init(TRANSFER_CONTROL_OP_TYPE_CKSM,
	                                      MsgHandle,
	                                      TransferControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Initialize the PIO control handle. */
	result = pio_control_cksm_init(MsgHandle,
	                               CommandInfo,
	                               &(*TransferControl)->PioControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Get the stripe block size. */
	globus_gridftp_server_get_stripe_block_size(Operation, 
	                                            &(*TransferControl)->StripeBlockSize);
	/* Fill the transfer range list. */
	result = range_list_fill_cksm_range((*TransferControl)->TransferRangeList,
	                                      CommandInfo);

cleanup:
    GlobusGFSHpssDebugExit();
    return result;
}

void
transfer_control_destroy(transfer_control_t * TransferControl)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (TransferControl != NULL)
	{
		/* Unregister to receive messages. */
		msg_unregister(TransferControl->MsgHandle, TransferControl->MsgRegisterID);

		/* Destroy the PIO control handle. */
		pio_control_destroy(TransferControl->PioControl);

/*
 * Temp workaround to make sure the thread that called us isn't still 
 * holding this lock.
 */
globus_mutex_lock(&TransferControl->Lock);
globus_mutex_unlock(&TransferControl->Lock);
		/* Dellocate the lock. */
		globus_mutex_destroy(&TransferControl->Lock);

		/* Destroy the transfer range list. */
		range_list_destroy(TransferControl->TransferRangeList);

		/* Deallocate our handle. */
		globus_free(TransferControl);
	}

    GlobusGFSHpssDebugExit();
}

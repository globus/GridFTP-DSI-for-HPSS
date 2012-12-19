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
#include "gridftp_dsi_hpss_pio_control.h"
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_msg.h"


struct pio_control {
	int                    FileFD;
	int                    DataNodeCount;
	unsigned32             FileStripeWidth;
	hpss_pio_operation_t   OperationType;
	msg_handle_t         * MsgHandle;

	struct {
		hpss_pio_grp_t StripeGroup;
		globus_off_t   Offset;
		globus_off_t   Length;

		pio_control_transfer_range_callback_t Callback;
		void                                * CallbackArg;
	} PioExecute;
};

static void *
pio_control_execute_thread(void * Arg);

static globus_result_t
pio_control_open_file_for_writing(pio_control_t * PioControl,
                                  char          * Pathname,
                                  globus_off_t    AllocSize,
                                  globus_bool_t   Truncate,
                                  int             FamilyID,
                                  int             CosID)
{
	int                     oflags      = 0;
	globus_off_t            file_length = 0;
	globus_result_t         result      = GLOBUS_SUCCESS;
	hpss_cos_hints_t        hints_in;
	hpss_cos_hints_t        hints_out;
	hpss_cos_priorities_t   priorities;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the hints in. */
	memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the hints out. */
	memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the priorities. */
	memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

	/*
 	* If this is a new file we need to determine the class of service
 	* by either:
 	*  1) set explicitly within the session handle or
 	*  2) determined by the size of the incoming file
 	*/
	if (Truncate == GLOBUS_TRUE)
	{
		/* Get our preferened cos id. */
		if (CosID != -1)
		{
			hints_in.COSId = CosID;
			priorities.COSIdPriority = REQUIRED_PRIORITY;
		} 

		if (AllocSize != 0)
		{
			/*
			 * Use the ALLO size.
			 */
			file_length = AllocSize;
			CONVERT_LONGLONG_TO_U64(file_length, hints_in.MinFileSize);
			CONVERT_LONGLONG_TO_U64(file_length, hints_in.MaxFileSize);
			priorities.MinFileSizePriority = REQUIRED_PRIORITY;
			priorities.MaxFileSizePriority = REQUIRED_PRIORITY;
		}

		/* Get our preferred family id. */
		if (FamilyID != -1)
		{
			hints_in.FamilyId = FamilyID;
			priorities.FamilyIdPriority = REQUIRED_PRIORITY;
		}
	}

	/* Determine the open flags. */
	oflags = O_WRONLY;
	if (Truncate == GLOBUS_TRUE)
		oflags |= O_CREAT|O_TRUNC;

	/* Open the HPSS file. */
	PioControl->FileFD = hpss_Open(Pathname,
                    	           oflags,
                    	           S_IRUSR|S_IWUSR,
                    	           &hints_in,
                    	           &priorities,
                    	           &hints_out);
	if (PioControl->FileFD < 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Open", -(PioControl->FileFD));
		goto cleanup;
	}

	/* Copy out the file stripe width. */
	PioControl->FileStripeWidth = hints_out.StripeWidth;

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
pio_control_open_file_for_reading(pio_control_t * PioControl,
                                  char          * Pathname)
{
	globus_result_t       result = GLOBUS_SUCCESS;
	hpss_cos_hints_t      hints_in;
	hpss_cos_hints_t      hints_out;
	hpss_cos_priorities_t priorities;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the hints in. */
	memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the hints out. */
	memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the priorities. */
	memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

	/* Open the HPSS file. */
	PioControl->FileFD = hpss_Open(Pathname,
                    	           O_RDONLY,
                    	           S_IRUSR|S_IWUSR,
                    	           &hints_in,
                    	           &priorities,
                    	           &hints_out);
	if (PioControl->FileFD < 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Open", -(PioControl->FileFD));
		goto cleanup;
	}

	/* Copy out the file stripe width. */
	PioControl->FileStripeWidth = hints_out.StripeWidth;

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
pio_control_common_init(hpss_pio_operation_t    OperationType,
                        msg_handle_t         *  MsgHandle,
                        pio_control_t        ** PioControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*PioControl = (pio_control_t *) globus_calloc(1, sizeof(pio_control_t));
	if (*PioControl == NULL)
	{
		result = GlobusGFSErrorMemory("pio_control_t");
		goto cleanup;
	}

	/* Initialize entries. */
	(*PioControl)->OperationType = OperationType;
	(*PioControl)->MsgHandle     = MsgHandle;
	(*PioControl)->FileFD        = -1;
	(*PioControl)->DataNodeCount = 1;

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
pio_control_stor_init(msg_handle_t               *  MsgHandle,
                      session_handle_t           *  Session,
                      globus_gfs_transfer_info_t *  TransferInfo,
                      pio_control_t              ** PioControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = pio_control_common_init(HPSS_PIO_WRITE, MsgHandle, PioControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Open the file. */
	result = pio_control_open_file_for_writing(*PioControl,
	                                           TransferInfo->pathname,
	                                           TransferInfo->alloc_size,
	                                           TransferInfo->truncate,
	                                           session_pref_get_family_id(Session),
	                                           session_pref_get_cos_id(Session));
cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

globus_result_t
pio_control_retr_init(msg_handle_t               *  MsgHandle,
                      globus_gfs_transfer_info_t *  TransferInfo,
                      pio_control_t              ** PioControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = pio_control_common_init(HPSS_PIO_READ, MsgHandle, PioControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Open the file. */
	result = pio_control_open_file_for_reading(*PioControl, TransferInfo->pathname);

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

globus_result_t
pio_control_cksm_init(msg_handle_t              *  MsgHandle,
                      globus_gfs_command_info_t *  CommandInfo,
                      pio_control_t             ** PioControl)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Perform the common init. */
	result = pio_control_common_init(HPSS_PIO_READ, MsgHandle, PioControl);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Open the file. */
	result = pio_control_open_file_for_reading(*PioControl, CommandInfo->pathname);

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

void
pio_control_destroy(pio_control_t * PioControl)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (PioControl != NULL)
	{
		/* Close the HPSS file. */
		if (PioControl->FileFD != -1)
			hpss_Close(PioControl->FileFD);

		globus_free(PioControl);
	}

	GlobusGFSHpssDebugExit();
}

void
pio_control_transfer_range(pio_control_t                       * PioControl,
                           unsigned32                            ClntStripeWidth,
                           globus_off_t                          StripeBlockSize,
                           globus_off_t                          Offset,
                           globus_off_t                          Length,
                           pio_control_transfer_range_callback_t Callback,
                           void                                * CallbackArg)
{
	int                 retval        = 0;
	int                 node_id       = 0;
	char                int_buf[16];
	void              * group_buffer  = NULL;
	unsigned int        buffer_length = 0;
	globus_bool_t       pio_started   = GLOBUS_FALSE;
	globus_result_t     result        = GLOBUS_SUCCESS;
	hpss_pio_params_t   pio_params;
	globus_thread_t     thread_id;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	PioControl->PioExecute.Offset      = Offset;
	PioControl->PioExecute.Length      = Length;
	PioControl->PioExecute.Callback    = Callback;
	PioControl->PioExecute.CallbackArg = CallbackArg;

	/*
	 * Don't use HPSS_PIO_HANDLE_GAP, it's bugged in HPSS 7.4.
	 */
	pio_params.Operation       = PioControl->OperationType;
	pio_params.ClntStripeWidth = ClntStripeWidth;
	pio_params.BlockSize       = StripeBlockSize;
	pio_params.FileStripeWidth = PioControl->FileStripeWidth;
	pio_params.IOTimeOutSecs   = 0;
	pio_params.Transport       = HPSS_PIO_MVR_SELECT;
	pio_params.Options         = 0;

	/* Now call the start routine. */
	retval = hpss_PIOStart(&pio_params, &PioControl->PioExecute.StripeGroup);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_PIOStart", -retval);
		goto cleanup;
	}

	/* Indicate that pio has been started. */
	pio_started = GLOBUS_TRUE;

	/* Export the stripe group for the caller. */
	retval = hpss_PIOExportGrp(PioControl->PioExecute.StripeGroup,
	                           &group_buffer,
	                           &buffer_length);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_PIOExportGrp", -retval);
		goto cleanup;
	}

	for (node_id = 0; node_id < PioControl->DataNodeCount; node_id++)
	{
		/* Encode this node's stripe index. */
		snprintf(int_buf, sizeof(int_buf), "%u", node_id);

		/* Send the stripe index message to the pio data node. */
		result = msg_send(PioControl->MsgHandle,
		                  node_id,
		                  MSG_ID_PIO_DATA,
		                  MSG_ID_PIO_CONTROL,
		                  PIO_CONTROL_MSG_TYPE_STRIPE_INDEX,
		                  sizeof(int_buf),
		                  int_buf);

		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Send the stripe group message to the pio data node. */
		result = msg_send(PioControl->MsgHandle,
		                  node_id,
		                  MSG_ID_PIO_DATA,
		                  MSG_ID_PIO_CONTROL,
		                  PIO_CONTROL_MSG_TYPE_STRIPE_GROUP,
		                  buffer_length,
		                  group_buffer);

		if (result != GLOBUS_SUCCESS)
			goto cleanup;
	}

/* XXX Do I free group_buffer or do they? */

	/* Launch the pio execute thread. */
	retval = globus_thread_create(&thread_id,
                                  NULL,
                                  pio_control_execute_thread,
                                  PioControl);

	if (retval != 0)
		result = GlobusGFSErrorSystemError("globus_thread_create", retval);

cleanup:
	/* Release the group buffer. */
	if (group_buffer != NULL)
		free(group_buffer);

	if (result != GLOBUS_SUCCESS)
	{
		/* Inform the caller. */
		Callback(CallbackArg, result);

		/* Shutdown PIO. */
		if (pio_started == GLOBUS_TRUE)
			hpss_PIOEnd(PioControl->PioExecute.StripeGroup);

		GlobusGFSHpssDebugExitWithError();
		return;
	}

	GlobusGFSHpssDebugExit();
}

static void *
pio_control_execute_thread(void * Arg)
{

	int                  retval      = 0;
	globus_result_t      result      = GLOBUS_SUCCESS;
	pio_control_t      * pio_control = NULL;
	hpss_pio_gapinfo_t   gap_info;
	u_signed64           bytes_moved;
	u_signed64           offset;
	u_signed64           length;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Make sure we got our handle. */
	globus_assert(Arg != NULL);
	/* Cast the handle. */
	pio_control = (pio_control_t *)Arg;

	/* Convert to HPSS 64. */
	CONVERT_LONGLONG_TO_U64(pio_control->PioExecute.Offset, offset);
	CONVERT_LONGLONG_TO_U64(pio_control->PioExecute.Length, length);

	/* Initialize bytes_moved. */
	bytes_moved = cast64(0);

	do {
		offset = add64m(offset, bytes_moved);
		length = sub64m(length, bytes_moved);
		bytes_moved = cast64(0);

		/* Call pio execute. */
		retval = hpss_PIOExecute(pio_control->FileFD,
		                         offset,
		                         length,
		                         pio_control->PioExecute.StripeGroup,
		                         &gap_info,
		                         &bytes_moved);

	} while (retval == 0 && !eq64(bytes_moved, length));

	if (retval != 0)
		result = GlobusGFSErrorSystemError("hpss_PIOExecute", -retval);

	/* Stop PIO */
	retval = hpss_PIOEnd(pio_control->PioExecute.StripeGroup);
	if (retval != 0 && result == GLOBUS_SUCCESS)
		result = GlobusGFSErrorSystemError("hpss_PIOEnd", -retval);

	/* Report back to the caller that we are done. */
	pio_control->PioExecute.Callback(pio_control->PioExecute.CallbackArg, result);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return NULL;
	}

	GlobusGFSHpssDebugExit();
	return NULL;
}

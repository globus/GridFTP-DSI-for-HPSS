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
#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>
#include <globus_callback.h>
#include <globus_time.h>

/*
 * HPSS includes.
 */
#include <hpss_api.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_transfer_control.h"
#include "gridftp_dsi_hpss_transfer_data.h"
#include "gridftp_dsi_hpss_data_ranges.h"
#include "gridftp_dsi_hpss_commands.h"
#include "gridftp_dsi_hpss_checksum.h"
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

enum {
	GLOBUS_GFS_HPSS_CMD_SITE_SETCOS = GLOBUS_GFS_MIN_CUSTOM_CMD,
	GLOBUS_GFS_HPSS_CMD_SITE_LSCOS,
	GLOBUS_GFS_HPSS_CMD_SITE_SETFAM,
	GLOBUS_GFS_HPSS_CMD_SITE_LSFAM,
	GLOBUS_GFS_HPSS_CMD_SITE_HARDLINKFROM,
	GLOBUS_GFS_HPSS_CMD_SITE_HARDLINKTO,
	GLOBUS_GFS_HPSS_CMD_SITE_STAGE,
};

typedef struct marker_handle {
	globus_mutex_t             Lock;
	globus_off_t               TotalBytes;
	globus_gfs_operation_t     Operation;
	msg_register_id_t          MsgRegisterID;
	globus_callback_handle_t   CallbackHandle;
	msg_handle_t             * MsgHandle;
} marker_handle_t;

globus_result_t
commands_init(globus_gfs_operation_t    Operation)
{
	globus_result_t result = GLOBUS_SUCCESS;
 
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/*
	 * Add our local commands.
	 */
	result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE SETCOS",
	                 GLOBUS_GFS_HPSS_CMD_SITE_SETCOS,
	                 3,
	                 3,
	                 "SITE SETCOS <sp> cos: Set the HPSS class of service ('default' to reset)",
	                 GLOBUS_FALSE,
	                 0);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to add custom 'SITE SETCOS' command", result);
		goto cleanup;
	}

	result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE LSCOS",
	                 GLOBUS_GFS_HPSS_CMD_SITE_LSCOS,
	                 2,
	                 2,
	                 "SITE LSCOS: List the allowed HPSS class of services.",
	                 GLOBUS_FALSE,
	                 0);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to add custom 'SITE LSCOS' command", result);
		goto cleanup;
	}

	result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE SETFAM",
	                 GLOBUS_GFS_HPSS_CMD_SITE_SETFAM,
	                 3,
	                 3,
	                 "SITE SETFAM <sp> family: Set the HPSS tape family ('default' to reset)",
	                 GLOBUS_FALSE,
	                 0);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to add custom 'SITE SETFAM' command", result);
		goto cleanup;
	}

	result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE LSFAM",
	                 GLOBUS_GFS_HPSS_CMD_SITE_LSFAM,
	                 2,
	                 2,
	                 "SITE LSFAM: List the allowed HPSS families.",
	                 GLOBUS_FALSE,
	                 0);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to add custom 'SITE LSFAM' command", result);
		goto cleanup;
	}

	result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE HARDLINKFROM",
	                 GLOBUS_GFS_HPSS_CMD_SITE_HARDLINKFROM,
	                 3,
	                 3,
	                 "SITE HARDLINKFROM <sp> reference-path",
	                 GLOBUS_TRUE,
	                 0);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to add custom 'SITE HARDLINKTO' command", result);
		goto cleanup;
	}

	result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE HARDLINKTO",
	                 GLOBUS_GFS_HPSS_CMD_SITE_HARDLINKTO,
	                 3,
	                 3,
	                 "SITE HARDLINKTO <sp> link-path",
	                 GLOBUS_TRUE,
	                 0);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to add custom 'SITE HARDLINKTO' command", result);
		goto cleanup;
	}

	result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE STAGE",
	                 GLOBUS_GFS_HPSS_CMD_SITE_STAGE,
	                 4,
	                 4,
	                 "SITE STAGE <sp> timeout <sp> path",
	                 GLOBUS_TRUE,
	                 0);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to add custom 'SITE STAGE' command", result);
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

static globus_result_t
commands_chgrp(char * Pathname, char * GroupName)
{
	int               retval = 0;
	gid_t             gid    = 0;
	globus_result_t   result = GLOBUS_SUCCESS;
	globus_gfs_stat_t gfs_stat_buf;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Stat it, make sure it exists. */
	result = misc_gfs_stat(Pathname, GLOBUS_FALSE, &gfs_stat_buf);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* If the given group is not a digit... */
	if (!isdigit(*GroupName))
	{
		/* Convert to the gid. */
		result = misc_groupname_to_gid(GroupName, &gid);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;
	} else
	{
		/* Convert to the gid. */
		gid = atoi(GroupName);
	}

	/* Now change the group. */
	retval = hpss_Chown(Pathname, gfs_stat_buf.uid, gid);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Chgrp", -retval);
		goto cleanup;
	}

cleanup:
	/* clean up the statbuf. */
	misc_destroy_gfs_stat(&gfs_stat_buf);

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static globus_result_t
commands_stage_file(char          * Path,
                    int             Timeout,
                    globus_bool_t * Staged,
                    globus_bool_t * TapeOnly)
{
	int               retval     = 0;
	time_t            start_time = time(NULL);
	hpss_reqid_t      reqid      = 0;
	globus_bool_t     archived   = GLOBUS_TRUE;
	globus_result_t   result     = GLOBUS_SUCCESS;
	globus_abstime_t  timeout;
	globus_mutex_t    mutex;
	globus_cond_t     cond;
	hpssoid_t         bitfile_id;
	u_signed64        size;
	globus_gfs_stat_t gfs_stat_buf;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Staged = GLOBUS_FALSE;

	globus_mutex_init(&mutex, NULL);
	globus_cond_init(&cond, NULL);

	/* Stat the object. */
	result = misc_gfs_stat(Path, GLOBUS_FALSE, &gfs_stat_buf);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Check if it is a file. */
	if (!S_ISREG(gfs_stat_buf.mode))
	{
		archived = GLOBUS_FALSE;
		goto cleanup;
	}

	/* Check if it is archived. */
	result = misc_file_archived(Path, &archived, TapeOnly);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	if (archived == GLOBUS_FALSE || *TapeOnly == GLOBUS_TRUE)
		goto cleanup;

	/*
	 * We use hpss_StageCallBack() so that we do not block while the
	 * stage completes. We could use hpss_Open(O_NONBLOCK) and then
	 * hpss_Stage(BFS_ASYNCH_CALL) but then we block in hpss_Close().
	 */

	/*
	 * We could optimize by saving these requests in a list and checking it
	 * before we issue another request. This helps clients (uberftp) that
	 * will request the file stage every second until complete.
	 */

	CONVERT_LONGLONG_TO_U64(gfs_stat_buf.size, size);
	retval = hpss_StageCallBack(Path, cast64m(0), size, 0, NULL, BFS_STAGE_ALL, &reqid, &bitfile_id);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_StageCallBack()", -retval);
		goto cleanup;
	}

	/* Now wait for the given about of time or the file staged. */
	while ((time(NULL) - start_time) < Timeout && archived == GLOBUS_TRUE)
	{
		globus_mutex_lock(&mutex);
		{
			/* Set our timeout for 1 second in the future. */
			GlobusTimeAbstimeSet(timeout, 1, 0);

			/* Now wait. */
			globus_cond_timedwait(&cond, &mutex, &timeout);
		}
		globus_mutex_unlock(&mutex);

		/* Check if it is archived. */
		result = misc_file_archived(Path, &archived, TapeOnly);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;
	}

cleanup:
	*Staged = !archived;

	globus_mutex_destroy(&mutex);
	globus_cond_destroy(&cond);

	/* Release the stat memory. */
	misc_destroy_gfs_stat(&gfs_stat_buf);

	GlobusGFSHpssDebugExit();
	return result;
}
 
/*
 * The returned CommandOutput is static, do not free.
 */
void
commands_setcos(session_handle_t *  SessionHandle,
                char             *  Cos,
                char             ** CommandOutput)
{
	int           cos_id      = SESSION_NO_COS_ID;
	globus_bool_t can_use_cos = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the returned command output. */
	*CommandOutput = NULL;

	/*
	 * Check for the 'default' cos.
	 */
	if (strcasecmp(Cos, "default") == 0)
	{
		/* Unset any early cos settings. */
		session_pref_set_cos_id(SessionHandle, SESSION_NO_COS_ID);
		goto cleanup;
	}

	/* Get the cos id. */
	cos_id = session_get_cos_id(SessionHandle, Cos);
	if (cos_id == SESSION_NO_COS_ID)
	{
		*CommandOutput = "550 That class of service does not exist\r\n";
		goto cleanup;
	}

	/* Check if the user is allowed to use this COS. */
	can_use_cos = session_can_user_use_cos(SessionHandle, cos_id);
	if (can_use_cos == GLOBUS_FALSE)
	{
		*CommandOutput = "550 Not permitted to use this class of service\r\n";
		goto cleanup;
	}

	/* Save this cos id in our preferences. */
	session_pref_set_cos_id(SessionHandle, cos_id);

cleanup:
    GlobusGFSHpssDebugExit();
}

/*
 * CommandOutput must be free'd.
 */
globus_result_t
commands_lscos(session_handle_t *  SessionHandle,
               char             ** CommandOutput)
{
	char            * cos_list = NULL;
	char            * response = "250 Allowed COS: %s\r\n";
	globus_result_t   result   = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the command output. */
	*CommandOutput = NULL;

	result = session_get_user_cos_list(SessionHandle, &cos_list);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Construct the command output.
	 */
	*CommandOutput = (char *) globus_malloc(strlen(response) + (cos_list ? strlen(cos_list) : 0) +1);
	if (*CommandOutput == NULL)
	{
		result = GlobusGFSErrorMemory("lscos");
		goto cleanup;
	}

	sprintf(*CommandOutput, response, cos_list ? cos_list : "");

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
 * The returned CommandOutput is static, do not free.
 */
void
commands_setfam(session_handle_t *  SessionHandle,
                char             *  Family,
                char             ** CommandOutput)
{
	int           fam_id      = SESSION_NO_FAMILY_ID;
	globus_bool_t can_use_fam = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the returned command output. */
	*CommandOutput = NULL;

	/*
	 * Check for the 'default' family.
	 */
	if (strcasecmp(Family, "default") == 0)
	{
		/* Unset any early family settings. */
		session_pref_set_family_id(SessionHandle, SESSION_NO_FAMILY_ID);
		goto cleanup;
	}

	/* Get the family id. */
	fam_id = session_get_family_id(SessionHandle, Family);
	if (fam_id == SESSION_NO_FAMILY_ID)
	{
		*CommandOutput = "550 That family does not exist\r\n";
		goto cleanup;
	}

	/* Check if the user is allowed to use this family. */
	can_use_fam = session_can_user_use_family(SessionHandle, fam_id);
	if (can_use_fam == GLOBUS_FALSE)
	{
		*CommandOutput = "550 Not permitted to use this family\r\n";
		goto cleanup;
	}

	/* Save this family id in our preferences. */
	session_pref_set_family_id(SessionHandle, fam_id);

cleanup:
    GlobusGFSHpssDebugExit();
}

/*
 * CommandOutput must be free'd.
 */
globus_result_t
commands_lsfam(session_handle_t *  SessionHandle,
               char             ** CommandOutput)
{
	char            * fam_list = NULL;
	char            * response = "250 Allowed Families: %s\r\n";
	globus_result_t   result   = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the command output. */
	*CommandOutput = NULL;

	result = session_get_user_family_list(SessionHandle, &fam_list);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Construct the command output.
	 */
	*CommandOutput = (char *) globus_malloc(strlen(response) + (fam_list ? strlen(fam_list) : 0) +1);
	if (*CommandOutput == NULL)
	{
		result = GlobusGFSErrorMemory("lsfam");
		goto cleanup;
	}

	sprintf(*CommandOutput, response, fam_list ? fam_list : "");

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
    	GlobusGFSHpssDebugExitWithError();
		return result;
	}

    GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

typedef struct monitor {
	globus_mutex_t  Lock;
	globus_cond_t   Cond;
	globus_result_t Result;
	globus_bool_t   Complete;
} monitor_t;

static void
commands_monitor_init(monitor_t * Monitor)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_init(&Monitor->Lock, NULL);
	globus_cond_init(&Monitor->Cond, NULL);
	Monitor->Result   = GLOBUS_SUCCESS;
	Monitor->Complete = GLOBUS_FALSE;

	GlobusGFSHpssDebugExit();
}

static void
commands_monitor_destroy(monitor_t * Monitor)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_destroy(&Monitor->Lock);
	globus_cond_destroy(&Monitor->Cond);

	GlobusGFSHpssDebugExit();
}


static void
commands_recv_transfer_control_msg(void * CallbackArg,
                                   int    MsgType,
                                   int    MsgLen,
                                   void * Msg)
{
	monitor_t                       * monitor      = NULL;
	transfer_control_complete_msg_t * complete_msg = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Cast to our monitor. */
	monitor = (monitor_t *)CallbackArg;

	switch(MsgType)
	{
	case TRANSFER_CONTROL_MSG_TYPE_COMPLETE:
		/* Cast our message. */
		complete_msg = (transfer_control_complete_msg_t*) Msg;

		globus_mutex_lock(&monitor->Lock);
		{
			/* Indicate that we have finished. */
			monitor->Complete = GLOBUS_TRUE;
			/* Save any error. */
			monitor->Result   = complete_msg->Result;
			/* Wake any waiters. */
			globus_cond_signal(&monitor->Cond);
		}
		globus_mutex_unlock(&monitor->Lock);
		break;
	default:
		break;
	}

	GlobusGFSHpssDebugExit();
}

static void
commands_recv_data_ranges_msg(void * CallbackArg,
                              int    MsgType,
                              int    MsgLen,
                              void * Msg)
{
	marker_handle_t                  * marker_handle  = (marker_handle_t *) CallbackArg;
	data_ranges_msg_range_complete_t * range_complete = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	switch (MsgType)
	{
	case DATA_RANGES_MSG_TYPE_RANGE_COMPLETE:
		/* Cast the msg. */
		range_complete = (data_ranges_msg_range_complete_t *) Msg;

		globus_mutex_lock(&marker_handle->Lock);
		{
			marker_handle->TotalBytes += range_complete->Length;
		}
		globus_mutex_unlock(&marker_handle->Lock);
		break;
	default:
		break;
	}

	GlobusGFSHpssDebugExit();
}

static void
commands_msg_recv(void          * CallbackArg,
                  msg_comp_id_t   DstMsgCompID,
                  msg_comp_id_t   SrcMsgCompID,
                  int             MsgType,
                  int             MsgLen,
                  void          * Msg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	switch (SrcMsgCompID)
	{
	case MSG_COMP_ID_TRANSFER_CONTROL:
		commands_recv_transfer_control_msg(CallbackArg, MsgType, MsgLen, Msg);
		break;

	case MSG_COMP_ID_TRANSFER_DATA_RANGES:
		commands_recv_data_ranges_msg(CallbackArg, MsgType, MsgLen, Msg);
		break;

	default:
		globus_assert(0);
	}

	GlobusGFSHpssDebugExit();
}

static void
commands_checksum_send_markers(void * UserArg)
{
	marker_handle_t * marker_handle = (marker_handle_t *) UserArg;
	char              total_bytes_string[128];

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&marker_handle->Lock);
	{
		/* Convert the byte count to a string. */
		sprintf(total_bytes_string, "%"GLOBUS_OFF_T_FORMAT, marker_handle->TotalBytes);

		/* Send the intermediate response. */
		globus_gridftp_server_intermediate_command(marker_handle->Operation,
		                                           GLOBUS_SUCCESS,
		                                           total_bytes_string);
	}
	globus_mutex_unlock(&marker_handle->Lock);

	GlobusGFSHpssDebugExit();
}

static globus_result_t
commands_start_markers(marker_handle_t        * MarkerHandle,
                       msg_handle_t           * MsgHandle,
                       globus_gfs_operation_t   Operation)
{
	int              marker_freq = 0;
	globus_result_t  result      = GLOBUS_SUCCESS;
	globus_reltime_t delay;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Setup the marker handle. */
	globus_mutex_init(&MarkerHandle->Lock, NULL);
	MarkerHandle->TotalBytes     = 0;
	MarkerHandle->Operation      = Operation;
	MarkerHandle->MsgRegisterID  = MSG_REGISTER_ID_NONE;
	MarkerHandle->CallbackHandle = GLOBUS_NULL_HANDLE;
	MarkerHandle->MsgHandle      = MsgHandle;

	/* Get the frequency for maker updates. */
	globus_gridftp_server_get_update_interval(Operation, &marker_freq);

	if (marker_freq > 0)
	{
		/* Register to receive completed data range messages. */
		result = msg_register(MsgHandle,
		                      MSG_COMP_ID_TRANSFER_DATA_RANGES,
		                      MSG_COMP_ID_NONE,
		                      commands_msg_recv,
		                      MarkerHandle,
		                      &MarkerHandle->MsgRegisterID);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Setup the periodic callback. */
		GlobusTimeReltimeSet(delay, marker_freq, 0);
		result = globus_callback_register_periodic(&MarkerHandle->CallbackHandle,
		                                           &delay,
		                                           &delay,
		                                           commands_checksum_send_markers,
		                                           MarkerHandle);

		if (result != GLOBUS_SUCCESS)
			goto cleanup;
	}

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

static void
commands_stop_markers(marker_handle_t * MarkerHandle)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Unregister for the messages. */
	msg_unregister(MarkerHandle->MsgHandle, MarkerHandle->MsgRegisterID);

	/* Unregister our periodic marker callback. */
	if (MarkerHandle->CallbackHandle != GLOBUS_NULL_HANDLE)
		globus_callback_unregister(MarkerHandle->CallbackHandle, NULL, NULL, NULL);

	globus_mutex_destroy(&MarkerHandle->Lock);

	GlobusGFSHpssDebugExit();
}

globus_result_t
commands_compute_checksum(globus_gfs_operation_t       Operation,
                          globus_gfs_command_info_t *  CommandInfo,
                          session_handle_t          *  Session,
                          char                      ** Checksum)
{
	monitor_t            monitor;
	globus_result_t      result           = GLOBUS_SUCCESS;
	transfer_data_t    * transfer_data    = NULL;
	transfer_control_t * transfer_control = NULL;
	msg_handle_t       * msg_handle       = NULL;
	marker_handle_t      marker_handle;
	msg_register_id_t    msg_register_id  = MSG_REGISTER_ID_NONE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the monitor */
	commands_monitor_init(&monitor);

	/* Get the message handle. */
	msg_handle = session_cache_lookup_object(Session,
	                                         SESSION_CACHE_OBJECT_ID_MSG_HANDLE);

	/* Register to receive messages. */
	result = msg_register(msg_handle,
	                      MSG_COMP_ID_TRANSFER_CONTROL,
	                      MSG_COMP_ID_NONE,
	                      commands_msg_recv,
	                      &monitor,
	                      &msg_register_id);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Setup perf markers. */
	result = commands_start_markers(&marker_handle, msg_handle, Operation);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Initialize the control side. */
	result = transfer_control_cksm_init(msg_handle,
	                                    Operation,
	                                    CommandInfo,
	                                    &transfer_control);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Initialize the data side. This will tell the control side
	 * when it's ready.
	 */
	result = transfer_data_cksm_init(msg_handle,
	                                 Operation,
	                                 CommandInfo,
	                                 &transfer_data);

	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Start the data side. (sync) */
	result = transfer_data_run(transfer_data);

	/* Pass this message to the control side. */
	transfer_control_data_complete(transfer_control, result);

	/* Wait for the control side to complete. */
	globus_mutex_lock(&monitor.Lock);
	{
		if (monitor.Complete == GLOBUS_FALSE)
			globus_cond_wait(&monitor.Cond, &monitor.Lock);
	}
	globus_mutex_unlock(&monitor.Lock);

	/* Save off the result. */
	result = monitor.Result;

	if (result == GLOBUS_SUCCESS)
		result = transfer_data_checksum(transfer_data, Checksum);

cleanup:
	/* Unregister to receive messages. */
	msg_unregister(msg_handle, msg_register_id);

	/* Stop perf markers. */
	commands_stop_markers(&marker_handle);

	/* Destroy the data side. */
	transfer_data_destroy(transfer_data);

	/* Destroy the control side. */
	transfer_control_destroy(transfer_control);

	/* Destroy the monitor */
	commands_monitor_destroy(&monitor);

	GlobusGFSHpssDebugExit();
	return result;
}

globus_result_t
commands_checksum(globus_gfs_operation_t       Operation,
                  globus_gfs_command_info_t *  CommandInfo,
                  session_handle_t          *  Session,
                  char                      ** Checksum)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Partial checksums */
	if (CommandInfo->cksm_offset != 0 || CommandInfo->cksm_length != -1)
	{
		/* Compute it. */
		result = commands_compute_checksum(Operation, CommandInfo, Session, Checksum);
		goto cleanup;
	}

	/* Full checksums. */

	/* Check if the sum is already stored in UDA. */
	result = checksum_get_file_sum(CommandInfo->pathname, Checksum);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* If the checksum wasn't previously stored... */
	if (*Checksum == NULL)
	{
		/* Compute it. */
		result = commands_compute_checksum(Operation, CommandInfo, Session, Checksum);

		/* Try to store the checksum in UDA. Ignore errors on set. */
		if (result == GLOBUS_SUCCESS)
			checksum_set_file_sum(CommandInfo->pathname, *Checksum);
	}

cleanup:

	GlobusGFSHpssDebugExit();
	return result;
}
                  
void
commands_handler(globus_gfs_operation_t      Operation,
                 globus_gfs_command_info_t * CommandInfo,
                 void                      * UserArg)
{
	int                 timeout         = 0;
	int                 argc            = 0;
	int                 retval          = 0;
	int                 staged          = 0;
	char             *  hard_link_from  = NULL;
	char             ** argv            = NULL;
	char             *  command_output  = NULL;
	globus_result_t     result          = GLOBUS_SUCCESS;
	struct utimbuf      times;
	session_handle_t *  session         = NULL;
	globus_bool_t       tape_only       = GLOBUS_FALSE;
	globus_bool_t       free_cmd_output = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Make sure we got our UserArg. */
	globus_assert(UserArg != NULL);

	/* Cast to our session handle. */
	session = (session_handle_t *)UserArg;

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

	case GLOBUS_GFS_CMD_RNTO:
#ifdef MARK_RENAMED_FILES
		{
			hpss_userattr_list_t attr_list;

			attr_list.len  = 1;
			attr_list.Pair = malloc(sizeof(hpss_userattr_t));
			if (!attr_list.Pair)
			{
				result = GlobusGFSErrorMemory("hpss_userattr_t");
				break;
			}

			attr_list.Pair[0].Key = "/hpss/ncsa/quota/Renamed";
			attr_list.Pair[0].Value = "1";

			retval = hpss_UserAttrSetAttrs(CommandInfo->from_pathname, &attr_list, NULL);
			free(attr_list.Pair);
			if (retval)
        		result = GlobusGFSErrorSystemError("hpss_UserAttrSetAttrs", -retval);
		}
#endif /* MARK_RENAMED_FILES */

		retval = hpss_Rename(CommandInfo->from_pathname, CommandInfo->pathname);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Rename", -retval);
		break;

	case GLOBUS_GFS_CMD_RNFR:
		/*
		 * XXX This is never called.
		 */
		break;

	case GLOBUS_GFS_CMD_SITE_CHMOD:
		retval = hpss_Chmod(CommandInfo->pathname, CommandInfo->chmod_mode);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Chmod", -retval);
		break;

	case GLOBUS_GFS_CMD_SITE_CHGRP:
	    result = commands_chgrp(CommandInfo->pathname, CommandInfo->chgrp_group);
		break;

	case GLOBUS_GFS_CMD_SITE_UTIME:
		times.actime  = CommandInfo->utime_time;
		times.modtime = CommandInfo->utime_time;

		retval = hpss_Utime(CommandInfo->pathname, &times);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Utime", -retval);
		break;

	case GLOBUS_GFS_CMD_SITE_SYMLINK:
		retval = hpss_Symlink(CommandInfo->from_pathname, CommandInfo->pathname);
		if (retval != 0)
        	result = GlobusGFSErrorSystemError("hpss_Symlink", -retval);
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_SETCOS:
		commands_setcos(session, 
		                CommandInfo->pathname,  /* The COS */
		                &command_output);
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_LSCOS:
		result = commands_lscos(session, &command_output);
		free_cmd_output = GLOBUS_TRUE;
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_SETFAM:
		commands_setfam(session, 
		                CommandInfo->pathname,  /* The family */
		                &command_output);
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_LSFAM:
		result = commands_lsfam(session, &command_output);
		free_cmd_output = GLOBUS_TRUE;
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_HARDLINKFROM:
		result = session_cmd_set_hardlinkfrom(session, CommandInfo->pathname);
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_HARDLINKTO:
		/* Check if the client issued a HARDLINKFROM */
		hard_link_from = session_cmd_get_hardlinkfrom(session);
		if (hard_link_from == NULL)
		{
			command_output = "501 Must specify HARDLINKFROM first\r\n";
			break;
		}

		/* Now link the two paths. */
		retval = hpss_Link(hard_link_from, CommandInfo->pathname);
		if (retval != 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Link", -retval);
		}

		/* Release the hard link source. */
		session_cmd_free_hardlinkfrom(session);
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_STAGE:
		/* Get the command arguments. */
		result = globus_gridftp_server_query_op_info(Operation, 
		                                             CommandInfo->op_info,
		                                             GLOBUS_GFS_OP_INFO_CMD_ARGS,
		                                             &argv,
		                                             &argc);
		if (result != GLOBUS_SUCCESS)
		{
			result = GlobusGFSErrorWrapFailed("Unable to get command args", result);
			break;
		}

		/* Convert the timeout. */
		retval = sscanf(argv[2], "%d", &timeout);
		if (retval != 1)
		{
			result = GlobusGFSErrorGeneric("Illegal timeout value");
			break;
		}

		/* Stage the file. */
		result = commands_stage_file(CommandInfo->pathname,
                                     timeout,
                                     &staged,
                                     &tape_only);
		if (result == GLOBUS_SUCCESS)
		{
			if (staged == GLOBUS_TRUE)
				command_output = globus_common_create_string(
				                     "250 Stage of file %s succeeded.\r\n",
				                     CommandInfo->pathname);
			else if (tape_only == GLOBUS_TRUE)
				command_output = globus_common_create_string(
				                     "250 %s is on a tape only class of service.\r\n",
				                     CommandInfo->pathname);
			else
				command_output = globus_common_create_string(
				                     "450 %s: is being retrieved from the archive...\r\n",
				                     CommandInfo->pathname);

			free_cmd_output = GLOBUS_TRUE;
        }
        break;

	case GLOBUS_GFS_CMD_CKSM:
		result = commands_checksum(Operation, CommandInfo, session, &command_output);
		if (result == GLOBUS_SUCCESS)
			free_cmd_output = GLOBUS_TRUE;
		break;

	case GLOBUS_GFS_CMD_SITE_DSI:
	case GLOBUS_GFS_CMD_SITE_SETNETSTACK:
	case GLOBUS_GFS_CMD_SITE_SETDISKSTACK:
	case GLOBUS_GFS_CMD_SITE_CLIENTINFO:
	case GLOBUS_GFS_CMD_DCSC:
	case GLOBUS_GFS_CMD_SITE_AUTHZ_ASSERT:
	case GLOBUS_GFS_CMD_SITE_RDEL:
	default:
		result = GlobusGFSErrorGeneric("Not Supported");
		break;
	}

	/*
	 * On error w/o command_output, code 500 is used which is bad. 500-509
	 * indicate that a command was not recognized which is not the case.
	 */
	if (command_output == NULL)
	{
		if (result == GLOBUS_SUCCESS)
			command_output = "250 Ok\r\n";
		else
			command_output = "550 Command failed\r\n";
	}

	globus_gridftp_server_finished_command(Operation, result, command_output);

	if (free_cmd_output == GLOBUS_TRUE && command_output != NULL)
		globus_free(command_output);

	GlobusGFSHpssDebugExit();
}

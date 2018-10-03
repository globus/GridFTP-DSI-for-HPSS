/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2015 NCSA.  All rights reserved.
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
 * System includes
 */
#include <sys/select.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>

/*
 * Globus includes
 */
#include <globus_list.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>
#include <hpss_version.h>

/*
 * Local includes
 */
#include "stage.h"
#include "stat.h"

static globus_list_t * _gStageList = NULL;

int
stage_match_bitfileid(void * Datum, void * Arg)
{
	return (memcmp((hpssoid_t *)Datum, (hpssoid_t *) Arg, sizeof(hpssoid_t)) == 0);
}

extern globus_list_t *
globus_list_search_pred (globus_list_t * head,
             globus_list_pred_t predicate,
             void * pred_args);

void
stage_add_bfid_to_list(hpssoid_t * BitFileID)
{
	hpssoid_t * alloced_bfid = malloc(sizeof(hpssoid_t));
	memcpy(alloced_bfid, BitFileID, sizeof(hpssoid_t));
	globus_list_insert(&_gStageList, alloced_bfid);
}

void
stage_rm_bfid_from_list(hpssoid_t * BitFileID)
{
	globus_list_t * match = globus_list_search_pred(_gStageList,
	                                                stage_match_bitfileid,
	                                                BitFileID);
	if (match)
	{
		hpssoid_t * alloced_bfid = globus_list_remove(&_gStageList, match);
		free(alloced_bfid);
	}
}

int
stage_check_bfid_in_list(hpssoid_t * BitFileID)
{
	globus_list_t * match = globus_list_search_pred(_gStageList,
	                                                stage_match_bitfileid,
	                                                BitFileID);

	return (match != NULL);
}

globus_result_t
stage_get_timeout(globus_gfs_operation_t      Operation,
                  globus_gfs_command_info_t * CommandInfo,
                  int                       * Timeout)
{
	globus_result_t result;
	char ** argv    = NULL;
	int     argc    = 0;
	int     retval  = 0;

	GlobusGFSName(stage_get_timeout);

	/* Get the command arguments. */
	result = globus_gridftp_server_query_op_info(Operation,
	                                             CommandInfo->op_info,
	                                             GLOBUS_GFS_OP_INFO_CMD_ARGS,
	                                             &argv,
	                                             &argc);

	if (result)
		return GlobusGFSErrorWrapFailed("Unable to get command args", result);

	/* Convert the timeout. */
	retval = sscanf(argv[2], "%d", Timeout);
	if (retval != 1)
		return GlobusGFSErrorGeneric("Illegal timeout value");

	return GLOBUS_SUCCESS;	
}

void
stage_free_xfileattr(hpss_xfileattr_t * XFileAttr)
{
	int storage_level = 0;
	int vv_index = 0;

	/* Free the extended information. */
	for (storage_level = 0; storage_level < HPSS_MAX_STORAGE_LEVELS; storage_level++)
	{
		for(vv_index = 0; vv_index < XFileAttr->SCAttrib[storage_level].NumberOfVVs; vv_index++)
		{
			if (XFileAttr->SCAttrib[storage_level].VVAttrib[vv_index].PVList != NULL)
			{
				free(XFileAttr->SCAttrib[storage_level].VVAttrib[vv_index].PVList);
			}
		}
	}
}

void
stage_check_residency(hpss_xfileattr_t * XFileAttr, stage_file_residency * Residency)
{
	int      storage_level = 0;
	uint64_t max_bytes_on_disk = 0;

	/* Check if this is a regular file. */
	if (XFileAttr->Attrs.Type != NS_OBJECT_TYPE_FILE && XFileAttr->Attrs.Type != NS_OBJECT_TYPE_HARD_LINK)
	{
		*Residency = STAGE_FILE_RESIDENT;
		return;
	}

	/* Check if the top level is tape. */
	if (XFileAttr->SCAttrib[0].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
	{
		*Residency = STAGE_FILE_TAPE_ONLY;
		return;
	}

	/* Handle zero length files. */
	if (eqz64m(XFileAttr->Attrs.DataLength))
	{
		*Residency = STAGE_FILE_RESIDENT;
		return;
	}

	/*
	 * Determine the archive status. Due to holes, we can not expect XFileAttr->Attrs.DataLength
	 * bytes on disk. And we really don't know how much data is really in this file. So the
	 * algorithm works like this: assume the file is staged unless you find a tape SC that
	 * has more BytesAtLevel than the disk SCs before it.
	 */

	*Residency = STAGE_FILE_RESIDENT;

	for (storage_level = 0; storage_level < HPSS_MAX_STORAGE_LEVELS; storage_level++)
	{
		if (XFileAttr->SCAttrib[storage_level].Flags & BFS_BFATTRS_LEVEL_IS_DISK)
		{
			/* Save the largest count of bytes on disk. */
			if (gt64(XFileAttr->SCAttrib[storage_level].BytesAtLevel, max_bytes_on_disk))
				max_bytes_on_disk = XFileAttr->SCAttrib[storage_level].BytesAtLevel;
		} else if (XFileAttr->SCAttrib[storage_level].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
		{
			/* File is purged if more bytes are on disk. */
			if (gt64(XFileAttr->SCAttrib[storage_level].BytesAtLevel, max_bytes_on_disk))
			{
				*Residency = STAGE_FILE_ARCHIVED;
				return;
			}
		}
	}
}

globus_result_t
stage_get_residency(char * Pathname, stage_file_residency * Residency)
{
	hpss_xfileattr_t xfileattr;
	int retval = 0;

	GlobusGFSName(stage_get_residency);

	memset(&xfileattr, 0, sizeof(hpss_xfileattr_t));

	/*
	 * Stat the object. Without API_GET_XATTRS_NO_BLOCK, this call would hang
	 * on any file moving between levels in its hierarchy (ie staging).
	 */
	retval = hpss_FileGetXAttributes(Pathname,
	                                 API_GET_STATS_FOR_ALL_LEVELS|API_GET_XATTRS_NO_BLOCK,
	                                 0,
	                                 &xfileattr);

	if (retval)
		return GlobusGFSErrorSystemError("hpss_FileGetXAttributes", -retval);

	/* Get the residency */
	stage_check_residency(&xfileattr, Residency);

	/* Release the hpss_xfileattr_t */
	stage_free_xfileattr(&xfileattr);
	return GLOBUS_SUCCESS;
}

globus_result_t
stage_file(char * Pathname, int Timeout, stage_file_residency * Residency)
{
	globus_result_t  result = GLOBUS_SUCCESS;
	hpss_xfileattr_t xfileattr;
	hpss_reqid_t     reqid;
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
	bfs_bitfile_obj_handle_t bitfile_id;
#else
	hpssoid_t        bitfile_id;
#endif
	time_t           start_time = time(NULL);
	int              retval;

	GlobusGFSName(stage_file);

	memset(&xfileattr, 0, sizeof(hpss_xfileattr_t));

	/*
	 * Stat the object. Without API_GET_XATTRS_NO_BLOCK, this call would hang
	 * on any file moving between levels in its hierarchy (ie staging).
	 */
	retval = hpss_FileGetXAttributes(Pathname,
	                                 API_GET_STATS_FOR_ALL_LEVELS|API_GET_XATTRS_NO_BLOCK,
	                                 0,
	                                 &xfileattr);

	if (retval)
		return GlobusGFSErrorSystemError("hpss_FileGetXAttributes", -retval);

	stage_check_residency(&xfileattr, Residency);

	switch (*Residency)
	{
	case STAGE_FILE_RESIDENT:
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
		stage_rm_bfid_from_list(&xfileattr.Attrs.BitfileObj.BfId);
#else

		stage_rm_bfid_from_list(&xfileattr.Attrs.BitfileId);
#endif
	case STAGE_FILE_TAPE_ONLY:
		goto cleanup;
	case STAGE_FILE_ARCHIVED:
		break;
	}

	/*
	 * Need to stage file.
	 */
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
	if (stage_check_bfid_in_list(&xfileattr.Attrs.BitfileObj.BfId))
		goto cleanup;
#else
	if (stage_check_bfid_in_list(&xfileattr.Attrs.BitfileId))
		goto cleanup;
#endif

	/*
	 * We use hpss_StageCallBack() so that we do not block while the
	 * stage completes. We could use hpss_Open(O_NONBLOCK) and then
	 * hpss_Stage(BFS_ASYNCH_CALL) but then we block in hpss_Close().
	 */
	retval = hpss_StageCallBack(Pathname,
	                            cast64m(0),
	                            xfileattr.Attrs.DataLength,
	                            0,
	                            NULL,
	                            BFS_STAGE_ALL,
	                            &reqid,
	                            &bitfile_id);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_StageCallBack()", -retval);
		goto cleanup;
	}

	/* Now wait for the given about of time or the file staged. */
	while ((time(NULL) - start_time) < Timeout && *Residency == STAGE_FILE_ARCHIVED)
	{
		// Sleep for 1.0 sec
		struct timeval tv;
		tv.tv_sec  = 1;
		tv.tv_usec = 0;
		select(0, NULL, NULL, NULL, &tv);

		result = stage_get_residency(Pathname, Residency);
		if (result)
			goto cleanup;
	}

	stage_check_residency(&xfileattr, Residency);
	if (*Residency != STAGE_FILE_ARCHIVED)
	{
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
		stage_add_bfid_to_list(&xfileattr.Attrs.BitfileObj.BfId);
#else
		stage_add_bfid_to_list(&xfileattr.Attrs.BitfileId);
#endif
	}

cleanup:
	stage_free_xfileattr(&xfileattr);
	return result;
}

void
stage(globus_gfs_operation_t      Operation,
      globus_gfs_command_info_t * CommandInfo,
      commands_callback           Callback)
{
	int                  timeout;
	char               * command_output = NULL;
	stage_file_residency residency;
	globus_result_t      result; 

	/* Get the timeout. */
	result = stage_get_timeout(Operation, CommandInfo, &timeout);
	if (result)
		goto cleanup;

	result = stage_file(CommandInfo->pathname, timeout, &residency);
	if (result)
		goto cleanup;

	switch (residency)
	{
	case STAGE_FILE_RESIDENT:
		command_output = globus_common_create_string(
		                                     "250 Stage of file %s succeeded.\r\n",
		                                     CommandInfo->pathname);
		goto cleanup;

	case STAGE_FILE_TAPE_ONLY:
		command_output = globus_common_create_string(
		                             "250 %s is on a tape only class of service.\r\n",
		                             CommandInfo->pathname);
		goto cleanup;

	case STAGE_FILE_ARCHIVED:
		command_output = globus_common_create_string(
		                             "450 %s: is being retrieved from the archive...\r\n",
		                             CommandInfo->pathname);
		break;
	}

cleanup:
	Callback(Operation, result, command_output);
	if (command_output)
		globus_free(command_output);
}

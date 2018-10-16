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
 * Local includes
 */
#include "stage.h"
#include "stage_test.h"

GlobusDebugDeclare(GLOBUS_GRIDFTP_SERVER_HPSS);

// Fragile. Defined in ../loaders/common_loader.c
#define TRACE 1 /* TRACE_STAGING */

#define DEBUG(message) \
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, TRACE, message)

/*
 * Use a constant request number for stage requests so that we can
 * query the stage request status between processes.
 */
static hpss_reqid_t REQUEST_ID = 0xDEADBEEF;

STATIC globus_result_t
check_request_status(bitfile_id_t * BitfileID, int * Status)
{
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || \
     HPSS_MAJOR_VERSION >= 8
	int retval = hpss_GetAsyncStatus(REQUEST_ID, BitfileID, Status);
#else
	int retval = hpss_GetAsynchStatus(REQUEST_ID, BitfileID, Status);
#endif

	if (retval)
		return GlobusGFSErrorSystemError("hpss_GetAsyncStatus", -retval);

	switch (*Status)
	{
	case HPSS_STAGE_STATUS_UNKNOWN:
		DEBUG(("Stage request status UNKNOWN"));
		break;
	case HPSS_STAGE_STATUS_ACTIVE:
		DEBUG(("Stage request status ACTIVE"));
		break;
	case HPSS_STAGE_STATUS_QUEUED:
		DEBUG(("Stage request status QUEUED"));
		break;
	}

	return GLOBUS_SUCCESS;
}

//int s = -1;

STATIC globus_result_t
submit_stage_request(const char * Pathname)
{
	hpss_fileattr_t fattrs;
	int retval = hpss_FileGetAttributes(Pathname, &fattrs);
	if (retval)
		return GlobusGFSErrorSystemError("hpss_FileGetAttributes", -retval);

	bitfile_id_t bitfile_id;

	bfs_callback_addr_t callback_addr;
	memset(&callback_addr, 0, sizeof(callback_addr));
	callback_addr.id = 0xDEADBEEF;

	DEBUG(("Requesting stage for ", Pathname));
	/*
	 * We use hpss_StageCallBack() so that we do not block while the
	 * stage completes. We could use hpss_Open(O_NONBLOCK) and then
	 * hpss_Stage(BFS_ASYNCH_CALL) but then we block in hpss_Close().
	 */
	retval = hpss_StageCallBack(Pathname,
	                            cast64m(0),
	                            fattrs.Attrs.DataLength,
	                            0,
	                            &callback_addr,
	                            BFS_STAGE_ALL,
	                            &REQUEST_ID,
	                            &bitfile_id);
	if (retval)
		return GlobusGFSErrorSystemError("hpss_StageCallBack()", -retval);

	return GLOBUS_SUCCESS;
}

STATIC residency_t
check_xattr_residency(hpss_xfileattr_t * XFileAttr)
{
	/* Check if this is a regular file. */
	if (XFileAttr->Attrs.Type != NS_OBJECT_TYPE_FILE && 
	    XFileAttr->Attrs.Type != NS_OBJECT_TYPE_HARD_LINK)
	{
		return RESIDENT;
	}

	/* Check if the top level is tape. */
	if (XFileAttr->SCAttrib[0].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
	{
		return TAPE_ONLY;
	}

	/* Handle zero length files. */
	if (eqz64m(XFileAttr->Attrs.DataLength))
	{
		return RESIDENT;
	}

	/*
	 * Determine the archive status. Due to holes, we can not expect
	 * XFileAttr->Attrs.DataLength bytes on disk. And we really don't know
	 * how much data is really in this file. So the algorithm assumes the
	 * file is staged unless it finds a tape SC that has more BytesAtLevel
	 * than the disk SCs before it.
	 */
	int level = 0;
	uint64_t max_bytes = 0;

	for (level = 0; level < HPSS_MAX_STORAGE_LEVELS; level++)
	{
		if (XFileAttr->SCAttrib[level].Flags & BFS_BFATTRS_LEVEL_IS_DISK)
		{
			/* Save the largest count of bytes on disk. */
			if (gt64(XFileAttr->SCAttrib[level].BytesAtLevel, max_bytes))
				max_bytes = XFileAttr->SCAttrib[level].BytesAtLevel;
		} else if (XFileAttr->SCAttrib[level].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
		{
			/* File is purged if more bytes are on tape. */
			if (gt64(XFileAttr->SCAttrib[level].BytesAtLevel, max_bytes))
				return ARCHIVED;
		}
	}

	return RESIDENT;
}

STATIC void
free_xfileattr(hpss_xfileattr_t * XFileAttr)
{
	int level = 0;
	int vv_index = 0;

	/* Free the extended information. */
	for (level = 0; level < HPSS_MAX_STORAGE_LEVELS; level++)
	{
		for(vv_index = 0; 
		    vv_index < XFileAttr->SCAttrib[level].NumberOfVVs; 
		    vv_index++)
		{
			if (XFileAttr->SCAttrib[level].VVAttrib[vv_index].PVList != NULL)
			{
				free(XFileAttr->SCAttrib[level].VVAttrib[vv_index].PVList);
			}
		}
	}
}

STATIC globus_result_t
check_file_residency(const char * Pathname, residency_t * Residency)
{
	int retval = 0;
	hpss_xfileattr_t xattr;

	GlobusGFSName(check_file_residency);

	memset(&xattr, 0, sizeof(hpss_xfileattr_t));

	/*
	 * Stat the object. Without API_GET_XATTRS_NO_BLOCK, this call would hang
	 * on any file moving between levels in its hierarchy (ie staging).
	 */
	retval = hpss_FileGetXAttributes(Pathname,
	                                 API_GET_STATS_FOR_ALL_LEVELS|
	                                 API_GET_XATTRS_NO_BLOCK,
	                                 0,
	                                 &xattr);

	if (retval)
		return GlobusGFSErrorSystemError("hpss_FileGetXAttributes", -retval);

	*Residency = check_xattr_residency(&xattr);

	/* Release the hpss_xfileattr_t */
	free_xfileattr(&xattr);

	switch (*Residency)
	{
	case ARCHIVED:
		DEBUG(("File is ARCHIVED: %s", Pathname));
		break;
	case RESIDENT:
		DEBUG(("File is RESIDENT: %s", Pathname));
		break;
	case TAPE_ONLY:
		DEBUG(("File is TAPE_ONLY: %s", Pathname));
		break;
	}

	return GLOBUS_SUCCESS;
}

STATIC globus_result_t
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

STATIC globus_result_t
get_bitfile_id(const char * Pathname, bitfile_id_t * bitfile_id)
{
	hpss_fileattr_t attrs;
	int retval = hpss_FileGetAttributes(Pathname, &attrs);
	if (retval)
		return GlobusGFSErrorSystemError("hpss_FileGetAttributes", -retval);

	memcpy(bitfile_id, &ATTR_TO_BFID(attrs), sizeof(bitfile_id_t));
	return GLOBUS_SUCCESS;
}

STATIC char *
generate_output(const char * Pathname, residency_t Residency)
{
	if (Residency == RESIDENT)
		return globus_common_create_string(
		                 "250 Stage of file %s succeeded.\r\n",
		                 Pathname);

	if (Residency == TAPE_ONLY)
		return globus_common_create_string(
		                 "250 %s is on a tape only class of service.\r\n",
		                 Pathname);

	return globus_common_create_string(
		                 "450 %s: is being retrieved from the archive...\r\n",
		                 Pathname);
}

/* Returns 1 if Timeout has elapsed, 0 otherwise. */
STATIC int
pause_1_second(time_t StartTime, int Timeout)
{
	if ((time(NULL) - StartTime) > Timeout)
		return 1;

	// Sleep for 1.0 seconds
	struct timeval tv;
	tv.tv_sec  = 1;
	tv.tv_usec = 0;
	select(0, NULL, NULL, NULL, &tv);

	return 0;
}

STATIC globus_result_t
stage_internal(const char * Path, int Timeout, residency_t * Residency)
{
	int             time_elapsed = 0;
	time_t          start_time = time(NULL);
	bitfile_id_t    bitfile_id;
	globus_result_t result; 

	*Residency = ARCHIVED;

	GlobusGFSName(stage);

	result = get_bitfile_id(Path, &bitfile_id);
	if (result)
		goto cleanup;

	while (*Residency == ARCHIVED && !time_elapsed)
	{
		result = check_file_residency(Path, Residency);
		if (result)
			goto cleanup;
		if (*Residency != ARCHIVED)
			break;

		int status;
		result = check_request_status(&bitfile_id, &status);
		if (result)
			goto cleanup;

		if (status == HPSS_STAGE_STATUS_UNKNOWN)
		{
			result = submit_stage_request(Path);
			if (result)
				goto cleanup;
		}

		time_elapsed = pause_1_second(start_time, Timeout);
	}


cleanup:
	return result;
}

void
stage(globus_gfs_operation_t      Operation,
      globus_gfs_command_info_t * CommandInfo,
      commands_callback           Callback)
{
	int             timeout;
	char          * command_output = NULL;
	residency_t     residency = ARCHIVED;
	globus_result_t result; 

	GlobusGFSName(stage);

	DEBUG(("Stage request for ", CommandInfo->pathname));

	result = stage_get_timeout(Operation, CommandInfo, &timeout);
	if (result)
		goto cleanup;

	result = stage_internal(CommandInfo->pathname, timeout, &residency);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	command_output = generate_output(CommandInfo->pathname, residency);

cleanup:
	Callback(Operation, result, command_output);
	if (command_output)
		globus_free(command_output);
}


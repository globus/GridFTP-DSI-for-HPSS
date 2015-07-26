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
#include <sys/types.h>
#include <stdlib.h>
#include <grp.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>

/*
 * Local includes
 */
#include "commands.h"
#include "config.h"
#include "cksm.h"

globus_result_t
commands_init(globus_gfs_operation_t Operation)
{
	GlobusGFSName(commands_init);

	globus_result_t result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE STAGE",
	                 GLOBUS_GFS_HPSS_CMD_SITE_STAGE,
	                 4,
	                 4,
	                 "SITE STAGE <sp> timeout <sp> path",
	                 GLOBUS_TRUE,
	                 GFS_ACL_ACTION_READ);

	if (result != GLOBUS_SUCCESS)
		return GlobusGFSErrorWrapFailed("Failed to add custom 'SITE STAGE' command", result);

	return GLOBUS_SUCCESS;
}

void
commands_mkdir(globus_gfs_operation_t      Operation,
               globus_gfs_command_info_t * CommandInfo,
               commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(commands_mkdir);

	int retval = hpss_Mkdir(CommandInfo->pathname, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Mkdir", -retval);

	Callback(Operation, result, NULL);
}

void
commands_rmdir(globus_gfs_operation_t      Operation,
               globus_gfs_command_info_t * CommandInfo,
               commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(commands_rmdir);

	int retval = hpss_Rmdir(CommandInfo->pathname);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Rmdir", -retval);

	Callback(Operation, result, NULL);
}

void
commands_unlink(globus_gfs_operation_t      Operation,
                globus_gfs_command_info_t * CommandInfo,
                commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(commands_unlink);

	int retval = hpss_Unlink(CommandInfo->pathname);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Unlink", -retval);

	Callback(Operation, result, NULL);
}

void
commands_rename(globus_gfs_operation_t      Operation,
                globus_gfs_command_info_t * CommandInfo,
                config_t                  * Config,
                commands_callback           Callback)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(commands_rename);

	if (Config->QuotaSupport)
	{
		hpss_userattr_list_t attr_list;

		attr_list.len  = 1;
		attr_list.Pair = malloc(sizeof(hpss_userattr_t));
		if (!attr_list.Pair)
		{
			result = GlobusGFSErrorMemory("hpss_userattr_t");
			goto cleanup;
		}

		attr_list.Pair[0].Key = "/hpss/ncsa/quota/Renamed";
		attr_list.Pair[0].Value = "1";

		retval = hpss_UserAttrSetAttrs(CommandInfo->from_pathname, &attr_list, NULL);
		free(attr_list.Pair);
		if (retval)
		{
			result = GlobusGFSErrorSystemError("hpss_UserAttrSetAttrs", -retval);
			goto cleanup;
		}
	}

	retval = hpss_Rename(CommandInfo->from_pathname, CommandInfo->pathname);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Rename", -retval);

cleanup:
	Callback(Operation, result, NULL);
}

void
commands_chmod(globus_gfs_operation_t      Operation,
               globus_gfs_command_info_t * CommandInfo,
               commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(commands_chmod);

	int retval = hpss_Chmod(CommandInfo->pathname, CommandInfo->chmod_mode);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Chmod", -retval);

	Callback(Operation, result, NULL);
}

globus_result_t
session_get_gid(char * GroupName, int * Gid)
{
    struct group * group = NULL;
    struct group   group_buf;
    char           buffer[1024];
    int            retval = 0;

    GlobusGFSName(session_get_gid);

    /* Find the passwd entry. */
    retval = getgrnam_r(GroupName,
                        &group_buf,
                        buffer,
                        sizeof(buffer),
                        &group);
    if (retval != 0)
        return GlobusGFSErrorSystemError("getgrnam_r", errno);

    if (group == NULL)
        return GlobusGFSErrorGeneric("Group not found");

    /* Copy out the gid */
    *Gid = group->gr_gid;

    return GLOBUS_SUCCESS;
}

void
commands_chgrp(globus_gfs_operation_t      Operation,
               globus_gfs_command_info_t * CommandInfo,
               commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;
	int gid;

	GlobusGFSName(commands_chgrp);

	hpss_stat_t hpss_stat_buf;
	int retval = hpss_Stat(CommandInfo->pathname, &hpss_stat_buf);
	if (retval)
	{
		result = GlobusGFSErrorSystemError("hpss_Stat", -retval);
		Callback(Operation, result, NULL);
		return;
	}

	if (!isdigit(*CommandInfo->chgrp_group))
	{
		result = session_get_gid(CommandInfo->chgrp_group, &gid);
		if (result != GLOBUS_SUCCESS)
		{
			Callback(Operation, result, NULL);
			return;
		}
	} else
	{
		gid = atoi(CommandInfo->chgrp_group);
	}

	retval = hpss_Chown(CommandInfo->pathname, hpss_stat_buf.st_uid, gid);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Chgrp", -retval);

	Callback(Operation, result, NULL);
}

void
commands_utime(globus_gfs_operation_t      Operation,
               globus_gfs_command_info_t * CommandInfo,
               commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(commands_utime);

	struct utimbuf times;
	times.actime  = CommandInfo->utime_time;
	times.modtime = CommandInfo->utime_time;

	int retval = hpss_Utime(CommandInfo->pathname, &times);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Utime", -retval);

	Callback(Operation, result, NULL);
}

void
commands_symlink(globus_gfs_operation_t      Operation,
                 globus_gfs_command_info_t * CommandInfo,
                 commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(commands_symlink);

	int retval = hpss_Symlink(CommandInfo->from_pathname, CommandInfo->pathname);
	if (retval)
		result = GlobusGFSErrorSystemError("hpss_Symlink", -retval);

	Callback(Operation, result, NULL);
}

void
commands_run(globus_gfs_operation_t      Operation,
             globus_gfs_command_info_t * CommandInfo,
             config_t                  * Config,
             commands_callback           Callback)
{
	GlobusGFSName(commands_run);

	switch (CommandInfo->command)
	{
	case GLOBUS_GFS_CMD_MKD:
		commands_mkdir(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_CMD_RMD:
		commands_rmdir(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_CMD_DELE:
		commands_unlink(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_CMD_RNTO:
		commands_rename(Operation, CommandInfo, Config, Callback);
		break;
	case GLOBUS_GFS_CMD_RNFR:
		break;
	case GLOBUS_GFS_CMD_SITE_CHMOD:
		commands_chmod(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_CMD_SITE_CHGRP:
		commands_chgrp(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_CMD_SITE_UTIME:
		commands_utime(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_CMD_SITE_SYMLINKFROM:
		break;
	case GLOBUS_GFS_CMD_SITE_SYMLINK:
		commands_symlink(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_CMD_CKSM:
		cksm(Operation, CommandInfo, Callback);
		break;
	case GLOBUS_GFS_HPSS_CMD_SITE_STAGE:
		stage(Operation, CommandInfo, Callback);
		break;

	case GLOBUS_GFS_CMD_SITE_AUTHZ_ASSERT:
	case GLOBUS_GFS_CMD_SITE_RDEL:
	case GLOBUS_GFS_CMD_SITE_DSI:
	case GLOBUS_GFS_CMD_SITE_SETNETSTACK:
	case GLOBUS_GFS_CMD_SITE_SETDISKSTACK:
	case GLOBUS_GFS_CMD_SITE_CLIENTINFO:
	case GLOBUS_GFS_CMD_DCSC:
	case GLOBUS_GFS_CMD_HTTP_PUT:
	case GLOBUS_GFS_CMD_HTTP_GET:
	case GLOBUS_GFS_CMD_HTTP_CONFIG:
	case GLOBUS_GFS_CMD_TRNC:
	case GLOBUS_GFS_CMD_SITE_TASKID:
	default:
		return Callback(Operation, GlobusGFSErrorGeneric("Not Supported"), NULL);
	}
}


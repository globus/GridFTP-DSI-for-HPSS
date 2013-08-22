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
#include <sys/types.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <pwd.h>
#include <grp.h>

/*
 * Globus includes.
 */
#include <globus_common.h>
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_env_defs.h>
#include <hpss_Getenv.h>
#include <hpss_limits.h>
#include <u_signed64.h>
#include <hpss_stat.h>
#include <hpss_api.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_config.h"
#include "gridftp_dsi_hpss_misc.h"

#ifdef DMALLOC
/*
 * Dmalloc
 */
#include <dmalloc.h>
#endif /* DMALLOC */

static globus_result_t
misc_copy_basename(char * Path, char ** BaseName)
{
	char            * basename = NULL;
	globus_result_t   result   = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	basename = strrchr(Path, '/');
	if (basename != NULL)
		*BaseName = globus_libc_strdup(basename + 1);
	else
		*BaseName = globus_libc_strdup(Path);

	if (*BaseName == NULL)
	{
		result = GlobusGFSErrorMemory("Path");
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

globus_result_t
misc_translate_stat(char              * Name,
                    hpss_stat_t       * HpssStat,
                    globus_gfs_stat_t * GlobusStat)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;
	char            symlink_target[HPSS_MAX_PATH_NAME];

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return variable. */
	memset(GlobusStat, 0, sizeof(globus_gfs_stat_t));

	/* Translate from hpss stat to globus stat. */
	GlobusStat->mode  = HpssStat->st_mode;
	GlobusStat->nlink = HpssStat->st_nlink;
	GlobusStat->uid   = HpssStat->st_uid;
	GlobusStat->gid   = HpssStat->st_gid;
 	GlobusStat->dev   = 0;
	GlobusStat->atime = HpssStat->hpss_st_atime;
	GlobusStat->mtime = HpssStat->hpss_st_mtime;
	GlobusStat->ctime = HpssStat->hpss_st_ctime;
	GlobusStat->ino   = cast32m(HpssStat->st_ino);
	CONVERT_U64_TO_LONGLONG(HpssStat->st_size, GlobusStat->size);

	/* If it is a symbolic link... */
	if (S_ISLNK(HpssStat->st_mode))
	{
		/* Read the target. */
		retval = hpss_Readlink(Name, symlink_target, sizeof(symlink_target));

		if (retval < 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Readlink", -retval);
			goto cleanup;
		}

		/* Copy out the symlink target. */
		GlobusStat->symlink_target = globus_libc_strdup(symlink_target);
		if (GlobusStat->symlink_target == NULL)
		{
			result = GlobusGFSErrorMemory("SymlinkTarget");
			goto cleanup;
		}
	}

	/* Copy out the path name. We only want to use the basename.  */
	result = misc_copy_basename(Name, &GlobusStat->name);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		if (GlobusStat->symlink_target != NULL)
			globus_free(GlobusStat->symlink_target);
		if (GlobusStat->name != NULL)
			globus_free(GlobusStat->name);

		GlobusStat->symlink_target = NULL;
		GlobusStat->name           = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

globus_result_t
misc_gfs_stat(char              * Pathname,
              globus_bool_t       UseSymlinkInfo,
              globus_gfs_stat_t * GfsStatPtr)
{
    int             retval = 0;
    globus_result_t result = GLOBUS_SUCCESS;
    hpss_stat_t     hpss_stat_buf;

    GlobusGFSName(__func__);
    GlobusGFSHpssDebugEnter();

    /* lstat() the object. */
    retval = hpss_Lstat(Pathname, &hpss_stat_buf);
    if (retval != 0)
    {
        result = GlobusGFSErrorSystemError("hpss_Lstat", -retval);
        goto cleanup;
    }

    if (S_ISLNK(hpss_stat_buf.st_mode) && !UseSymlinkInfo)
    {
        /*
         * If this fails, technically it's an error. But I think that is
         * very confusing to the user. So, instead, we will return the symlink
         * stat on error until we find a reason to do otherwise.
         */
        hpss_Stat(Pathname, &hpss_stat_buf);
    }

    result = misc_translate_stat(Pathname, &hpss_stat_buf, GfsStatPtr);
    if (result != GLOBUS_SUCCESS)
        goto cleanup;

    GlobusGFSHpssDebugExit();
    return GLOBUS_SUCCESS;

cleanup:
    GlobusGFSHpssDebugExitWithError();
    return result;
}

void
misc_destroy_gfs_stat(globus_gfs_stat_t * GfsStat)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (GfsStat->name != NULL)
		globus_free(GfsStat->name);
	if (GfsStat->symlink_target != NULL)
		globus_free(GfsStat->symlink_target);

	GlobusGFSHpssDebugExit();
}

void
misc_destroy_gfs_stat_array(globus_gfs_stat_t * GfsStatArray,
                            int                 GfsStatCount)
{
	int index = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	for (index = 0; index < GfsStatCount; index++)
	{
		misc_destroy_gfs_stat(&(GfsStatArray[index]));
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
misc_build_path(char * Directory, char * EntryName, char ** EntryPath)
{
    GlobusGFSName(__func__);
    GlobusGFSHpssDebugEnter();

    /* Construct the entry name. */
    *EntryPath = (char *) globus_malloc(strlen(Directory) + strlen(EntryName) + 2);
    if (*EntryPath == NULL)
    {
        GlobusGFSHpssDebugExitWithError();
        return GlobusGFSErrorMemory("EntryPath");
    }

    if (Directory[strlen(Directory) - 1] == '/')
        sprintf(*EntryPath, "%s%s", Directory, EntryName);
    else
        sprintf(*EntryPath, "%s/%s", Directory, EntryName);

    GlobusGFSHpssDebugExit();
    return GLOBUS_SUCCESS;
}

globus_result_t
misc_file_archived(char          * Path,
                   globus_bool_t * Archived,
                   globus_bool_t * TapeOnly)
{
	int              retval            = 0;
	int              storage_level     = 0;
	int              vv_index          = 0;
	u_signed64       max_bytes_on_disk = cast64(0);
	globus_result_t  result            = GLOBUS_SUCCESS;
	hpss_xfileattr_t xfileattr;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Archived = GLOBUS_TRUE;
	*TapeOnly = GLOBUS_FALSE;

	memset(&xfileattr, 0, sizeof(hpss_xfileattr_t));

	/*
	 * Stat the object. Without API_GET_XATTRS_NO_BLOCK, this call would hang
	 * on any file moving between levels in its hierarchy (ie staging).
	 */
	retval = hpss_FileGetXAttributes(Path,
	                                 API_GET_STATS_FOR_ALL_LEVELS|API_GET_XATTRS_NO_BLOCK,
	                                 0,
	                                 &xfileattr);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_FileGetXAttributes", -retval);
		goto cleanup;
	}

	/* Check if the top level is tape. */
	if (xfileattr.SCAttrib[0].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
	{
		*Archived = GLOBUS_TRUE;
		*TapeOnly = GLOBUS_TRUE;
		goto cleanup;
	}

	/* Handle zero length files. */
	if (eqz64m(xfileattr.Attrs.DataLength))
	{
		*Archived = GLOBUS_FALSE;
		*TapeOnly = GLOBUS_FALSE;
		goto cleanup;
	}

	/*
	 * Determine the archive status. Due to holes, we can not expect xfileattr.Attrs.DataLength
	 * bytes on disk. And we really don't know how much data is really in this file. So the
	 * algorithm works like this: assume the file is staged unless you find a tape SC that
	 * has more BytesAtLevel than the disk SCs before it.
	 */

	*Archived = GLOBUS_FALSE;

	for (storage_level = 0; storage_level < HPSS_MAX_STORAGE_LEVELS; storage_level++)
	{
		if (xfileattr.SCAttrib[storage_level].Flags & BFS_BFATTRS_LEVEL_IS_DISK)
		{
			/* Save the largest count of bytes on disk. */
			if (gt64(xfileattr.SCAttrib[storage_level].BytesAtLevel, max_bytes_on_disk))
				max_bytes_on_disk = xfileattr.SCAttrib[storage_level].BytesAtLevel;
		} else if (xfileattr.SCAttrib[storage_level].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
		{
			/* File is purged if more bytes are on disk. */
			if (gt64(xfileattr.SCAttrib[storage_level].BytesAtLevel, max_bytes_on_disk))
			{
				*Archived = GLOBUS_TRUE;
				break;
			}
		}
	}

cleanup:
	/* Free the extended information. */
	for (storage_level = 0; storage_level < HPSS_MAX_STORAGE_LEVELS; storage_level++)
	{
		for(vv_index = 0; vv_index < xfileattr.SCAttrib[storage_level].NumberOfVVs; vv_index++)
		{
			if (xfileattr.SCAttrib[storage_level].VVAttrib[vv_index].PVList != NULL)
			{
				free(xfileattr.SCAttrib[storage_level].VVAttrib[vv_index].PVList);
			}
		}
	}

	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}
	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

globus_result_t
misc_get_file_size(char * Path, globus_off_t * FileSize)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	globus_gfs_stat_t gfs_stat_buf;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Stat the file. */
	result = misc_gfs_stat(Path, GLOBUS_FALSE, &gfs_stat_buf);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	*FileSize = gfs_stat_buf.size;

	misc_destroy_gfs_stat(&gfs_stat_buf);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}
	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

#ifdef DEPRECATED
globus_result_t
misc_username_to_home(char *  UserName, 
                      char ** HomeDirectory)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*HomeDirectory = NULL;

	/* Find the passwd entry. */
	retval = getpwnam_r(UserName,
	                    &passwd_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &passwd);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("getpwnam_r", errno);
		goto cleanup;
	}

	if (passwd == NULL)
	{
		result = GlobusGFSErrorGeneric("Account not found");
		goto cleanup;
	}

	/* Copy out the home directory */
	*HomeDirectory = (char *) globus_libc_strdup(passwd->pw_dir);
	if (*HomeDirectory == NULL)
	{
		result = GlobusGFSErrorMemory("Passwd Home Directory");
		goto cleanup;
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
#endif /* DEPRECATED */

globus_result_t
misc_username_to_uid(char * UserName, 
                     int  * Uid)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Uid = -1;

	/* Find the passwd entry. */
	retval = getpwnam_r(UserName,
	                    &passwd_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &passwd);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("getpwnam_r", errno);
		goto cleanup;
	}

	if (passwd == NULL)
	{
		result = GlobusGFSErrorGeneric("Account not found");
		goto cleanup;
	}

	/* Copy out the uid */
	*Uid = passwd->pw_uid;

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
misc_uid_to_username(int Uid, char ** UserName)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*UserName = NULL;

	/* Find the passwd entry. */
	retval = getpwuid_r(Uid,
	                    &passwd_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &passwd);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("getpwuid_r", errno);
		goto cleanup;
	}

	if (passwd == NULL)
	{
		result = GlobusGFSErrorGeneric("Account not found");
		goto cleanup;
	}

	/* Copy out the username */
	*UserName = (char *) globus_libc_strdup(passwd->pw_name);
	if (*UserName == NULL)
	{
		result = GlobusGFSErrorMemory("Passwd UserName");
		goto cleanup;
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


globus_result_t
misc_groupname_to_gid(char * GroupName, gid_t * Gid)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	struct group    * group  = NULL;
	struct group      group_buf;
	char              buffer[1024];
	int               retval = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Gid = 0;

	/* Find the group entry. */
	retval = getgrnam_r(GroupName,
	                    &group_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &group);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("getgrnam_r", errno);
		goto cleanup;
	}

	if (group == NULL)
	{
		result = GlobusGFSErrorGeneric("Group not found");
		goto cleanup;
	}

	/* Copy out the group id */
	*Gid = group->gr_gid;

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
misc_gid_to_groupname(gid_t Gid, char  ** GroupName)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	struct group    * group  = NULL;
	struct group      group_buf;
	char              buffer[1024];
	int               retval = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*GroupName = 0;

	/* Find the group entry. */
	retval = getgrgid_r(Gid,
	                    &group_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &group);

	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("getgrgid_r", errno);
		goto cleanup;
	}

	if (group == NULL)
	{
		result = GlobusGFSErrorGeneric("Group not found");
		goto cleanup;
	}

	/* Copy out the group id */
	*GroupName = (char *) globus_libc_strdup(group->gr_name);
	if (*GroupName == NULL)
	{
		result = GlobusGFSErrorMemory("Group GroupName");
		goto cleanup;
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

globus_bool_t
misc_is_user_in_group(char * UserName, char * GroupName)
{
	globus_bool_t   result = GLOBUS_FALSE;
	struct group  * group  = NULL;
	struct group    group_buf;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	gid_t           gid    = 0;
	int             retval = 0;
	int             index  = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Find the group entry. */
	retval = getgrnam_r(GroupName,
	                    &group_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &group);
	if (retval != 0)
		goto cleanup;

	if (group == NULL)
		goto cleanup;

	while (group->gr_mem[index] != NULL)
	{
		if (strcmp(group->gr_mem[index], UserName) == 0)
		{
			result = GLOBUS_TRUE;
			break;
		}

		index++;
	}

	/* Save the group's gid. */
	gid = group->gr_gid;

	/* Find the user's passwd entry. */
	retval = getpwnam_r(UserName,
	                    &passwd_buf,
	                    buffer,
	                    sizeof(buffer),
	                    &passwd);
	if (retval != 0)
		goto cleanup;

	if (passwd == NULL)
		goto cleanup;

	if (passwd->pw_gid == gid)
		result = GLOBUS_TRUE;

cleanup:

	if (result == GLOBUS_FALSE)
	{
		GlobusGFSHpssDebugExitWithError();
		return GLOBUS_FALSE;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_TRUE;
}

void
misc_destroy_result(globus_result_t Result)
{
	if (Result != GLOBUS_SUCCESS)
		globus_object_free(globus_error_get(Result));
}

char *
misc_strndup(char * String, int Length)
{
	char * new_string = NULL;

	new_string = (char *) globus_malloc(Length + 1);

	globus_assert(new_string != NULL);

	snprintf(new_string, Length + 1, "%s", String);

	return new_string;
}

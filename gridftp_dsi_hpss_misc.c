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

	GlobusGFSName(misc_copy_basename);
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

static globus_result_t
misc_translate_stat(char              * Name,
                    hpss_stat_t       * HpssStat,
                    globus_gfs_stat_t * GlobusStat)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;
	char            symlink_target[HPSS_MAX_PATH_NAME];

	GlobusGFSName(misc_translate_stat);
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
misc_gfs_stat(char              *  Path,
              globus_bool_t        FileOnly,
              globus_bool_t        UseSymlinkInfo,
              globus_bool_t        IncludePathStat,
              globus_gfs_stat_t ** GfsStatArray,
              int               *  GfsStatCount)
{
	int               dir_fd     = -1;
	int               index      = 0;
	int               retval     = 0;
	char            * entry_path = NULL;
	globus_result_t   result     = GLOBUS_SUCCESS;
	hpss_dirent_t     dirent;
	hpss_stat_t       hpss_stat_buf;

	GlobusGFSName(misc_stat);
	GlobusGFSHpssDebugEnter();

	/* Initialize the returned array information. */
	*GfsStatCount = 0;
	*GfsStatArray = NULL;

	/*
	 * Start by statting the target object.
	 */
	if (UseSymlinkInfo == GLOBUS_TRUE)
	{
		retval = hpss_Lstat(Path, &hpss_stat_buf);
		if (retval != 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Lstat", -retval);
			goto cleanup;
		}
	} else
	{
		retval = hpss_Stat(Path, &hpss_stat_buf);
		if (retval < 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Stat", -retval);
			goto cleanup;
		}
	}

	/*
	 * If we only wanted the target info or if this is not a directory,
	 * then we can translate the stat info and we are done. 
	 */
	if (FileOnly == GLOBUS_TRUE || !S_ISDIR(hpss_stat_buf.st_mode))
	{
		/* Allocate the statbuf array of length 1. */
		*GfsStatCount = 1;
		*GfsStatArray = (globus_gfs_stat_t *) globus_calloc(1, sizeof(globus_gfs_stat_t));
		if (*GfsStatArray == NULL)
		{
			result = GlobusGFSErrorMemory("GfsStatArray");
			goto cleanup;
		}

		/* Translate from hpss stat to globus stat. */
		result = misc_translate_stat(Path, &hpss_stat_buf, *GfsStatArray);

		goto cleanup;
	}

	/*
	 * We need to expand this directory. 
	 */

	/* Open the directory. */
	dir_fd = hpss_Opendir(Path);
	if (dir_fd < 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Opendir", -dir_fd);
		goto cleanup;
	}

	/* Count the entries.  */
	while (TRUE)
	{
		/* Read the next entry. */
		retval = hpss_Readdir(dir_fd, &dirent);
		if (retval != 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Readdir", -retval);
			goto cleanup;
		}

		/* Check if we are done. */
		if (dirent.d_namelen == 0)
			break;

		(*GfsStatCount)++;
	}

	/* Increment GfsStatCount if we need to include the original path. */
	if (IncludePathStat == GLOBUS_TRUE)
		(*GfsStatCount)++;

	/* Rewind. */
	retval = hpss_Rewinddir(dir_fd);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Rewinddir", -retval);
		goto cleanup;
	}

	/* Allocate the array. */
	*GfsStatArray = globus_calloc(*GfsStatCount, sizeof(globus_gfs_stat_t));
	if (*GfsStatArray == NULL)
	{
		result = GlobusGFSErrorMemory("GfsStatArray");
		goto cleanup;
	}

	/* Include Path if we were supposed to. */
	if (IncludePathStat == GLOBUS_TRUE)
	{
		/* Translate from hpss stat to globus stat. */
		result = misc_translate_stat(Path,
		                               &hpss_stat_buf,
		                               &((*GfsStatArray)[index++]));
	}

	/*
	 * Record the entries. If the directory should happen to grow, let's
	 * be sure not to overflow the array.
	 */
	for (; index < *GfsStatCount; index++)
	{
		/* Read the next entry. */
		retval = hpss_Readdir(dir_fd, &dirent);
		if (retval != 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Readdir", -retval);
			goto cleanup;
		}

		/* Check if we are done. */
		if (dirent.d_namelen == 0)
			break;

		/* Construct the entry name. */
		entry_path = (char *) globus_malloc(strlen(Path) + strlen(dirent.d_name) + 2);
		if (entry_path == NULL)
		{
			result = GlobusGFSErrorMemory("entry_path");
			goto cleanup;
		}

		if (Path[strlen(Path) - 1] == '/')
			sprintf(entry_path, "%s%s", Path, dirent.d_name);
		else
			sprintf(entry_path, "%s/%s", Path, dirent.d_name);

		/* Now stat the object. XXX Should we obey UseSymlinkInfo here?*/
		if (UseSymlinkInfo == GLOBUS_TRUE)
			retval = hpss_Lstat(entry_path, &hpss_stat_buf);
		else
			retval = hpss_Stat(entry_path, &hpss_stat_buf);
		if (retval < 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Stat", -retval);
			goto cleanup;
		}

		/* Translate from hpss stat to globus stat. */
		result = misc_translate_stat(entry_path,
		                               &hpss_stat_buf,
		                               &((*GfsStatArray)[index]));

		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Free up entry_path */
		globus_free(entry_path);
		/* Mark it as freed. */
		entry_path = NULL;
	}

	/* Adjust GfsStatCount in case we didn't read enough entries for the array. */
	*GfsStatCount = index;

cleanup:
	if (dir_fd >= 0)
		hpss_Closedir(dir_fd);

	if (entry_path != NULL)
		globus_free(entry_path);

	if (result != GLOBUS_SUCCESS)
	{
		/* Destroy the stat array. */
		misc_destroy_gfs_stat_array(*GfsStatArray, *GfsStatCount);
		*GfsStatArray = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}


void
misc_destroy_gfs_stat_array(globus_gfs_stat_t * GfsStatArray,
                            int                 GfsStatCount)
{
	int index = 0;

	GlobusGFSName(misc_destroy_gfs_stat_array);
	GlobusGFSHpssDebugEnter();

	if (GfsStatArray != NULL)
	{
		for (index = 0; index < GfsStatCount; index++)
		{
			if (GfsStatArray[index].name != NULL)
				globus_free(GfsStatArray[index].name);
			if (GfsStatArray[index].symlink_target != NULL)
				globus_free(GfsStatArray[index].symlink_target);
		}
		globus_free(GfsStatArray);
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
misc_file_archived(char          * Path,
                   globus_bool_t * Archived,
                   globus_bool_t * TapeOnly)
{
	int              retval        = 0;
	int              storage_level = 0;
	int              vv_index      = 0;
	globus_result_t  result        = GLOBUS_SUCCESS;
	hpss_xfileattr_t xfileattr;

	GlobusGFSName(misc_file_archived);
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

	/* Determine the archive status. */
	for (storage_level = 0; storage_level < HPSS_MAX_STORAGE_LEVELS; storage_level++)
	{
		if (xfileattr.SCAttrib[storage_level].Flags & BFS_BFATTRS_LEVEL_IS_DISK)
		{
			/* Check for the entire file on disk at this level. */
			if (eq64m(xfileattr.SCAttrib[storage_level].BytesAtLevel, xfileattr.Attrs.DataLength))
			{
				*Archived = GLOBUS_FALSE;
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
	globus_result_t     result         = GLOBUS_SUCCESS;
	globus_gfs_stat_t * gfs_stat_array = NULL;
	int                 gfs_stat_count = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Stat the file. */
	result = misc_gfs_stat(Path,
	                       GLOBUS_TRUE,
	                       GLOBUS_FALSE,
	                       GLOBUS_TRUE,
	                       &gfs_stat_array,
	                       &gfs_stat_count);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	*FileSize = gfs_stat_array[0].size;

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
misc_username_to_home(char *  UserName, 
                      char ** HomeDirectory)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(misc_username_to_home);
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

globus_result_t
misc_username_to_uid(char * UserName, 
                     int  * Uid)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(misc_username_to_uid);
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

	GlobusGFSName(misc_uid_to_username);
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

	GlobusGFSName(misc_groupname_to_gid);
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

	GlobusGFSName(misc_gid_to_groupname);
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

	GlobusGFSName(misc_is_user_in_group);
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

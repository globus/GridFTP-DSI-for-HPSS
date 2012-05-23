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
#include "globus_gridftp_server_hpss_common.h"
#include "globus_gridftp_server_hpss_config.h"

static globus_result_t
globus_l_gfs_hpss_common_copy_basename(char * Path, char ** BaseName)
{
	char            * basename = NULL;
	globus_result_t   result   = GLOBUS_SUCCESS;

	GlobusGFSName(globus_l_gfs_hpss_common_copy_basename);
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
globus_i_gfs_hpss_common_translate_stat(char              * Name,
                                        hpss_stat_t       * HpssStat,
                                        globus_gfs_stat_t * GlobusStat)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;
	char            symlink_target[HPSS_MAX_PATH_NAME];

	GlobusGFSName(globus_i_gfs_hpss_common_translate_stat);
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
	result = globus_l_gfs_hpss_common_copy_basename(Name, &GlobusStat->name);


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
globus_l_gfs_hpss_common_stat(char              *  Path,
                              globus_bool_t        FileOnly,
                              globus_bool_t        UseSymlinkInfo,
                              globus_bool_t        IncludePathStat,
                              globus_gfs_stat_t ** StatBufArray,
                              int               *  StatBufCount)
{
	int               dir_fd     = -1;
	int               index      = 0;
	int               retval     = 0;
	char            * entry_path = NULL;
	globus_result_t   result     = GLOBUS_SUCCESS;
	hpss_dirent_t     dirent;
	hpss_stat_t       hpss_stat_buf;

	GlobusGFSName(globus_l_gfs_hpss_common_stat);
	GlobusGFSHpssDebugEnter();

	/* Initialize the returned array information. */
	*StatBufCount = 0;
	*StatBufArray = NULL;

	/*
	 * Start by stating the target object.
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
		*StatBufCount = 1;
		*StatBufArray = (globus_gfs_stat_t *) globus_calloc(1, sizeof(globus_gfs_stat_t));
		if (*StatBufArray == NULL)
		{
			result = GlobusGFSErrorMemory("StatBufArray");
			goto cleanup;
		}

		/* Translate from hpss stat to globus stat. */
		result = globus_i_gfs_hpss_common_translate_stat(Path,
		                                                 &hpss_stat_buf,
		                                                 *StatBufArray);

		goto cleanup;
	}

	/*
	 * We need to expand this directory. 
	 */

	/* Open the directory. */
	dir_fd = hpss_Opendir(Path);
	if (dir_fd < 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Opendir", -retval);
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

		(*StatBufCount)++;
	}

	/* Increment StatBufCount if we need to include the original path. */
	if (IncludePathStat == GLOBUS_TRUE)
	{
		(*StatBufCount)++;
	}

	/* Allocate the array. */
	*StatBufArray = globus_calloc(*StatBufCount, sizeof(globus_gfs_stat_t));
	if (*StatBufArray == NULL)
	{
		result = GlobusGFSErrorMemory("StatBufArray");
		goto cleanup;
	}

	/* Rewind. */
	retval = hpss_Rewinddir(dir_fd);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Rewinddir", -retval);
		goto cleanup;
	}

	/* Include Path if we were supposed to. */
	if (IncludePathStat == GLOBUS_TRUE)
	{
		/* Translate from hpss stat to globus stat. */
		result = globus_i_gfs_hpss_common_translate_stat(Path,
		                                                 &hpss_stat_buf,
		                                                 &((*StatBufArray)[index++]));
	}
	/*
	 * Record the entries. If the directory should happen to grow, let's
	 * be sure not to overflow the array.
	 */
	for (; index < *StatBufCount; index++)
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
		retval = hpss_Stat(entry_path, &hpss_stat_buf);
		if (retval < 0)
		{
			result = GlobusGFSErrorSystemError("hpss_Stat", -retval);
			goto cleanup;
		}

		/* Translate from hpss stat to globus stat. */
		result = globus_i_gfs_hpss_common_translate_stat(entry_path,
		                                                 &hpss_stat_buf,
		                                                 &((*StatBufArray)[index]));

		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Free up entry_path */
		globus_free(entry_path);
		/* Mark it as freed. */
		entry_path = NULL;
	}


cleanup:
	if (dir_fd >= 0)
		hpss_Closedir(dir_fd);
	if (entry_path != NULL)
		globus_free(entry_path);

	if (result != GLOBUS_SUCCESS)
	{
		/* Destroy the stat array. */
		globus_l_gfs_hpss_common_destroy_stat_array(*StatBufArray,
		                                            *StatBufCount);
		*StatBufArray = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}


void
globus_l_gfs_hpss_common_destroy_stat_array(
    globus_gfs_stat_t * StatArray,
    int                 StatCount)
{
	int index = 0;

	GlobusGFSName(globus_l_gfs_hpss_common_destroy_stat_array);
	GlobusGFSHpssDebugEnter();

	if (StatArray != NULL)
	{
		for (index = 0; index < StatCount; index++)
		{
			if (StatArray[index].name != NULL)
				globus_free(StatArray[index].name);
			if (StatArray[index].symlink_target != NULL)
				globus_free(StatArray[index].symlink_target);
		}
		globus_free(StatArray);
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
globus_l_gfs_hpss_common_username_to_home(char *  UserName, 
                                          char ** HomeDirectory)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(globus_l_gfs_hpss_common_username_to_home);
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
globus_l_gfs_hpss_common_username_to_uid(char * UserName, 
                                         int  * Uid)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(globus_l_gfs_hpss_common_username_to_uid);
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
globus_l_gfs_hpss_common_uid_to_username(int     Uid,
                                         char ** UserName)
{
	globus_result_t result = GLOBUS_SUCCESS;
	struct passwd * passwd = NULL;
	struct passwd   passwd_buf;
	char            buffer[1024];
	int             retval = 0;

	GlobusGFSName(globus_l_gfs_hpss_common_uid_to_username);
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
globus_l_gfs_hpss_common_groupname_to_gid(char  * GroupName,
                                          gid_t * Gid)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	struct group    * group  = NULL;
	struct group      group_buf;
	char              buffer[1024];
	int               retval = 0;

	GlobusGFSName(globus_l_gfs_hpss_common_groupname_to_gid);
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

void
globus_l_gfs_hpss_common_destroy_result(globus_result_t Result)
{
	globus_object_free(globus_error_get(Result));
}

char *
globus_l_gfs_hpss_common_strndup(char * String, int Length)
{
	char * new_string = NULL;

	new_string = (char *) globus_malloc(Length + 1);

	snprintf(new_string, Length + 1, "%s", String);

	return new_string;
}

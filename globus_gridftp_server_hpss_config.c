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
#include <ctype.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <grp.h>
#include <pwd.h>

/*
 * Globus includes.
 */
#include <globus_common.h>
#include <globus_options.h>
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "globus_gridftp_server_hpss_config.h"
#include "globus_gridftp_server_hpss_common.h"

/*
 * Yeah, it's global. Sue me. I'm getting tired of passing it around everywhere.
 */
static config_t * _Config = NULL;

static void
globus_i_gfs_hpss_find_next_word(char *  Buffer,
                                 char ** Word,
                                 int  *  Length)
{
    GlobusGFSName(globus_i_gfs_hpss_find_next_word);
    GlobusGFSHpssDebugEnter();

    *Word   = NULL;
    *Length = 0;

	if (Buffer == NULL)
		goto cleanup;

    /* Skip spacing. */
    while (isspace(*Buffer)) Buffer++;

    /* Skip EOL */
    if (*Buffer == '\0' || *Buffer == '\n')
        goto cleanup;

    /* Skip comments. */
    if (*Buffer == '#')
        goto cleanup;

    /* Return the start of the found keep word */
    *Word = Buffer;

    /* Find the length of the word. */
    while (!isspace(*Buffer) && *Buffer != '\0' && *Buffer != '\n')
	{
		(*Length)++;
		Buffer++;
	}

cleanup:
    GlobusGFSHpssDebugExit();
}

static globus_result_t
globus_i_gfs_hpss_parse_config_file(char * ConfigFile)
{
    int                index        = 0;
    int                tmp_length   = 0;
    int                key_length   = 0;
    int                value_length = 0;
    int                retval       = 0;
    FILE            *  config_f     = NULL;
    char            *  tmp          = NULL;
    char            *  key          = NULL;
    char            *  value        = NULL;
    char               buffer[1024];
    globus_result_t    result       = GLOBUS_SUCCESS;

    GlobusGFSName(globus_i_gfs_hpss_parse_config_file);
    GlobusGFSHpssDebugEnter();

    /* If no config file was given... */
    if (ConfigFile == NULL)
    {
        /* Check if the default exists. */
        retval = access(DEFAULT_CONFIG_FILE, R_OK);

        /* If we can not access the file for reading... */
        if (retval != 0)
        {
            /* Construct the error */
            result = GlobusGFSErrorSystemError("Can not access config file", errno);
            goto cleanup;
        }

        /* Use the default config file. */
        ConfigFile = DEFAULT_CONFIG_FILE;
    }

    /*
     * Open the config file.
     */
    config_f = fopen(ConfigFile, "r");
    if (config_f == NULL)
    {
        result = GlobusGFSErrorSystemError("Failed to open config file", errno);
        goto cleanup;
    }

    while (fgets(buffer, sizeof(buffer), config_f) != NULL)
    {
        /* Reset index. */
        index = 0;

        /* Locate the keyword */
        globus_i_gfs_hpss_find_next_word(buffer, &key, &key_length);
        if (key == NULL)
            continue;

        /* Locate the value */
        globus_i_gfs_hpss_find_next_word(key+key_length, &value, &value_length);
        if (key == NULL)
        {
            result = GlobusGFSErrorWrapFailed("Unknown configuration option",
                                              GlobusGFSErrorGeneric(buffer));
            goto cleanup;
        }

        /* Make sure the value was the last word. */
        /* Locate the value */
        globus_i_gfs_hpss_find_next_word(value+value_length, &tmp, &tmp_length);
        if (tmp != NULL)
        {
            result = GlobusGFSErrorWrapFailed("Unknown configuration option",
                                              GlobusGFSErrorGeneric(buffer));
            goto cleanup;
        }

        /* Now match the directive. */
        if (key_length == strlen("LoginName") && 
		    strncasecmp(key, "LoginName", key_length) == 0)
        {
            _Config->LoginName = globus_l_gfs_hpss_common_strndup(value, value_length);
        } else if (key_length == strlen("KeytabFile") && 
		           strncasecmp(key, "KeytabFile", key_length) == 0)
        {
            _Config->KeytabFile = globus_l_gfs_hpss_common_strndup(value, value_length);
        } else
        {
            result = GlobusGFSErrorWrapFailed("Unknown configuration option",
                                              GlobusGFSErrorGeneric(buffer));
            goto cleanup;
        }
    }

cleanup:
    if (config_f != NULL)
        fclose(config_f);

    if (result != GLOBUS_SUCCESS)
    {
        GlobusGFSHpssDebugExitWithError();
        return result;
    }

    GlobusGFSHpssDebugExit();
    return GLOBUS_SUCCESS;
}

globus_result_t
globus_l_gfs_hpss_config_init(char * ConfigFile)
{
    globus_result_t result = GLOBUS_SUCCESS;

    GlobusGFSName(globus_l_gfs_hpss_config_init);
    GlobusGFSHpssDebugEnter();

	if (_Config == NULL)
	{
		_Config = (config_t *) globus_calloc(1, sizeof(config_t));
		if (_Config == NULL)
		{
			result = GlobusGFSErrorMemory("session_handle");
			goto cleanup;
		}
	}

    /* Initialize the config for sane cleanup. */
    memset(_Config, 0, sizeof(config_t));

    /* Parse the config file. */
    result = globus_i_gfs_hpss_parse_config_file(ConfigFile);
    if (result != GLOBUS_SUCCESS)
        goto cleanup;

	/* Make sure these values are defined. */
	if (_Config->LoginName == NULL)
	{
		result = GlobusGFSErrorGeneric("Missing LoginName in configuration file!\n");
		goto cleanup;
	}

	if (_Config->KeytabFile == NULL)
	{
		result = GlobusGFSErrorGeneric("Missing KeytabFile in configuration file!\n");
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

void
globus_l_gfs_hpss_config_destroy()
{
    GlobusGFSName(globus_l_gfs_hpss_config_destroy);
    GlobusGFSHpssDebugEnter();

	if (_Config != NULL)
	{
		if (_Config->LoginName != NULL)
			globus_free(_Config->LoginName);
		if (_Config->KeytabFile != NULL)
			globus_free(_Config->KeytabFile);

		_Config->LoginName   = NULL;
		_Config->KeytabFile  = NULL;
	}

    GlobusGFSHpssDebugExit();
}

char *
globus_l_gfs_hpss_config_get_login_name()
{
	char * login_name = NULL;

	globus_assert(_Config != NULL);
	globus_assert(_Config->LoginName != NULL);

	login_name = strdup(_Config->LoginName);
	globus_assert(login_name != NULL);
	return login_name;
}

char *
globus_l_gfs_hpss_config_get_keytab()
{
	char * keytab_file = NULL;

	globus_assert(_Config != NULL);
	globus_assert(_Config->KeytabFile != NULL);

	keytab_file = strdup(_Config->KeytabFile);
	globus_assert(keytab_file != NULL);
	return keytab_file;
}

static globus_bool_t
globus_l_gfs_hpss_config_is_user_in_group(char * UserName,
                                          char * GroupName)
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

    GlobusGFSName(globus_l_gfs_hpss_config_is_user_in_group);
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


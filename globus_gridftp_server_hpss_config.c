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
        } else if (key_length == strlen("ProjectFile") &&
                   strncasecmp(key, "ProjectFile", key_length) == 0)
        {
            _Config->ProjectFile = globus_l_gfs_hpss_common_strndup(value, value_length);
        } else if (key_length == strlen("FamilyFile") &&
                   strncasecmp(key, "FamilyFile", key_length) == 0)
        {
            _Config->FamilyFile = globus_l_gfs_hpss_common_strndup(value, value_length);
        } else if (key_length == strlen("CosFile") &&
                   strncasecmp(key, "CosFile", key_length) == 0)
        {
            _Config->CosFile = globus_l_gfs_hpss_common_strndup(value, value_length);
        } else if (key_length == strlen("Admin") &&
                   strncasecmp(key, "Admin", key_length) == 0)
        {
            _Config->AdminList = globus_l_gfs_hpss_common_strndup(value, value_length);
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
		if (_Config->ProjectFile != NULL)
			globus_free(_Config->ProjectFile);
		if (_Config->FamilyFile != NULL)
			globus_free(_Config->FamilyFile);
		if (_Config->CosFile != NULL)
			globus_free(_Config->CosFile);
		if (_Config->AdminList != NULL)
			globus_free(_Config->AdminList);

		_Config->LoginName   = NULL;
		_Config->KeytabFile  = NULL;
		_Config->ProjectFile = NULL;
		_Config->FamilyFile  = NULL;
		_Config->CosFile     = NULL;
		_Config->AdminList   = NULL;
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

static globus_bool_t
globus_l_gfs_hpss_config_is_user_in_project(char * UserName,
                                            char * ProjectName)
{
	FILE          * project_f = NULL;
	char          * user      = NULL;
	char          * project   = NULL;
	char          * newline   = NULL;
	char          * saveptr   = NULL;
	char            buffer[1024];
	globus_bool_t   result    = GLOBUS_FALSE;

    GlobusGFSName(globus_l_gfs_hpss_config_is_user_in_project);
    GlobusGFSHpssDebugEnter();

	/* Check for a project file */
	if (_Config->ProjectFile == NULL)
		goto cleanup;

	project_f = fopen(_Config->ProjectFile, "r");
	if (project_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), project_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the user */
		user = strtok_r(buffer, ":", &saveptr);

		if (user == NULL || strcmp(user, UserName) != 0)
			continue;

		/* Get the projects */
		while ((project = strtok_r(NULL, ",", &saveptr)) != NULL)
		{
			if (strcmp(project, ProjectName) == 0)
			{
				result = GLOBUS_TRUE;
				goto cleanup;
			}
		}
	}

cleanup:

	if (project_f != NULL)
		fclose(project_f);

    if (result == GLOBUS_FALSE)
    {
        GlobusGFSHpssDebugExitWithError();
        return GLOBUS_FALSE;
    }

    GlobusGFSHpssDebugExit();
    return GLOBUS_TRUE;
}

/*
 * Family file format is:
 *  family_name:family_id:acl_list
 *
 *  where family_name is a character string (max length 15)
 *        family_id is an integer
 *        acl_list is [[u:]user[,p:project][,g:group]]
 *
 *  The special keyword 'all' in the acl_list enables that
 *  family for all users.
 *  
 *  No spaces (leading, following or in between).
 *  Lines starting with # are comments.
 *
 * Family can be the name or the id
 */
globus_bool_t
globus_l_gfs_hpss_config_can_user_use_family(char * UserName,
                                             char * Family)
{
	FILE          * family_f      = NULL;
	char          * family_name   = NULL;
	char          * family_id_str = NULL;
	char          * entry         = NULL;
	char          * saveptr       = NULL;
	char          * newline       = NULL;
	char            buffer[1024];
	globus_bool_t   result = GLOBUS_FALSE;

    GlobusGFSName(globus_l_gfs_hpss_config_can_user_use_family);
    GlobusGFSHpssDebugEnter();

	/* Check for a family file */
	if (_Config->FamilyFile == NULL)
		goto cleanup;

	family_f = fopen(_Config->FamilyFile, "r");
	if (family_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), family_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the family name*/
		family_name = strtok_r(buffer, ":", &saveptr);
		if (family_name == NULL)
			continue;

		/* Get the family id */
		family_id_str = strtok_r(NULL, ":", &saveptr);
		if (family_id_str == NULL)
			continue;

		/* Check that Family matches the name or the id. */
		if (strcmp(family_name,   Family) != 0 &&
		    strcmp(family_id_str, Family) != 0)
		{
			continue;
		}

		/* Get the entries in the list */
		while ((entry = strtok_r(NULL, ",", &saveptr)) != NULL)
		{
			if (strncmp(entry, "g:", 2) == 0)
			{
				result = globus_l_gfs_hpss_config_is_user_in_group(UserName, entry+2);
			} else if (strncmp(entry, "p:", 2) == 0)
			{
				result = globus_l_gfs_hpss_config_is_user_in_project(UserName, entry+2);
			} else if (strncmp(entry, "u:", 2) == 0)
			{
				if (strcmp(entry+2, UserName) == 0)
					result = GLOBUS_TRUE;
			} else if (strcmp(entry, UserName) == 0)
			{
					result = GLOBUS_TRUE;
			} else if (strcasecmp(entry, "all") == 0)
			{
				result = GLOBUS_TRUE;
			}

			if (result == GLOBUS_TRUE)
				break;
		}
	}
cleanup:
	if (family_f != NULL)
		fclose(family_f);

    if (result == GLOBUS_FALSE)
    {
        GlobusGFSHpssDebugExitWithError();
        return GLOBUS_FALSE;
    }

    GlobusGFSHpssDebugExit();
    return GLOBUS_TRUE;
}

char *
globus_l_gfs_hpss_config_get_my_families(char * UserName)
{
	FILE          * family_f      = NULL;
	char          * family_list   = NULL;
	char          * family_name   = NULL;
	char          * family_id     = NULL;
	char          * entry         = NULL;
	char          * saveptr       = NULL;
	char          * newline       = NULL;
	char            buffer[1024];
	globus_bool_t   can_use_family = GLOBUS_FALSE;
	globus_bool_t   user_is_admin  = GLOBUS_FALSE;

    GlobusGFSName(globus_l_gfs_hpss_config_get_my_families);
    GlobusGFSHpssDebugEnter();

	/* Check if the user is an admin. */
	user_is_admin = globus_l_gfs_hpss_config_is_user_admin(UserName);

	/* Check for a family file */
	if (_Config->FamilyFile == NULL)
		goto cleanup;

	family_f = fopen(_Config->FamilyFile, "r");
	if (family_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), family_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the family name*/
		family_name = strtok_r(buffer, ":", &saveptr);
		if (family_name == NULL)
			continue;

		/* Get the family id */
		family_id = strtok_r(NULL, ":", &saveptr);
		if (family_id == NULL)
			continue;

		can_use_family = GLOBUS_FALSE;

		/* Get the entries in the list */
		while ((entry = strtok_r(NULL, ",", &saveptr)) != NULL)
		{
			if (strncmp(entry, "g:", 2) == 0)
			{
				can_use_family = globus_l_gfs_hpss_config_is_user_in_group(UserName, entry+2);
			} else if (strncmp(entry, "p:", 2) == 0)
			{
				can_use_family = globus_l_gfs_hpss_config_is_user_in_project(UserName, entry+2);
			} else if (strncmp(entry, "u:", 2) == 0)
			{
				if (strcmp(entry+2, UserName) == 0)
					can_use_family = GLOBUS_TRUE;
			} else if (strcmp(entry, UserName) == 0)
			{
					can_use_family = GLOBUS_TRUE;
			} else if (strcasecmp(entry, "all") == 0)
			{
				can_use_family = GLOBUS_TRUE;
			}

			if (can_use_family)
				break;
		}

		if (user_is_admin == GLOBUS_TRUE)
			can_use_family = GLOBUS_TRUE;

		if (can_use_family == GLOBUS_TRUE)
		{
			/* Add this family to the list. */
			if (family_list == NULL)
			{
				family_list = (char *) globus_malloc(strlen(family_id) + strlen(family_name) + 2);
				sprintf(family_list, "%s:%s", family_id, family_name);
			} else
			{
				family_list = (char *) globus_realloc(family_list, strlen(family_list) + 
				                                                   strlen(family_id)   +
				                                                   strlen(family_name) + 3);

				strcat(family_list, ",");
				strcat(family_list, family_id);
				strcat(family_list, ":");
				strcat(family_list, family_name);
			}
		}
	}
cleanup:
	if (family_f != NULL)
		fclose(family_f);

    GlobusGFSHpssDebugExit();
    return family_list;
}
int
globus_l_gfs_hpss_config_get_family_id(char * Family)
{
	int    family_id     = -1;
	FILE * family_f      = NULL;
	char * family_name   = NULL;
	char * family_id_str = NULL;
	char * saveptr       = NULL;
	char * newline       = NULL;
	char   buffer[1024];

    GlobusGFSName(globus_l_gfs_hpss_config_get_family_id);
    GlobusGFSHpssDebugEnter();

	/* Check for a family file */
	if (_Config->FamilyFile == NULL)
		goto cleanup;

	family_f = fopen(_Config->FamilyFile, "r");
	if (family_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), family_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the family name*/
		family_name = strtok_r(buffer, ":", &saveptr);
		if (family_name == NULL)
			continue;

		/* Get the family id */
		family_id_str = strtok_r(NULL, ":", &saveptr);
		if (family_id_str == NULL)
			continue;

		/* Check that Family matches the name or the id. */
		if (strcasecmp(family_name,   Family) == 0 ||
		    strcasecmp(family_id_str, Family) == 0)
		{
			family_id = atoi(family_id_str);
			break;
		}
	}
cleanup:
	if (family_f != NULL)
		fclose(family_f);

    GlobusGFSHpssDebugExit();
    return family_id;
}

char *
globus_l_gfs_hpss_config_get_family_name(char * Family)
{
	FILE * family_f      = NULL;
	char * family_name   = NULL;
	char * family_id_str = NULL;
	char * saveptr       = NULL;
	char * newline       = NULL;
	char   buffer[1024];

    GlobusGFSName(globus_l_gfs_hpss_config_get_family_name);
    GlobusGFSHpssDebugEnter();

	/* Check for a family file */
	if (_Config->FamilyFile == NULL)
		goto cleanup;

	family_f = fopen(_Config->FamilyFile, "r");
	if (family_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), family_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the family name*/
		family_name = strtok_r(buffer, ":", &saveptr);
		if (family_name == NULL)
			continue;

		/* Get the family id */
		family_id_str = strtok_r(NULL, ":", &saveptr);
		if (family_id_str == NULL)
			continue;

		/* Check that Family matches the name or the id. */
		if (strcasecmp(family_name,   Family) == 0 ||
		    strcasecmp(family_id_str, Family) == 0)
		{
			break;
		}

		/* Reset family_name */
		family_name = NULL;
	}
cleanup:
	if (family_f != NULL)
		fclose(family_f);

    GlobusGFSHpssDebugExit();

	if (family_name != NULL)
		return strdup(family_name);

    return NULL;
}

/*
 * COS file format is:
 *  cos_id:cos_name:acl_list
 *
 *  where cos_name is a character string (max length 15)
 *        cos_id is an integer
 *        acl_list is [[u:]user[,p:project][,g:group]]
 *
 *  The special keyword 'all' in the acl_list enables that
 *  COS for all users.
 *
 *  No spaces (leading or following).
 *  Lines starting with # are comments.
 *
 * Cos can be the name or the id
 */
globus_bool_t
globus_l_gfs_hpss_config_can_user_use_cos(char * UserName,
                                          char * Cos)
{
	FILE          * cos_f    = NULL;
	char          * cos_id   = NULL;
	char          * cos_name = NULL;
	char          * entry    = NULL;
	char          * saveptr  = NULL;
	char          * newline  = NULL;
	char            buffer[1024];
	globus_bool_t   result = GLOBUS_FALSE;

    GlobusGFSName(globus_l_gfs_hpss_config_can_user_use_cos);
    GlobusGFSHpssDebugEnter();

	/* Check for a COS file */
	if (_Config->CosFile == NULL)
		goto cleanup;

	cos_f = fopen(_Config->CosFile, "r");
	if (cos_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), cos_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the cos id. */
		cos_id = strtok_r(buffer, ":", &saveptr);
		if (cos_id == NULL)
			continue;

		/* Get the cos name*/
		cos_name = strtok_r(NULL, ":", &saveptr);
		if (cos_name == NULL)
			continue;

		if (strcasecmp(cos_name, Cos) != 0 && strcasecmp(cos_id, Cos) != 0)
			continue;

		/* Get the entries in the list */
		while ((entry = strtok_r(NULL, ",", &saveptr)) != NULL)
		{
			if (strncmp(entry, "g:", 2) == 0)
			{
				result = globus_l_gfs_hpss_config_is_user_in_group(UserName, entry+2);
			} else if (strncmp(entry, "p:", 2) == 0)
			{
				result = globus_l_gfs_hpss_config_is_user_in_project(UserName, entry+2);
			} else if (strncmp(entry, "u:", 2) == 0)
			{
				if (strcmp(entry+2, UserName) == 0)
					result = GLOBUS_TRUE;
			} else if (strcmp(entry, UserName) == 0)
			{
					result = GLOBUS_TRUE;
			} else if (strcasecmp(entry, "all") == 0)
			{
					result = GLOBUS_TRUE;
			}

			if (result == GLOBUS_TRUE)
				break;
		}
	}
cleanup:
	if (cos_f != NULL)
		fclose(cos_f);

    if (result == GLOBUS_FALSE)
    {
        GlobusGFSHpssDebugExitWithError();
        return GLOBUS_FALSE;
    }

    GlobusGFSHpssDebugExit();
    return GLOBUS_TRUE;
}

char *
globus_l_gfs_hpss_config_get_my_cos(char * UserName)
{
	FILE          * cos_f    = NULL;
	char          * cos_list = NULL;
	char          * cos_id   = NULL;
	char          * cos_name = NULL;
	char          * entry    = NULL;
	char          * saveptr  = NULL;
	char          * newline  = NULL;
	char            buffer[1024];
	globus_bool_t   can_use_cos   = GLOBUS_FALSE;
	globus_bool_t   user_is_admin = GLOBUS_FALSE;

    GlobusGFSName(globus_l_gfs_hpss_config_get_my_cos);
    GlobusGFSHpssDebugEnter();

	/* Check if the user is an admin. */
	user_is_admin = globus_l_gfs_hpss_config_is_user_admin(UserName);

	/* Check for a COS file */
	if (_Config->CosFile == NULL)
		goto cleanup;

	cos_f = fopen(_Config->CosFile, "r");
	if (cos_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), cos_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the cos id. */
		cos_id = strtok_r(buffer, ":", &saveptr);
		if (cos_id == NULL)
			continue;

		/* Get the cos name*/
		cos_name = strtok_r(NULL, ":", &saveptr);
		if (cos_name == NULL)
			continue;

		can_use_cos = GLOBUS_FALSE;

		/* Get the entries in the list */
		while ((entry = strtok_r(NULL, ",", &saveptr)) != NULL)
		{
			if (strncmp(entry, "g:", 2) == 0)
			{
				can_use_cos = globus_l_gfs_hpss_config_is_user_in_group(UserName, entry+2);
			} else if (strncmp(entry, "p:", 2) == 0)
			{
				can_use_cos = globus_l_gfs_hpss_config_is_user_in_project(UserName, entry+2);
			} else if (strncmp(entry, "u:", 2) == 0)
			{
				if (strcmp(entry+2, UserName) == 0)
					can_use_cos = GLOBUS_TRUE;
			} else if (strcmp(entry, UserName) == 0)
			{
					can_use_cos = GLOBUS_TRUE;
			} else if (strcasecmp(entry, "all") == 0)
			{
					can_use_cos = GLOBUS_TRUE;
			}

			if (can_use_cos == GLOBUS_TRUE)
				break;
		}

		if (user_is_admin == GLOBUS_TRUE)
			can_use_cos = GLOBUS_TRUE;

		if (can_use_cos == GLOBUS_TRUE)
		{
			/* Add this cos to the list. */
			if (cos_list == NULL)
			{
				cos_list = (char *) globus_malloc(strlen(cos_id) + strlen(cos_name) + 2);
				sprintf(cos_list, "%s:%s", cos_id, cos_name);
			} else
			{
				cos_list = (char *) globus_realloc(cos_list, strlen(cos_list) + 
				                                             strlen(cos_id)   +
				                                             strlen(cos_name) + 3);

				strcat(cos_list, ",");
				strcat(cos_list, cos_id);
				strcat(cos_list, ":");
				strcat(cos_list, cos_name);
			}
		}
	}
cleanup:
	if (cos_f != NULL)
		fclose(cos_f);

    GlobusGFSHpssDebugExit();
    return cos_list;
}
int
globus_l_gfs_hpss_config_get_cos_id(char * Cos)
{
	int    cos_id     = -1;
	FILE * cos_f      = NULL;
	char * cos_id_str = NULL;
	char * cos_name   = NULL;
	char * saveptr    = NULL;
	char * newline    = NULL;
	char   buffer[1024];

    GlobusGFSName(globus_l_gfs_hpss_config_get_cos_id);
    GlobusGFSHpssDebugEnter();

	/* Check for a COS file */
	if (_Config->CosFile == NULL)
		goto cleanup;

	cos_f = fopen(_Config->CosFile, "r");
	if (cos_f == NULL)
		goto cleanup;

	while (fgets(buffer, sizeof(buffer), cos_f) != NULL)
	{
		/* Remove the newline. */
		newline = strchr(buffer, '\n');
		if (newline != NULL)
			*newline = '\0';

		/* Skip comments. */
		if (buffer[0] == '#')
			continue;

		/* Reset saveptr */
		saveptr = NULL;

		/* Get the id. */
		cos_id_str = strtok_r(buffer, ":", &saveptr);
		if (cos_id_str == NULL)
			continue;

		/* Get the cos name */
		cos_name = strtok_r(NULL, ":", &saveptr);
		if (cos_name == NULL)
			continue;

		if (strcasecmp(cos_name, Cos) == 0 || strcasecmp(cos_id_str, Cos) == 0)
		{
			cos_id = atoi(cos_id_str);
			break;
		}
	}
cleanup:
	if (cos_f != NULL)
		fclose(cos_f);

    GlobusGFSHpssDebugExit();
    return cos_id;
}

globus_bool_t
globus_l_gfs_hpss_config_is_user_admin(char * UserName)
{
	char          * admin_list = NULL;
	char          * tmp        = NULL;
	char          * saveptr    = NULL;
	char          * entry      = NULL;
	globus_bool_t   result     = GLOBUS_FALSE;

    GlobusGFSName(globus_l_gfs_hpss_config_is_user_admin);
    GlobusGFSHpssDebugEnter();

	/* If no list, no one is admin */
	if (_Config->AdminList == NULL)
		goto cleanup;

	/* Duplicate the list. */
	tmp = admin_list = globus_libc_strdup(_Config->AdminList);

	while ((entry = strtok_r(tmp, ",", &saveptr)) != NULL)
	{
		tmp = NULL;

		if (strncmp(entry, "g:", 2) == 0)
		{
			result = globus_l_gfs_hpss_config_is_user_in_group(UserName, entry+2);
		} else if (strncmp(entry, "u:", 2) == 0)
		{
			result = (strcmp(entry+2, UserName) == 0);
		} else
		{
			result = (strcmp(entry, UserName) == 0);
		}

		if (result == GLOBUS_TRUE)
			break;
	}

cleanup:
	if (admin_list != NULL)
		globus_free(admin_list);

    if (result == GLOBUS_FALSE)
    {
        GlobusGFSHpssDebugExitWithError();
        return GLOBUS_FALSE;
    }

    GlobusGFSHpssDebugExit();
    return GLOBUS_TRUE;
}

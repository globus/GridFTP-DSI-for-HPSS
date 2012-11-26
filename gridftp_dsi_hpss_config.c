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
#include <strings.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdio.h>
#include <errno.h>
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
#include "gridftp_dsi_hpss_config.h"
#include "gridftp_dsi_hpss_misc.h"

#ifdef DMALLOC
/*
 * Dmalloc
 */
#include <dmalloc.h>
#endif /* DMALLOC */

typedef enum {
	ACL_TYPE_USER,
	ACL_TYPE_GROUP,
} acl_type_t;

typedef struct acl_list {
	acl_type_t        Type;
	char            * Name;
	struct acl_list * Next;
} acl_list_t;

typedef struct TranslationFile {
	struct translation {
		int                  ID;
		char               * Name;
		acl_list_t         * ACLList;
		struct translation * Next;
	} * Translations;
} TranslationFile_t;

struct config_handle {
	char   * ConfigFile;

	struct {
		char       * LoginName;
		char       * KeytabFile;
		char       * FamilyFile;
		char       * CosFile;
		acl_list_t * AdminList;
	} Config;

	/* Family file entries. */
	TranslationFile_t Family;
	TranslationFile_t Cos;
};

static void
config_destroy_acl_list(acl_list_t * ACLList)
{
	acl_list_t * acl_save = NULL;

	GlobusGFSName(config_destroy_acl_list);
	GlobusGFSHpssDebugEnter();

	while (ACLList != NULL)
	{
		/* Save off this ACL */
		acl_save = ACLList;
		/* Move the list forward. */
		ACLList = ACLList->Next;

		if (acl_save->Name != NULL)
			free(acl_save->Name);
		free(acl_save);
	}

    GlobusGFSHpssDebugExit();
}

static void
config_destroy_translations(struct translation * Translations)
{
	struct translation * trans_save = NULL;

	GlobusGFSName(config_destroy_translations);
	GlobusGFSHpssDebugEnter();

	if (Translations != NULL)
	{
		/* Save off this entry. */
		trans_save = Translations;
		/* Move the list forward. */
		Translations = Translations->Next;

		if (trans_save->Name != NULL)
			globus_free(trans_save->Name);

		config_destroy_acl_list(trans_save->ACLList);
		globus_free(trans_save);
	}

    GlobusGFSHpssDebugExit();
}

/*
 * Helper that removes leading whitespace, newlines, comments, etc.
 */
static void
config_find_next_word(char *  Buffer,
                      char ** Word,
                      int  *  Length)
{
	GlobusGFSName(config_find_next_word);
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

/*
 * String will be trashed.
 */
static globus_result_t
config_create_acl_list(char       *  String,
                       acl_list_t ** ACLList)
{
	int               length  = 0;
	char            * tmp     = String;
	char            * word    = NULL;
	char            * entry   = NULL;
	char            * saveptr = NULL;
	acl_list_t      * acl     = NULL;
	globus_result_t   result  = GLOBUS_SUCCESS;

	GlobusGFSName(config_create_acl_list);
	GlobusGFSHpssDebugEnter();

	if (String == NULL)
		goto cleanup;

	while ((entry = strtok_r(tmp, ",", &saveptr)) != NULL)
	{
		tmp = NULL;

		/* This will remove any garbage. */
		config_find_next_word(entry, &word, &length);

		/* Skip if there was nothing there. */
		if (length == 0)
			continue;

		/* Allocate the new ACL. */
		acl = (acl_list_t *) globus_calloc(1, sizeof(acl_list_t));
		if (acl == NULL)
		{
			result = GlobusGFSErrorMemory("config_handle_t");
			goto cleanup;
		}

		/* Set the default. */
		acl->Type = ACL_TYPE_USER;

		if (strncmp(word, "u:", 2) == 0)
		{
			acl->Type = ACL_TYPE_USER;
			word   += 2;
			length -= 2;
		} else if (strncmp(word, "g:", 2) == 0)
		{
			acl->Type = ACL_TYPE_GROUP;
			word   += 2;
			length -= 2;
		}

		acl->Name = misc_strndup(word, length);

		/* Now put it on the list. */
		acl->Next = *ACLList;
		*ACLList = acl;
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		/* Destroy the ACL list. */
		config_destroy_acl_list(*ACLList);
		/* Release our handle to it. */
		*ACLList = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

    GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static globus_result_t
config_create_translation(char               *  IDStr,
                          char               *  Name,
                          char               *  ACLList,
                          struct translation ** Translation)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;

    GlobusGFSName(config_create_translation);
    GlobusGFSHpssDebugEnter();

	/* Allocate the translation. */
	*Translation = (struct translation *) globus_calloc(1, sizeof(struct translation));
	if (*Translation == NULL)
	{
		result = GlobusGFSErrorMemory("struct translation");
		goto cleanup;
	}

	/* Convert the id. */
	retval = sscanf(IDStr, "%d", &(*Translation)->ID);
	if (retval != 1)
	{
		result = GlobusGFSErrorWrapFailed("Not an integer value", GlobusGFSErrorGeneric(IDStr));
		goto cleanup;
	}

	/* Copy in the name. */
	(*Translation)->Name = globus_libc_strdup(Name);

	/* Create the ACL list. */
	result = config_create_acl_list(ACLList, &(*Translation)->ACLList);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		/* Destroy the translations. */
		config_destroy_translations(*Translation);
		/* Release our handle. */
		*Translation = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

    GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * Translation file all have the same format.
 *
 *  id:name:acl_list
 *
 *  where id is an integer
 *        name is a character string
 *        acl_list is [[u:]user[,g:group]]
 *
 *  The special keyword 'all' in the acl_list enables that
 *  entry for all users.
 *  
 *  No spaces (leading, following or in between).
 *  Lines starting with # are comments.
 *
 */
static globus_result_t
config_parse_translation_file(TranslationFile_t * TranslationFile,
                              char              * FileName)
{
	FILE               *  file_f           = NULL;
	char               *  name             = NULL;
	char               *  id_str           = NULL;
	char               *  acl_list         = NULL;
	char               *  saveptr          = NULL;
	char               *  newline          = NULL;
	struct translation *  translation      = NULL;
	struct translation ** translation_tail = NULL;
	char                  buffer[1024];
	globus_result_t       result           = GLOBUS_SUCCESS;

    GlobusGFSName(config_parse_translation_file);
    GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	TranslationFile->Translations = NULL;

	/* Save a pointer to the tail. */
	translation_tail = &TranslationFile->Translations;

	/* Open the translation file. */
	file_f = fopen(FileName, "r");
	if (file_f == NULL)
	{
		result = GlobusGFSErrorWrapFailed("Failed to open file", 
		                                  GlobusGFSErrorSystemError(FileName, errno));
		goto cleanup;
	}

	/* For each line... */
	while (fgets(buffer, sizeof(buffer), file_f) != NULL)
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

		/* Get the id */
		id_str = strtok_r(buffer, ":", &saveptr);
		if (id_str == NULL)
			continue;

		/* Get the name*/
		name = strtok_r(NULL, ":", &saveptr);
		if (name == NULL)
			continue;

		/* Get the acl list. This can be empty. */
		acl_list = strtok_r(NULL, ":", &saveptr);

		/* Create the translation. */
		result = config_create_translation(id_str, name, acl_list, &translation);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Put this translation on the end of the list. */
		*translation_tail = translation;

		/* Move the tail forward. */
		translation_tail = &translation->Next;

		/* Release our handle. */
		translation = NULL;
	}
cleanup:
	if (file_f != NULL)
		fclose(file_f);

    if (result != GLOBUS_SUCCESS)
    {
		/* Destroy the tranlsations. */
		config_destroy_translations(TranslationFile->Translations);
		/* Release our handle. */
		TranslationFile->Translations = NULL;

        GlobusGFSHpssDebugExitWithError();
        return result;
    }

    GlobusGFSHpssDebugExit();
    return GLOBUS_SUCCESS;
}

/*
 * Family file format is:
 *  family_id:family_name:acl_list
 *
 *  where family_id is an integer
 *        family_name is a character string (max length 15)
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
globus_result_t
config_parse_family_file(config_handle_t   * ConfigHandle,
                         TranslationFile_t * Family)
{
	globus_result_t result = GLOBUS_SUCCESS;

    GlobusGFSName(config_parse_family_file);
    GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	Family->Translations = NULL;

	/* Check for a family file */
	if (ConfigHandle->Config.FamilyFile == NULL)
		goto cleanup;

	result = config_parse_translation_file(Family, ConfigHandle->Config.FamilyFile);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to parse family file", result);
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
globus_result_t
config_parse_cos_file(config_handle_t   * ConfigHandle,
                      TranslationFile_t * Cos)
{
	globus_result_t result = GLOBUS_SUCCESS;

    GlobusGFSName(config_parse_cos_file);
    GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	Cos->Translations = NULL;

	/* Check for a cos file */
	if (ConfigHandle->Config.CosFile == NULL)
		goto cleanup;

	result = config_parse_translation_file(Cos, ConfigHandle->Config.CosFile);
	if (result != GLOBUS_SUCCESS)
	{
		result = GlobusGFSErrorWrapFailed("Failed to parse cos file", result);
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

static globus_result_t
config_parse_config_file(config_handle_t * ConfigHandle)
{
	int                index        = 0;
	int                tmp_length   = 0;
	int                key_length   = 0;
	int                value_length = 0;
	FILE            *  config_f     = NULL;
	char            *  tmp          = NULL;
	char            *  key          = NULL;
	char            *  value        = NULL;
	char               buffer[1024];
	globus_result_t    result       = GLOBUS_SUCCESS;

	GlobusGFSName(config_parse_config_file);
	GlobusGFSHpssDebugEnter();

	/*
	 * Open the config file.
	 */
	config_f = fopen(ConfigHandle->ConfigFile, "r");
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
		config_find_next_word(buffer, &key, &key_length);
		if (key == NULL)
			continue;

		/* Locate the value */
		config_find_next_word(key+key_length, &value, &value_length);
		if (key == NULL)
		{
			result = GlobusGFSErrorWrapFailed("Unknown configuration option",
			                                  GlobusGFSErrorGeneric(buffer));
			goto cleanup;
		}

		/* Make sure the value was the last word. */
		/* Locate the value */
		config_find_next_word(value+value_length, &tmp, &tmp_length);
		if (tmp != NULL)
		{
			result = GlobusGFSErrorWrapFailed("Unknown configuration option",
			                                  GlobusGFSErrorGeneric(buffer));
			goto cleanup;
		}

		/* Now match the directive. */
		if (key_length == strlen("LoginName") && strncasecmp(key, "LoginName", key_length) == 0)
		{
			ConfigHandle->Config.LoginName = misc_strndup(value, value_length);
		} else if (key_length == strlen("KeytabFile") && strncasecmp(key, "KeytabFile", key_length) == 0)
		{
			ConfigHandle->Config.KeytabFile = misc_strndup(value, value_length);
		} else if (key_length == strlen("FamilyFile") && strncasecmp(key, "FamilyFile", key_length) == 0)
		{
			ConfigHandle->Config.FamilyFile = misc_strndup(value, value_length);
		} else if (key_length == strlen("CosFile") && strncasecmp(key, "CosFile", key_length) == 0)
		{
			ConfigHandle->Config.CosFile = misc_strndup(value, value_length);
		} else if (key_length == strlen("Admin") && strncasecmp(key, "Admin", key_length) == 0)
		{
			/* Construct the admin acl list. */
			result = config_create_acl_list(value, &ConfigHandle->Config.AdminList);
			if (result != GLOBUS_SUCCESS)
				goto cleanup;
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

void
config_destroy(config_handle_t * ConfigHandle)
{
	GlobusGFSName(config_destroy);
	GlobusGFSHpssDebugEnter();

	if (ConfigHandle != NULL)
	{
		if (ConfigHandle->Config.LoginName != NULL)
			globus_free(ConfigHandle->Config.LoginName);
		if (ConfigHandle->Config.KeytabFile != NULL)
			globus_free(ConfigHandle->Config.KeytabFile);
		if (ConfigHandle->Config.FamilyFile != NULL)
			globus_free(ConfigHandle->Config.FamilyFile);
		if (ConfigHandle->Config.CosFile != NULL)
			globus_free(ConfigHandle->Config.CosFile);
		config_destroy_acl_list(ConfigHandle->Config.AdminList);

		config_destroy_translations(ConfigHandle->Family.Translations);
		config_destroy_translations(ConfigHandle->Cos.Translations);
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
config_init(char * ConfigFile, config_handle_t ** ConfigHandle)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(config_init);
	GlobusGFSHpssDebugEnter();

	*ConfigHandle = (config_handle_t *) globus_calloc(1, sizeof(config_handle_t));
	if (*ConfigHandle == NULL)
	{
		result = GlobusGFSErrorMemory("config_handle_t");
		goto cleanup;
	}

	/* Save the config file. */
	if (ConfigFile != NULL)
		(*ConfigHandle)->ConfigFile = globus_libc_strdup(ConfigFile);
	else
		(*ConfigHandle)->ConfigFile = globus_libc_strdup("/var/hpss/etc/gridftp.conf");

	/* Parse the config file. */
	result = config_parse_config_file(*ConfigHandle);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Parse the family translation file. */
	result = config_parse_family_file(*ConfigHandle, &(*ConfigHandle)->Family);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Parse the cos translation file. */
	result = config_parse_cos_file(*ConfigHandle, &(*ConfigHandle)->Cos);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		/* Destroy our config. */
		config_destroy(*ConfigHandle);
		/* Release our handle. */
		*ConfigHandle = NULL;

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

char *
config_get_login_name(config_handle_t * ConfigHandle)
{
	GlobusGFSName(config_get_login_name);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();
	return ConfigHandle->Config.LoginName;
}

char *
config_get_keytab_file(config_handle_t * ConfigHandle)
{
	GlobusGFSName(config_get_login_name);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();
	return ConfigHandle->Config.KeytabFile;
}

/*
 *  Cos is the name.
 */
int
config_get_cos_id(config_handle_t * ConfigHandle, char * Cos)
{
	int                  cos_id      = CONFIG_NO_COS_ID;
	struct translation * translation = NULL;

	GlobusGFSName(config_get_cos_id);
	GlobusGFSHpssDebugEnter();

	for (translation  = ConfigHandle->Cos.Translations; 
	     translation != NULL; 
	     translation = translation->Next)
	{
		if (strcmp(translation->Name, Cos) == 0)
		{
			cos_id = translation->ID;
			break;
		}
	}

	GlobusGFSHpssDebugExit();

	return cos_id;
}

/*
 * The returned value is not dup'ed.
 */
char *
config_get_cos_name(config_handle_t * ConfigHandle, int CosID)
{
	char               * cos_name    = NULL;
	struct translation * translation = NULL;

	GlobusGFSName(config_get_cos_name);
	GlobusGFSHpssDebugEnter();

	for (translation  = ConfigHandle->Cos.Translations; 
	     translation != NULL; 
	     translation = translation->Next)
	{
		if (translation->ID == CosID)
		{
			cos_name = translation->Name;
			break;
		}
	}

	GlobusGFSHpssDebugExit();

	return cos_name;
}

globus_bool_t
config_user_use_cos(config_handle_t * ConfigHandle,
                    char            * UserName,
                    int               CosID)
{
	globus_bool_t        result      = GLOBUS_FALSE;
	acl_list_t         * acl_list    = NULL;
	struct translation * translation = NULL;

	GlobusGFSName(config_user_use_cos);
	GlobusGFSHpssDebugEnter();

	/* Search for the right COS tranlation. */
	for (translation  = ConfigHandle->Cos.Translations; 
	     translation != NULL; 
	     translation = translation->Next)
	{
		if (translation->ID == CosID)
		{
			/* Search the ACL list. */
			for (acl_list  = translation->ACLList;
			     acl_list != NULL;
			     acl_list  = acl_list->Next)
			{
				switch (acl_list->Type)
				{
				case ACL_TYPE_USER:
					if (strcasecmp(acl_list->Name, UserName) == 0)
					{
						result = GLOBUS_TRUE;
						goto cleanup;
					}
					break;
				case ACL_TYPE_GROUP:
					result = misc_is_user_in_group(UserName, acl_list->Name);
					if (result == GLOBUS_TRUE)
						goto cleanup;
					break;
				}
			}
		}
	}
cleanup:
	GlobusGFSHpssDebugExit();

	return result;
}

static globus_result_t
config_add_string_to_list(char *** List,
                          char *   String,
                          int      Index)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(config_add_string_to_list);
	GlobusGFSHpssDebugEnter();

	/* Initialize the list on the first pass. */
	if (Index == 0)
		*List = NULL;

	/* Extend the array. */
	*List = (char **) globus_realloc(*List, sizeof(char *) * (Index + 2));
	if (*List == NULL)
	{
		result = GlobusGFSErrorMemory("cos list");
		goto cleanup;
	}

	/* Copy the string. */
	(*List)[Index] = globus_libc_strdup(String);

	if ((*List)[Index] == NULL)
	{
		result = GlobusGFSErrorMemory("cos list");
		goto cleanup;
	}

	/* NULL terminate the array. */
	(*List)[Index + 1] = NULL;

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		if (*List != NULL)
		{
			/* Free the list. */
			for (Index = 0; (*List)[Index] != NULL; Index++)
			{
				globus_free((*List)[Index]);
			}
			globus_free(*List);
		}

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

globus_result_t
config_get_user_cos_list(config_handle_t *   ConfigHandle,
                         char            *   UserName,
                         char            *** CosList)
{
	int                   index         = 0;
	acl_list_t         *  acl_list      = NULL;
	globus_result_t       result        = GLOBUS_SUCCESS;
	globus_bool_t         user_in_group = GLOBUS_FALSE;
	globus_bool_t         skip_acls     = GLOBUS_FALSE;
	struct translation *  translation   = NULL;

	GlobusGFSName(config_get_user_cos_list);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*CosList = NULL;

	/* Search for the right COS tranlation. */
	for (translation  = ConfigHandle->Cos.Translations; 
	     translation != NULL; 
	     translation  = translation->Next)
	{
		/* Admins can access all classes of service. */
		if (config_user_is_admin(ConfigHandle, UserName) == GLOBUS_TRUE)
		{
			/* Add this cos to the list. */
			result = config_add_string_to_list(CosList,
			                                   translation->Name,
			                                   index++);
			if (result != GLOBUS_SUCCESS)
				goto cleanup;

			/* No need to parse the ACL list. */
			continue;
		}

		/* Initialize this before the loop. */
		skip_acls = GLOBUS_FALSE;

		/* Search the ACL list. */
		for (acl_list  = translation->ACLList;
		     acl_list != NULL;
		     acl_list  = acl_list->Next)
		{
			switch (acl_list->Type)
			{
			case ACL_TYPE_USER:
				if (strcasecmp(acl_list->Name, UserName) == 0)
				{
					result = config_add_string_to_list(CosList,
					                                   translation->Name,
					                                   index++);
					if (result != GLOBUS_SUCCESS)
						goto cleanup;

					/* We can skip the reset of the acls on this list. */
					skip_acls = GLOBUS_TRUE;
				}
				break;
			case ACL_TYPE_GROUP:
				user_in_group = misc_is_user_in_group(UserName, acl_list->Name);
				if (user_in_group == GLOBUS_TRUE)
				{
					result = config_add_string_to_list(CosList,
					                                   translation->Name,
					                                   index++);
					if (result != GLOBUS_SUCCESS)
						goto cleanup;

					/* We can skip the reset of the acls on this list. */
					skip_acls = GLOBUS_TRUE;
				}
				break;
			}
		}
	}

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

/*
 *  Family is the name.
 */
int
config_get_family_id(config_handle_t * ConfigHandle, char * Family)
{
	int                  fam_id      = CONFIG_NO_FAMILY_ID;
	struct translation * translation = NULL;

	GlobusGFSName(config_get_family_id);
	GlobusGFSHpssDebugEnter();

	for (translation  = ConfigHandle->Cos.Translations; 
	     translation != NULL; 
	     translation = translation->Next)
	{
		if (strcmp(translation->Name, Family) == 0)
		{
			fam_id = translation->ID;
			break;
		}
	}

	GlobusGFSHpssDebugExit();

	return fam_id;
}

/*
 * The returned value is not dup'ed.
 */
char *
config_get_family_name(config_handle_t * ConfigHandle, int FamilyID)
{
	char               * family_name = NULL;
	struct translation * translation = NULL;

	GlobusGFSName(config_get_family_name);
	GlobusGFSHpssDebugEnter();

	for (translation  = ConfigHandle->Family.Translations; 
	     translation != NULL; 
	     translation = translation->Next)
	{
		if (translation->ID == FamilyID)
		{
			family_name = translation->Name;
			break;
		}
	}

	GlobusGFSHpssDebugExit();

	return family_name;
}

globus_bool_t
config_user_use_family(config_handle_t * ConfigHandle,
                       char            * UserName,
                       int               FamilyID)
{
	globus_bool_t        result      = GLOBUS_FALSE;
	acl_list_t         * acl_list    = NULL;
	struct translation * translation = NULL;

	GlobusGFSName(config_user_use_family);
	GlobusGFSHpssDebugEnter();

	/* Search for the right COS tranlation. */
	for (translation  = ConfigHandle->Family.Translations; 
	     translation != NULL; 
	     translation = translation->Next)
	{
		if (translation->ID == FamilyID)
		{
			/* Search the ACL list. */
			for (acl_list  = translation->ACLList;
			     acl_list != NULL;
			     acl_list  = acl_list->Next)
			{
				switch (acl_list->Type)
				{
				case ACL_TYPE_USER:
					if (strcasecmp(acl_list->Name, UserName) == 0)
					{
						result = GLOBUS_TRUE;
						goto cleanup;
					}
					break;
				case ACL_TYPE_GROUP:
					result = misc_is_user_in_group(UserName, acl_list->Name);
					if (result == GLOBUS_TRUE)
						goto cleanup;
					break;
				}
			}
		}
	}
cleanup:
	GlobusGFSHpssDebugExit();

	return result;
}

globus_result_t
config_get_user_family_list(config_handle_t *   ConfigHandle,
                            char            *   UserName,
                            char            *** FamilyList)
{
	int                   index         = 0;
	acl_list_t         *  acl_list      = NULL;
	globus_result_t       result        = GLOBUS_SUCCESS;
	globus_bool_t         user_in_group = GLOBUS_FALSE;
	globus_bool_t         skip_acls     = GLOBUS_FALSE;
	struct translation *  translation   = NULL;

	GlobusGFSName(config_get_user_family_list);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*FamilyList = NULL;

	/* Search for the right COS tranlation. */
	for (translation  = ConfigHandle->Family.Translations; 
	     translation != NULL; 
	     translation  = translation->Next)
	{
		/* Admins can access all families. */
		if (config_user_is_admin(ConfigHandle, UserName) == GLOBUS_TRUE)
		{
			/* Add this family to the list. */
			result = config_add_string_to_list(FamilyList,
			                                   translation->Name,
			                                   index++);
			if (result != GLOBUS_SUCCESS)
				goto cleanup;

			/* No need to parse the ACL list. */
			continue;
		}

		/* Initialize this before the loop. */
		skip_acls = GLOBUS_FALSE;

		/* Search the ACL list. */
		for (acl_list  = translation->ACLList;
		     acl_list != NULL;
		     acl_list  = acl_list->Next)
		{
			switch (acl_list->Type)
			{
			case ACL_TYPE_USER:
				if (strcasecmp(acl_list->Name, UserName) == 0)
				{
					result = config_add_string_to_list(FamilyList,
					                                   translation->Name,
					                                   index);
					if (result != GLOBUS_SUCCESS)
						goto cleanup;

					/* We can skip the reset of the acls on this list. */
					skip_acls = GLOBUS_TRUE;
				}
				break;
			case ACL_TYPE_GROUP:
				user_in_group = misc_is_user_in_group(UserName, acl_list->Name);
				if (user_in_group == GLOBUS_TRUE)
				{
					result = config_add_string_to_list(FamilyList,
					                                   translation->Name,
					                                   index);
					if (result != GLOBUS_SUCCESS)
						goto cleanup;

					/* We can skip the reset of the acls on this list. */
					skip_acls = GLOBUS_TRUE;
				}
				break;
			}
		}
	}

cleanup:
	GlobusGFSHpssDebugExit();
	return result;
}

globus_bool_t
config_user_is_admin(config_handle_t * ConfigHandle, char * UserName)
{
	globus_bool_t   result = GLOBUS_FALSE;
	acl_list_t    * acl_list = NULL;

	GlobusGFSName(config_user_is_admin);
	GlobusGFSHpssDebugEnter();

	/* Search the ACL list. */
	for (acl_list  = ConfigHandle->Config.AdminList;
	     acl_list != NULL;
	     acl_list  = acl_list->Next)
	{
		switch (acl_list->Type)
		{
		case ACL_TYPE_USER:
			if (strcasecmp(acl_list->Name, UserName) == 0)
			{
				result = GLOBUS_TRUE;
				goto cleanup;
			}
			break;
		case ACL_TYPE_GROUP:
			result = misc_is_user_in_group(UserName, acl_list->Name);
			if (result == GLOBUS_TRUE)
				goto cleanup;
			break;
		}
	}

cleanup:
	GlobusGFSHpssDebugExit();

	return result;
}

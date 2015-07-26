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
#include <stdlib.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_Getenv.h>

/*
 * Local includes
 */
#include "config.h"

/*
 * The config file search order is:
 *   1) env HPSS_DSI_CONFIG_FILE=<path>
 *   2) $HPSS_PATH_ETC/gridftp.conf
 *   3) DEFAULT_CONFIG_FILE (/var/hpss/etc/gridftp.conf)
 */
globus_result_t
config_find_config_file(char ** ConfigFilePath)
{
	globus_result_t   result        = GLOBUS_SUCCESS;
	char            * hpss_path_etc = NULL;
	int               retval        = 0;

	GlobusGFSName(config_find_config_file);

	/* Initialize the return value. */
	*ConfigFilePath = NULL;

	if (getenv("HPSS_DSI_CONFIG_FILE"))
	{
		*ConfigFilePath = strdup(getenv("HPSS_DSI_CONFIG_FILE"));
		if (!*ConfigFilePath)
		{
			result = GlobusGFSErrorMemory("HPSS_DSI_CONFIG_FILE");
			goto cleanup;
		}

		/* Check if it exists and if we have access. */
		retval = access(*ConfigFilePath, R_OK);
		if (retval)
			result = GlobusGFSErrorGeneric("Could not open config file defined in environment");
		goto cleanup;
	}

	/* Check for HPSS_PATH_ETC in the environment. */
	hpss_path_etc = hpss_Getenv("HPSS_PATH_ETC");

	/* If it exists... */
	if (hpss_path_etc != NULL)
	{
		/* Construct the full path. */
		*ConfigFilePath = globus_common_create_string("%s/gridftp.conf", hpss_path_etc);
		if (!*ConfigFilePath)
		{
			result = GlobusGFSErrorMemory("config file path");
			goto cleanup;
		}

		/* Check if it exists and if we have access. */
		retval = access(*ConfigFilePath, R_OK);

		/* If we have access, we are done. */
		if (retval == 0)
			goto cleanup;

		/* Release the full path to the config file. */
		globus_free(*ConfigFilePath);
		*ConfigFilePath = NULL;

		/* Otherwise, determine why we do not have access. */
		switch (errno)
		{
		/*
		 * For all cases in which the file does not exist...
		 */
		case ENOENT:
		case ENOTDIR:
			break;

		/*
		 * All other cases indicate failure at some level.
		 */
		default:
			result = GlobusGFSErrorSystemError("Can not access config file", errno);
			goto cleanup;
		}
	}

	/*
	 * No success from the environment, let's check the default.
	 */

	/* Check if it exists and if we have access. */
	retval = access(DEFAULT_CONFIG_FILE, R_OK);

	/* All failures are error conditions at this stage. */
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("Can not access config file", errno);
		goto cleanup;
	}

	/* Copy out the config file. */
	*ConfigFilePath = globus_libc_strdup(DEFAULT_CONFIG_FILE);
	if (!*ConfigFilePath)
	{
		result = GlobusGFSErrorMemory("config file path");
		goto cleanup;
	}

cleanup:
	if (result != GLOBUS_SUCCESS && *ConfigFilePath)
		globus_free(*ConfigFilePath);

	return result;
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

	*Word   = NULL;
	*Length = 0;

	if (Buffer == NULL)
		return;

	/* Skip spacing. */
	while (isspace(*Buffer)) Buffer++;

	/* Skip EOL */
	if (*Buffer == '\0' || *Buffer == '\n')
		return;

	/* Skip comments. */
	if (*Buffer == '#')
		return;

	/* Return the start of the found keep word */
	*Word = Buffer;

	/* Find the length of the word. */
	while (!isspace(*Buffer) && *Buffer != '\0' && *Buffer != '\n')
	{
		(*Length)++;
		Buffer++;
	}
}

int
config_get_bool_value(char * Value, int ValueLength)
{
	if (!strncasecmp(Value, "on", ValueLength)   ||
	    !strncasecmp(Value, "true", ValueLength) ||
	    !strncasecmp(Value, "yes", ValueLength))
	{
		return 1;
	}

	return 0;
}

static globus_result_t
config_parse_file(char     * ConfigFilePath,
                  config_t * Config)
{
	int                tmp_length   = 0;
	int                key_length   = 0;
	int                value_length = 0;
	FILE            *  config_f     = NULL;
	char            *  tmp          = NULL;
	char            *  key          = NULL;
	char            *  value        = NULL;
	char               buffer[1024];
	globus_result_t    result       = GLOBUS_SUCCESS;

	GlobusGFSName(config_parse_file);

	/*
	 * Open the config file.
	 */
	config_f = fopen(ConfigFilePath, "r");
	if (!config_f)
	{
		result = GlobusGFSErrorWrapFailed("Attempting to open config file",
		                                  GlobusGFSErrorSystemError("fopen()", errno));
		goto cleanup;
	}

	while (fgets(buffer, sizeof(buffer), config_f) != NULL)
	{
		/* Locate the keyword */
		config_find_next_word(buffer, &key, &key_length);
		if (key == NULL)
			continue;

		/* Locate the value */
		config_find_next_word(key+key_length, &value, &value_length);
		if (key == NULL)
		{
			result = GlobusGFSErrorWrapFailed("Parsing config options",
			                                  GlobusGFSErrorGeneric(buffer));
			goto cleanup;
		}

		/* Make sure the value was the last word. */
		config_find_next_word(value+value_length, &tmp, &tmp_length);
		if (tmp != NULL)
		{
			result = GlobusGFSErrorWrapFailed("Parsing config options",
			                                  GlobusGFSErrorGeneric(buffer));
			goto cleanup;
		}

		/* Now match the directive. */
		if (key_length == strlen("LoginName") && strncasecmp(key, "LoginName", key_length) == 0)
		{
			Config->LoginName = strndup(value, value_length);
		} else if (key_length == strlen("AuthenticationMech") &&
		           strncasecmp(key, "AuthenticationMech", key_length) == 0)
		{
			Config->AuthenticationMech = strndup(value, value_length);
		} else if (key_length == strlen("Authenticator") && strncasecmp(key, "Authenticator", key_length) == 0)
		{
			Config->Authenticator = strndup(value, value_length);
		} else if (key_length == strlen("QuotaSupport") && strncasecmp(key, "QuotaSupport", key_length) == 0)
		{
			Config->QuotaSupport = config_get_bool_value(value, value_length);
		} else
		{
			result = GlobusGFSErrorWrapFailed("Parsing config options", GlobusGFSErrorGeneric(buffer));
			goto cleanup;
		}
	}

cleanup:
	if (config_f != NULL)
		fclose(config_f);

	return result;
}

globus_result_t
config_init(config_t ** Config)
{
	char          * config_file_path = NULL;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(config_init);

	/* Find the config file. */
	result = config_find_config_file(&config_file_path);
	if (result != GLOBUS_SUCCESS)
		return result;

	/* Allocate the config struct */
	*Config = globus_malloc(sizeof(config_t));
	if (!*Config)
	{
		globus_free(config_file_path);
		return GlobusGFSErrorMemory("config_t");
	}
	memset(*Config, 0, sizeof(config_t));

	result = config_parse_file(config_file_path, *Config);
	if (result)
	{
		config_destroy(*Config);
		*Config = NULL;
	}
	globus_free(config_file_path);
	return result;
}

void
config_destroy(config_t * Config)
{
	if (Config)
	{
		if (Config->LoginName)
			globus_free(Config->LoginName);
		if (Config->AuthenticationMech)
			globus_free(Config->AuthenticationMech);
		if (Config->Authenticator)
			globus_free(Config->Authenticator);

		globus_free(Config);
	}
}


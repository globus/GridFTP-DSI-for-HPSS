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
 *   1) $HPSS_PATH_ETC/gridftp.conf
 *   2) DEFAULT_CONFIG_FILE (/var/hpss/etc/gridftp.conf)
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

globus_result_t
config_init(config_t ** Config)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(config_init);

	/* Allocate the session struct */
	*Config = globus_malloc(sizeof(config_t));
	if (!*Config)
		return GlobusGFSErrorMemory("config_t");
	memset(*Config, 0, sizeof(config_t));

    return GLOBUS_SUCCESS;
}

void
config_destroy(config_t * Config)
{
	if (Config)
		globus_free(Config);
}


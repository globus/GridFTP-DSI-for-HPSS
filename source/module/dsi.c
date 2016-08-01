/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2012-2014 NCSA.  All rights reserved.
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
#include <string.h>

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
#include "authenticate.h"
#include "commands.h"
#include "markers.h"
#include "config.h"
#include "stat.h"
#include "stor.h"
#include "retr.h"

void
dsi_init(globus_gfs_operation_t      Operation,
         globus_gfs_session_info_t * SessionInfo)
{
	globus_result_t result = GLOBUS_SUCCESS;
	config_t      * config = NULL;
	char          * home   = NULL;
	sec_cred_t      user_cred;

	GlobusGFSName(dsi_init);

	/*
	 * Read in the config.
	 */
	result = config_init(&config);
	if (result)
		goto cleanup;

	/* Now authenticate. */
	result = authenticate(config->LoginName,
	                      config->AuthenticationMech,
	                      config->Authenticator,
	                      SessionInfo->username);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/*
	 * Pulling the HPSS directory from the user's credential will support
	 * sites that use HPSS LDAP.
	 */
	result = hpss_GetThreadUcred(&user_cred);
	if (result)
		goto cleanup;

	home = strdup(user_cred.Directory);
	if (!home)
	{
		result = GlobusGFSErrorMemory("home directory");
		goto cleanup;
	}

	result = commands_init(Operation);

cleanup:

	/*
	 * Inform the server that we are done. If we do not pass in a username, the
	 * server will use the name we mapped to with GSI. If we do not pass in a
	 * home directory, the server will (1) look it up if we are root or
	 * (2) leave it as the unprivileged user's home directory.
	 *
	 * As far as I can tell, the server keeps a pointer to home_directory and frees
	 * it when it is done.
	 */
	globus_gridftp_server_finished_session_start(Operation,
	                                             result,
	                                             result ? NULL : config,
	                                             NULL,  // username
	                                             result ? NULL : home);

	if (result) config_destroy(config);
	if (result && home) free(home);
}


void
dsi_destroy(void * Arg)
{
	if (Arg)
		config_destroy(Arg);
}

int
dsi_restart_transfer(globus_gfs_transfer_info_t * TransferInfo)
{
	globus_off_t offset;
	globus_off_t length;

	if (globus_range_list_size(TransferInfo->range_list) != 1)
		return 1;

	globus_range_list_at(TransferInfo->range_list, 0, &offset, &length);
		return (offset != 0 || length != -1);
}

void
dsi_send(globus_gfs_operation_t       Operation,
         globus_gfs_transfer_info_t * TransferInfo,
         void                       * UserArg)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(dsi_send);

	retr(Operation, TransferInfo);
}

static void
dsi_recv(globus_gfs_operation_t       Operation,
         globus_gfs_transfer_info_t * TransferInfo,
         void                       * UserArg)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(dsi_recv);

	if (dsi_restart_transfer(TransferInfo) && !markers_restart_supported())
	{
		result = GlobusGFSErrorGeneric("Restarts are not supported");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	stor(Operation, TransferInfo, UserArg);
}

void
dsi_command(globus_gfs_operation_t      Operation,
            globus_gfs_command_info_t * CommandInfo,
            void                      * UserArg)
{
	commands_run(Operation, CommandInfo, UserArg, globus_gridftp_server_finished_command);
}

void
dsi_stat(globus_gfs_operation_t   Operation,
         globus_gfs_stat_info_t * StatInfo,
         void                   * Arg)
{
	GlobusGFSName(dsi_stat);

	globus_result_t   result = GLOBUS_SUCCESS;
	globus_gfs_stat_t gfs_stat;

	switch (StatInfo->use_symlink_info)
	{
	case 0:
		result = stat_object(StatInfo->pathname, &gfs_stat);
		break;
	default:
		result = stat_link(StatInfo->pathname, &gfs_stat);
		break;
	}

	if (result != GLOBUS_SUCCESS || StatInfo->file_only || !S_ISDIR(gfs_stat.mode))
	{
		globus_gridftp_server_finished_stat(Operation, result, &gfs_stat, 1);
		stat_destroy(&gfs_stat);
		return;
	}

	stat_destroy(&gfs_stat);

#define STAT_ENTRIES_PER_REPLY 200
	/*
	 * Directory listing.
	 */

	hpss_fileattr_t dir_attrs;

	int retval;
	if ((retval = hpss_FileGetAttributes(StatInfo->pathname, &dir_attrs)) < 0)
	{
		result = GlobusGFSErrorSystemError("hpss_FileGetAttributes", -retval);
		globus_gridftp_server_finished_stat(Operation, result, NULL, 0);
		return;
	}

	uint64_t offset = 0;
	uint32_t end    = FALSE;
	while (!end)
	{
		globus_gfs_stat_t gfs_stat_array[STAT_ENTRIES_PER_REPLY];
		uint32_t count_out;

		result = stat_directory_entries(&dir_attrs.ObjectHandle,
		                                offset,
		                                STAT_ENTRIES_PER_REPLY,
		                                &end,
		                                &offset,
		                                gfs_stat_array,
		                                &count_out);
		if (result)
			break;

		globus_gridftp_server_finished_stat_partial(Operation,
		                                            GLOBUS_SUCCESS,
		                                            gfs_stat_array,
		                                            count_out);

		stat_destroy_array(gfs_stat_array, count_out);
	}

	globus_gridftp_server_finished_stat(Operation, result, NULL, 0);
}


/* Can request ordered data in globus-gridftp-server 11.x, removing the need to force transfers to a single stream */
#ifdef GLOBUS_GFS_DSI_DESCRIPTOR_REQUIRES_ORDERED_DATA
#define HPSS_DESC GLOBUS_GFS_DSI_DESCRIPTOR_SENDER | \
                  GLOBUS_GFS_DSI_DESCRIPTOR_REQUIRES_ORDERED_DATA
#else
#define HPSS_DESC GLOBUS_GFS_DSI_DESCRIPTOR_SENDER
#endif

globus_gfs_storage_iface_t hpss_local_dsi_iface =
{
	HPSS_DESC,    /* Descriptor       */
	dsi_init,     /* init_func        */
	dsi_destroy,  /* destroy_func     */
	NULL,         /* list_func        */
	dsi_send,     /* send_func        */
	dsi_recv,     /* recv_func        */
	NULL,         /* trev_func        */
	NULL,         /* active_func      */
	NULL,         /* passive_func     */
	NULL,         /* data_destroy     */
	dsi_command,  /* command_func     */
	dsi_stat,     /* stat_func        */
	NULL,         /* set_cred_func    */
	NULL,         /* buffer_send_func */
	NULL,         /* realpath_func    */
};

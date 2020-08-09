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
 * Local includes
 */
#include "authenticate.h"
#include "commands.h"
#include "logging.h"
#include "config.h"
#include "fixups.h"
#include "retr.h"
#include "stat.h"
#include "stor.h"
#include "hpss.h"

void
dsi_init(globus_gfs_operation_t     Operation,
         globus_gfs_session_info_t *SessionInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;
    config_t *      config = NULL;
    char *          home   = NULL;
    sec_cred_t      user_cred;

    logging_init();

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
    result = Hpss_GetThreadUcred(&user_cred);
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
     * As far as I can tell, the server keeps a pointer to home_directory and
     * frees it when it is done.
     */
    globus_gridftp_server_finished_session_start(Operation,
                                                 result,
                                                 result ? NULL : config,
                                                 NULL, // username
                                                 result ? NULL : home);

    if (result)
        config_destroy(config);
    if (result && home)
        free(home);
}

void
dsi_destroy(void *Arg)
{
    if (Arg)
        config_destroy(Arg);
}

void
dsi_send(globus_gfs_operation_t      Operation,
         globus_gfs_transfer_info_t *TransferInfo,
         void *                      UserArg)
{
    retr(Operation, TransferInfo);
}

static void
dsi_recv(globus_gfs_operation_t      Operation,
         globus_gfs_transfer_info_t *TransferInfo,
         void *                      UserArg)
{
    stor(Operation, TransferInfo, UserArg);
}

void
dsi_command(globus_gfs_operation_t     Operation,
            globus_gfs_command_info_t *CommandInfo,
            void *                     UserArg)
{
    globus_result_t result;

    switch (CommandInfo->command)
    {
    case GLOBUS_GFS_CMD_RMD:
        result = commands_rmdir(CommandInfo->pathname);
        result = fixup_rmd(CommandInfo->pathname, result);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    default:
        commands_run(Operation,
                     CommandInfo,
                     UserArg,
                     globus_gridftp_server_finished_command);
    }
}

struct _stat_dir_cb_arg {
    globus_gfs_operation_t   Operation;
    globus_gfs_stat_info_t * StatInfo;
};

static globus_result_t
_stat_dir_callback(globus_gfs_stat_t * GFSStatArray,
                   uint32_t            ArrayLength,
                   uint32_t            End,
                   void              * CallbackArg)
{
    struct _stat_dir_cb_arg * cb_arg = CallbackArg;

    globus_result_t result;
    result = fixup_stat_directory(cb_arg->StatInfo->pathname,
                                  GFSStatArray,
                                  &ArrayLength);

    if (result != GLOBUS_SUCCESS)
        return result;

    if (!End)
        globus_gridftp_server_finished_stat_partial(cb_arg->Operation,
                                                    GLOBUS_SUCCESS,
                                                    GFSStatArray,
                                                    ArrayLength);
    else 
        globus_gridftp_server_finished_stat(cb_arg->Operation,
                                            result,
                                            GFSStatArray,
                                            ArrayLength);

    return GLOBUS_SUCCESS;
}

void
dsi_stat(globus_gfs_operation_t   Operation,
         globus_gfs_stat_info_t * StatInfo,
         void *                   Arg)
{
    globus_result_t   result = GLOBUS_SUCCESS;

    if (StatInfo->file_only)
    {
        globus_gfs_stat_t gfs_stat;

        switch (StatInfo->use_symlink_info)
        {
        case 0:
            INFO(("stat() of %s\n", StatInfo->pathname));
            result = stat_object(StatInfo->pathname, &gfs_stat);
            result = fixup_stat_object(StatInfo->pathname, result, &gfs_stat);
            break;
        default:
            INFO(("lstat() of %s\n", StatInfo->pathname));
            result = stat_link(StatInfo->pathname, &gfs_stat);
            break;
        }

        globus_gridftp_server_finished_stat(Operation, result, &gfs_stat, 1);
        if (result == GLOBUS_SUCCESS)
            stat_destroy(&gfs_stat);
        return;
    }

    /*
     * Directory listing.
     */
    INFO(("List stat of %s\n", StatInfo->pathname));

    struct _stat_dir_cb_arg cb_arg = {Operation, StatInfo};
    result = stat_directory(StatInfo->pathname, _stat_dir_callback, &cb_arg);

    // Error path. Success path is handled in the callback to avoid some
    // timing issues.
    if (result != GLOBUS_SUCCESS)
        globus_gridftp_server_finished_stat(Operation, result, NULL, 0);
}

globus_gfs_storage_iface_t hpss_dsi_iface = {
    /* Descriptor       */
    GLOBUS_GFS_DSI_DESCRIPTOR_SENDER | GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING |
        GLOBUS_GFS_DSI_DESCRIPTOR_REQUIRES_ORDERED_DATA,

    dsi_init,    /* init_func        */
    dsi_destroy, /* destroy_func     */
    NULL,        /* list_func        */
    dsi_send,    /* send_func        */
    dsi_recv,    /* recv_func        */
    NULL,        /* trev_func        */
    NULL,        /* active_func      */
    NULL,        /* passive_func     */
    NULL,        /* data_destroy     */
    dsi_command, /* command_func     */
    dsi_stat,    /* stat_func        */
    NULL,        /* set_cred_func    */
    NULL,        /* buffer_send_func */
    NULL,        /* realpath_func    */
};

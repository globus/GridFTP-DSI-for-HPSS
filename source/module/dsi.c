/*
 * System includes
 */
#include <string.h>
#include <stdlib.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * Local includes
 */
#include "authenticate.h"
#include "commands.h"
#include "logging.h"
#include "config.h"
#include "fixups.h"
#include "stage.h"
#include "retr.h"
#include "stat.h"
#include "stor.h"
#include "hpss.h"
#include "cksm.h"

static void
set_logging_task_id(globus_gfs_operation_t Operation)
{
    char * task_id = NULL;
    globus_gridftp_server_get_task_id(Operation, &task_id);
    logging_set_taskid(task_id);
    if (task_id)
        free(task_id);
}

void
dsi_init(globus_gfs_operation_t     Operation,
         globus_gfs_session_info_t *SessionInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;
    config_t *      config = NULL;
    char *          home   = NULL;
    sec_cred_t      user_cred;

    logging_init();
    logging_set_user(SessionInfo->username);

    /*
     * Verify the HPSS version.
     *
     * HPSS_MAJOR_VERSION could be either double digits, ie 07 for pre HPSS 8.x or
     * single digit, ie 8, for HPSS 8.x and later.
     */
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
    // Runtime version string
    char * version_string = Hpss_BuildLevelString();
    char major[2] = {version_string[0]};
    char minor[2] = {version_string[2]};

    if (version_string == NULL ||
        strlen(version_string) < 3 ||
        atoi(major) != HPSS_MAJOR_VERSION ||
        atoi(minor) != HPSS_MINOR_VERSION)
    {
        ERROR("The HPSS connector was not built for this version of HPSS. "
              "Runtime version is %s. Buildtime version was %d.%d",
              version_string ? version_string : "NULL",
              HPSS_MAJOR_VERSION,
              HPSS_MINOR_VERSION);
        result  = HPSSWrongVersion();
        goto cleanup;
    }
    free(version_string);

    /*
     * Read in the config.
     */
    result = config_init(Operation, &config);
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
    int retval = Hpss_GetThreadUcred(&user_cred);
    if (retval)
    {
        result = hpss_error_to_globus_result(retval);
        goto cleanup;
    }

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
    set_logging_task_id(Operation);

    // Defering the INFO() call until inside of retr() since it has the
    // critical information.
    retr(Operation, TransferInfo);
}

static void
dsi_recv(globus_gfs_operation_t      Operation,
         globus_gfs_transfer_info_t *TransferInfo,
         void *                      UserArg)
{
    config_t * config = UserArg;

    set_logging_task_id(Operation);

    // Defering the INFO() call until inside of stor() since it has the
    // critical information.
    stor(Operation, TransferInfo, config->UDAChecksumSupport);
}

void
dsi_command(globus_gfs_operation_t     Operation,
            globus_gfs_command_info_t *CommandInfo,
            void *                     UserArg)
{
    globus_result_t result;
    config_t * config = UserArg;

    set_logging_task_id(Operation);

    commands_callback Callback = globus_gridftp_server_finished_command;

    switch (CommandInfo->command)
    {
    case GLOBUS_GFS_CMD_RMD:
        INFO("Removing directory %s", CommandInfo->pathname);
        result = commands_rmdir(CommandInfo->pathname);
        result = fixup_rmd(CommandInfo->pathname, result);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    case GLOBUS_GFS_CMD_MKD:
        INFO("Creating directory %s", CommandInfo->pathname);
        result = commands_mkdir(CommandInfo);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    case GLOBUS_GFS_CMD_DELE:
        INFO("Deleting %s", CommandInfo->pathname);
        result = commands_unlink(CommandInfo);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    case GLOBUS_GFS_CMD_RNTO:
        INFO("Renaming %s to %s",
             CommandInfo->from_pathname,
             CommandInfo->pathname);
        result = commands_rename(CommandInfo);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    case GLOBUS_GFS_CMD_RNFR:
        break;
    case GLOBUS_GFS_CMD_SITE_CHMOD:
        // TODO: CHMOD is not used by Transfer.
        INFO("Changing permissions on %s to %.3o",
             CommandInfo->pathname,
             CommandInfo->chmod_mode);
        result = commands_chmod(CommandInfo);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    case GLOBUS_GFS_CMD_SITE_UTIME:
        INFO("Setting access and modification times on %s to %ld",
             CommandInfo->pathname,
             CommandInfo->utime_time);
        result = commands_utime(CommandInfo);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    case GLOBUS_GFS_CMD_SITE_SYMLINKFROM:
        break;
    case GLOBUS_GFS_CMD_SITE_SYMLINK:
        INFO("Creating symlink %s to %s",
             CommandInfo->from_pathname,
             CommandInfo->pathname);
        result = commands_symlink(CommandInfo);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    case GLOBUS_GFS_CMD_CKSM:
        INFO("Get checksum of %s", CommandInfo->pathname);
        cksm(Operation, CommandInfo, config->UDAChecksumSupport, Callback);
        break;
    case GLOBUS_GFS_HPSS_CMD_SITE_STAGE:
        INFO("Staging %s", CommandInfo->pathname);
        stage(Operation, CommandInfo, Callback);
        break;
    case GLOBUS_GFS_CMD_TRNC:
        // TODO: I don't think Transfer uses this command
        INFO("Truncating %s", CommandInfo->pathname);
        result = commands_truncate(CommandInfo);
        globus_gridftp_server_finished_command(Operation, result, NULL);
        break;
    default:
        globus_gridftp_server_finished_command(
            Operation,
            GlobusGFSErrorGeneric("Not Supported"),
            NULL);
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

    set_logging_task_id(Operation);

    if (StatInfo->file_only)
    {
        INFO("Getting attributes of %s", StatInfo->pathname);

        globus_gfs_stat_t gfs_stat;

        switch (StatInfo->use_symlink_info)
        {
        case 0:
            result = stat_object(StatInfo->pathname, &gfs_stat);
            result = fixup_stat_object(StatInfo->pathname, result, &gfs_stat);
            break;
        default:
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
    INFO("Listing directory %s", StatInfo->pathname);

    struct _stat_dir_cb_arg cb_arg = {Operation, StatInfo};
    result = stat_directory(StatInfo->pathname, _stat_dir_callback, &cb_arg);

    // Error path. Success path is handled in the callback to avoid some
    // timing issues.
    if (result != GLOBUS_SUCCESS)
        globus_gridftp_server_finished_stat(Operation, result, NULL, 0);
}

void
_force_process_exit(void * arg)
{
    DEBUG("Forcing process exit");
    _exit(1);
}

void
dsi_trev(globus_gfs_event_info_t *           event_info,
         void *                              user_arg)
{
    switch (event_info->type)
    {
    case GLOBUS_GFS_EVENT_TRANSFER_ABORT:
        DEBUG("Abort received, forcing process exit in 10 seconds");
        globus_reltime_t timer;
        GlobusTimeReltimeSet(timer, 10, 0);
        globus_callback_register_oneshot(
            NULL,
            &timer,
            _force_process_exit,
            NULL);
        break;
    default:
        break;
    }
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
    dsi_trev,    /* trev_func        */
    NULL,        /* active_func      */
    NULL,        /* passive_func     */
    NULL,        /* data_destroy     */
    dsi_command, /* command_func     */
    dsi_stat,    /* stat_func        */
    NULL,        /* set_cred_func    */
    NULL,        /* buffer_send_func */
    NULL,        /* realpath_func    */
};

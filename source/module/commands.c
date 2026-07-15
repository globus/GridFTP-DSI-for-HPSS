/*
 * System includes
 */
#include <grp.h>
#include <stdlib.h>
#include <sys/types.h>


/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * Local includes
 */
#include "commands.h"
#include "logging.h"
#include "config.h"
#include "stage.h"
#include "cksm.h"
#include "hpss.h"

globus_result_t
commands_init(globus_gfs_operation_t Operation)
{
    globus_result_t result;

    result = globus_gridftp_server_add_command(Operation,
                                               "SITE STAGE",
                                               GLOBUS_GFS_HPSS_CMD_SITE_STAGE,
                                               4,
                                               4,
                                               "SITE STAGE <sp> timeout <sp> path",
                                               GLOBUS_TRUE,
                                               GFS_ACL_ACTION_READ);

    if (result != GLOBUS_SUCCESS)
        return GlobusGFSErrorWrapFailed(
            "Failed to add custom 'SITE STAGE' command", result);

#if (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
    //
    // New stage interface
    //

    // SITE STGBEGIN
    result = globus_gridftp_server_add_command(Operation,
                                               "SITE STGBEGIN",
                                               GLOBUS_GFS_HPSS_CMD_SITE_STGBEGIN,
                                               2,
                                               2,
                                               "SITE STGBEGIN",
                                               GLOBUS_FALSE, // has_pathname
                                               GFS_ACL_ACTION_READ);
    if (result != GLOBUS_SUCCESS)
        return GlobusGFSErrorWrapFailed(
            "Failed to add custom 'SITE STGBEGIN' command", result);

    // SITE STGFILE
    result = globus_gridftp_server_add_command(Operation,
                                               "SITE STGFILE",
                                               GLOBUS_GFS_HPSS_CMD_SITE_STGFILE,
                                               3,
                                               3,
                                               "SITE STGFILE <sp> path",
                                               GLOBUS_TRUE, // has_pathname
                                               GFS_ACL_ACTION_READ);
    if (result != GLOBUS_SUCCESS)
        return GlobusGFSErrorWrapFailed(
            "Failed to add custom 'SITE STGFILE' command", result);

    // SITE STGEND
    result = globus_gridftp_server_add_command(Operation,
                                               "SITE STGEND",
                                               GLOBUS_GFS_HPSS_CMD_SITE_STGEND,
                                               2,
                                               2,
                                               "SITE STGEND",
                                               GLOBUS_FALSE, // has_pathname
                                               GFS_ACL_ACTION_READ);
    if (result != GLOBUS_SUCCESS)
        return GlobusGFSErrorWrapFailed(
            "Failed to add custom 'SITE STGEND' command", result);

    // SITE STGCHK
    result = globus_gridftp_server_add_command(Operation,
                                               "SITE STGCHK",
                                               GLOBUS_GFS_HPSS_CMD_SITE_STGCHK,
                                               4,
                                               4,
                                               "SITE STGCHK <sp> request_id <sp> path",
                                               GLOBUS_TRUE, // has_pathname
                                               GFS_ACL_ACTION_READ);
    if (result != GLOBUS_SUCCESS)
        return GlobusGFSErrorWrapFailed(
            "Failed to add custom 'SITE STGCHK' command", result);

#endif // (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9

    return GLOBUS_SUCCESS;
}

globus_result_t
commands_chmod(globus_gfs_command_info_t *CommandInfo)
{
    int retval = Hpss_Chmod(CommandInfo->pathname, CommandInfo->chmod_mode);
    if (retval)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

globus_result_t
commands_mkdir(globus_gfs_command_info_t *CommandInfo)
{
    int retval = Hpss_Mkdir(CommandInfo->pathname,
                            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if (retval)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

globus_result_t
commands_rename(globus_gfs_command_info_t *CommandInfo)
{
    int retval = Hpss_Rename(CommandInfo->from_pathname, CommandInfo->pathname);
    if (retval)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

globus_result_t
commands_rmdir(char * Pathname)
{
    int retval = Hpss_Rmdir(Pathname);
    if (retval)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

globus_result_t
commands_symlink(globus_gfs_command_info_t *CommandInfo)
{
    int retval = Hpss_Symlink(CommandInfo->from_pathname,
                              CommandInfo->pathname);
    if (retval)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

globus_result_t
commands_truncate(globus_gfs_command_info_t *CommandInfo)
{
    int retval =
        Hpss_Truncate(CommandInfo->from_pathname, CommandInfo->cksm_offset);
    if (retval)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

globus_result_t
commands_utime(globus_gfs_command_info_t *CommandInfo)
{
    struct utimbuf times;
    times.actime  = CommandInfo->utime_time;
    times.modtime = CommandInfo->utime_time;

    int retval = Hpss_Utime(CommandInfo->pathname, &times);
    if (retval)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

globus_result_t
commands_unlink(globus_gfs_command_info_t *CommandInfo)
{
    int retval = Hpss_Unlink(CommandInfo->pathname);
    if (retval != HPSS_E_NOERROR)
        return hpss_error_to_globus_result(retval);
    return GLOBUS_SUCCESS;
}

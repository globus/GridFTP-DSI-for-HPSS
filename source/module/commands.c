/*
 * System includes
 */
#include <grp.h>
#include <stdlib.h>
#include <sys/types.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * Local includes
 */
#include "cksm.h"
#include "hpss.h"
#include "commands.h"
#include "logging.h"
#include "config.h"
#include "stage.h"

globus_result_t
commands_init(globus_gfs_operation_t Operation)
{
    globus_result_t result =
        globus_gridftp_server_add_command(Operation,
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

    return GLOBUS_SUCCESS;
}

globus_result_t
commands_mkdir(globus_gfs_command_info_t *CommandInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;

    int retval = Hpss_Mkdir(CommandInfo->pathname,
                            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Mkdir", -retval);
    return result;
}

globus_result_t
commands_rmdir(char * Pathname)
{
    globus_result_t result = GLOBUS_SUCCESS;

    int retval = Hpss_Rmdir(Pathname);
    if (retval != HPSS_E_NOERROR)
        result = GlobusGFSErrorSystemError("hpss_Rmdir", -retval);
    return result;
}

globus_result_t
commands_unlink(globus_gfs_command_info_t *CommandInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;

    int retval = Hpss_Unlink(CommandInfo->pathname);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Unlink", -retval);
    return result;
}

globus_result_t
commands_rename(globus_gfs_command_info_t *CommandInfo)
{
    int             retval = 0;
    globus_result_t result = GLOBUS_SUCCESS;

    INFO("rename %s to %s\n",
           CommandInfo->from_pathname,
           CommandInfo->pathname);

    retval = hpss_Rename(CommandInfo->from_pathname, CommandInfo->pathname);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Rename", -retval);
    return result;
}

globus_result_t
commands_chmod(globus_gfs_command_info_t *CommandInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;

    int retval = Hpss_Chmod(CommandInfo->pathname, CommandInfo->chmod_mode);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Chmod", -retval);
    return result;
}

globus_result_t
session_get_gid(char *GroupName, int *Gid)
{
    struct group *group = NULL;
    struct group  group_buf;
    char          buffer[1024];
    int           retval = 0;

    /* Find the passwd entry. */
    retval = getgrnam_r(GroupName, &group_buf, buffer, sizeof(buffer), &group);
    if (retval != 0)
        return GlobusGFSErrorSystemError("getgrnam_r", errno);

    if (group == NULL)
        return GlobusGFSErrorGeneric("Group not found");

    /* Copy out the gid */
    *Gid = group->gr_gid;

    return GLOBUS_SUCCESS;
}

globus_result_t
commands_chgrp(globus_gfs_command_info_t *CommandInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;
    int             gid;

    hpss_stat_t hpss_stat_buf;
    int         retval = Hpss_Stat(CommandInfo->pathname, &hpss_stat_buf);
    if (retval)
        return GlobusGFSErrorSystemError("hpss_Stat", -retval);

    if (!isdigit(*CommandInfo->chgrp_group))
    {
        result = session_get_gid(CommandInfo->chgrp_group, &gid);
        if (result != GLOBUS_SUCCESS)
            return result;
    } else
    {
        gid = atoi(CommandInfo->chgrp_group);
    }

    retval = Hpss_Chown(CommandInfo->pathname, hpss_stat_buf.st_uid, gid);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Chgrp", -retval);
    return result;
}

globus_result_t
commands_utime(globus_gfs_command_info_t *CommandInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;

    struct utimbuf times;
    times.actime  = CommandInfo->utime_time;
    times.modtime = CommandInfo->utime_time;

    int retval = Hpss_Utime(CommandInfo->pathname, &times);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Utime", -retval);
    return result;
}

globus_result_t
commands_symlink(globus_gfs_command_info_t *CommandInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;

    int retval =
        Hpss_Symlink(CommandInfo->from_pathname, CommandInfo->pathname);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Symlink", -retval);
    return result;
}

globus_result_t
commands_truncate(globus_gfs_command_info_t *CommandInfo)
{
    globus_result_t result = GLOBUS_SUCCESS;

    int retval =
        Hpss_Truncate(CommandInfo->from_pathname, CommandInfo->cksm_offset);
    if (retval)
        result = GlobusGFSErrorSystemError("hpss_Truncate", -retval);
    return result;
}

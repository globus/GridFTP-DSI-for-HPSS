#ifndef HPSS_DSI_COMMANDS_H
#define HPSS_DSI_COMMANDS_H

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * Local includes
 */
#include "hpss.h"

enum
{
    GLOBUS_GFS_HPSS_CMD_SITE_STAGE = GLOBUS_GFS_MIN_CUSTOM_CMD,
#if (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
    GLOBUS_GFS_HPSS_CMD_SITE_STGBEGIN,
    GLOBUS_GFS_HPSS_CMD_SITE_STGEND,
#endif // (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
};

globus_result_t
commands_init(globus_gfs_operation_t Operation);

typedef void (*commands_callback)(globus_gfs_operation_t Operation,
                                  globus_result_t        Result,
                                  char *                 CommandResponse);

globus_result_t
commands_chmod(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_mkdir(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_rename(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_rmdir(char * Pathname);

globus_result_t
commands_symlink(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_truncate(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_unlink(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_utime(globus_gfs_command_info_t *CommandInfo);

#endif /* HPSS_DSI_COMMANDS_H */

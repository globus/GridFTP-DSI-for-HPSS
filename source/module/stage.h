#ifndef HPSS_DSI_STAGE_H
#define HPSS_DSI_STAGE_H

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * Local includes
 */
#include "commands.h"

void
stage(globus_gfs_operation_t     Operation,
      globus_gfs_command_info_t *CommandInfo,
      commands_callback          Callback);

#endif /* HPSS_DSI_STAGE_H */

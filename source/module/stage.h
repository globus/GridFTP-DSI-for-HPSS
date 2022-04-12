#ifndef HPSS_DSI_STAGE_H
#define HPSS_DSI_STAGE_H

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * Local includes
 */
#include "hpss.h"
#include "commands.h"

typedef enum
{
    RESIDENCY_ARCHIVED,
    RESIDENCY_RESIDENT,
    RESIDENCY_TAPE_ONLY,
} residency_t;

// DSI entry point
void
stage(globus_gfs_operation_t      Operation,
      globus_gfs_command_info_t * CommandInfo,
      commands_callback           Callback);

// Utils entry point
globus_result_t
stage_ex(
    const char   * Path,
    int            Timeout,
    const char   * TaskID,
    hpss_reqid_t * RequestID,
    residency_t  * Residency);

#endif /* HPSS_DSI_STAGE_H */

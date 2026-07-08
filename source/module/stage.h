#ifndef HPSS_DSI_STAGE_H
#define HPSS_DSI_STAGE_H

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

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

//
// Legacy stage interface
//

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


#if (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
//
// New stage interface
//

/*
 * Maximum number of files to stage in a single batch.
 */
#define BATCH_STAGE_MAX_FILES 100

/*
 * Batch stage structure. Keeps state between successive staging calls.
 */
typedef struct batch_stage batch_stage_t;

// BatchStage should be pointer-to-NULL on first call
void
stgbegin(
    globus_gfs_operation_t         Operation,    // IN
    globus_gfs_command_info_t   *  CommandInfo,  // IN
    batch_stage_t               ** BatchStage,   // IN/OUT
    commands_callback              Callback);    // IN

void
stgfile(
    globus_gfs_operation_t         Operation,   // IN
    globus_gfs_command_info_t   *  CommandInfo, // IN
    batch_stage_t               *  BatchStage,  // IN/OUT
    commands_callback              Callback);   // IN

void
stgend(
    globus_gfs_operation_t         Operation,    // IN
    globus_gfs_command_info_t   *  CommandInfo,  // IN
    batch_stage_t               ** BatchStage,   // IN/OUT
    commands_callback              Callback);    // IN

#endif  //(HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9

#endif /* HPSS_DSI_STAGE_H */

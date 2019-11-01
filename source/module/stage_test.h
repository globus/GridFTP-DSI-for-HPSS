#ifndef HPSS_DSI_STAGE_TEST_H
#define HPSS_DSI_STAGE_TEST_H

/*
 * HPSS includes
 */
#include <hpss_api.h>
#include <hpss_version.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * TESTING INTERFACE.
 */
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) ||                     \
    HPSS_MAJOR_VERSION >= 8
#define bitfile_id_t bfs_bitfile_obj_handle_t
#define ATTR_TO_BFID(x) (x.Attrs.BitfileObj.BfId)
#else
#define bitfile_id_t hpssoid_t
#define ATTR_TO_BFID(x) (x.Attrs.BitfileId)
#endif

#ifdef TESTING
#define STATIC
#else
#define STATIC static
#endif /* TESTING */

typedef enum
{
    ARCHIVED,
    RESIDENT,
    TAPE_ONLY,
} residency_t;

STATIC globus_result_t
       check_request_status(bitfile_id_t *BitfileID, int *Status);

STATIC globus_result_t
       submit_stage_request(const char *Pathname);

STATIC residency_t
       check_xattr_residency(hpss_xfileattr_t *XFileAttr);

STATIC void
free_xfileattr(hpss_xfileattr_t *XFileAttr);

STATIC globus_result_t
       check_file_residency(const char *Pathname, residency_t *Residency);

STATIC globus_result_t
       stage_get_timeout(globus_gfs_operation_t     Operation,
                         globus_gfs_command_info_t *CommandInfo,
                         int *                      Timeout);

STATIC globus_result_t
       get_bitfile_id(const char *Pathname, bitfile_id_t *bitfile_id);

STATIC int
pause_1_second(time_t StartTime, int Timeout);

STATIC globus_result_t
       stage_internal(const char *Path, int Timeout, residency_t *Residency);

#endif /* HPSS_DSI_STAGE_TEST_H */

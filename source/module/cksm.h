#ifndef HPSS_DSI_CKSM_H
#define HPSS_DSI_CKSM_H

/*
 * System includes
 */
#include <openssl/md5.h>
#include <stdbool.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_xml.h>

/*
 * Local includes
 */
#include "commands.h"

typedef struct
{
    pthread_mutex_t          Lock;
    globus_off_t             TotalBytes;
    globus_gfs_operation_t   Operation;
    globus_callback_handle_t CallbackHandle;
} cksm_marker_t;

typedef struct
{
    globus_gfs_operation_t     Operation;
    globus_gfs_command_info_t *CommandInfo;
    bool                       UseUDAChecksums;
    char *                     Pathname;
    commands_callback          Callback;
    MD5_CTX                    MD5Context;
    globus_result_t            Result;
    int                        FileFD;
    globus_size_t              BlockSize;
    globus_off_t               RangeLength;
    cksm_marker_t *            Marker;
} cksm_info_t;

void
cksm(globus_gfs_operation_t     Operation,
     globus_gfs_command_info_t *CommandInfo,
     bool                       UseUDAChecksums,
     commands_callback          Callback);

globus_result_t
cksm_set_uda_checksum(char *Pathname, char *Checksum);

globus_result_t
cksm_get_uda_checksum(char *  Pathname, char ** ChecksumString);

globus_result_t
cksm_clear_uda_checksum(char *Pathname);

#endif /* HPSS_DSI_CKSM_H */

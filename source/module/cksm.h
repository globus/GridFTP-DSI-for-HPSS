#ifndef HPSS_DSI_CKSM_H
#define HPSS_DSI_CKSM_H

/*
 * System includes
 */
#include <openssl/md5.h>

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
#include "config.h"

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
    config_t *                 Config;
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
     config_t *                 Config,
     commands_callback          Callback);

globus_result_t
cksm_set_checksum(char *Pathname, config_t *Config, char *Checksum);

globus_result_t
checksum_get_file_sum(char *Pathname, config_t *Config, char **ChecksumString);

globus_result_t
cksm_clear_checksum(char *Pathname, config_t *Config);

#endif /* HPSS_DSI_CKSM_H */

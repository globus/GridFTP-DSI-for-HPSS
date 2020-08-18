#ifndef HPSS_DSI_RETR_H
#define HPSS_DSI_RETR_H

/*
 * System includes
 */
#include <pthread.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>
#include <globus_list.h>

/*
 * Local includes
 */
#include "pio.h"

struct retr_info;

typedef struct
{
    char *            Buffer;
    struct retr_info *RetrInfo;
#define VALID_TAG 0xDEADBEEF
#define INVALID_TAG 0x00000000
    int Valid; // Debug Entry
} retr_buffer_t;

typedef struct retr_info
{
    globus_gfs_operation_t      Operation;
    globus_gfs_transfer_info_t *TransferInfo;

    int      FileFD;
    uint64_t FileSize;

    globus_result_t Result;
    globus_size_t   BlockSize;
    globus_off_t    RangeLength;
    globus_off_t    CurrentOffset;

    pthread_mutex_t Mutex;
    pthread_cond_t  Cond;

    int OptConnCnt;
    int ConnChkCnt;

    globus_list_t *AllBufferList;
    globus_list_t *FreeBufferList;

} retr_info_t;

void
retr(globus_gfs_operation_t      Operation,
     globus_gfs_transfer_info_t *TransferInfo);

#endif /* HPSS_DSI_RETR_H */

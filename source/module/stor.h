#ifndef HPSS_DSI_STOR_H
#define HPSS_DSI_STOR_H

/*
 * System includes
 */
#include <pthread.h>
#include <stdbool.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>
#include <globus_list.h>

/*
 * Local includes
 */
#include "pio.h"

/*
 * Because of the sequential, ascending nature of offsets with PIO,
 * we do not need a range list.
 */
struct stor_info;

typedef struct
{
    char *            Buffer;
    globus_off_t      BufferOffset;   // Moves as buffer is consumed
    globus_off_t      TransferOffset; // Moves as BufferOffset moves
    globus_off_t      BufferLength;   // Moves as BufferOffset moves
    struct stor_info *StorInfo;
#define VALID_TAG 0xDEADBEEF
#define INVALID_TAG 0x00000000
    int Valid; // Debug Entry
} stor_buffer_t;

typedef struct stor_info
{
    globus_gfs_operation_t      Operation;
    globus_gfs_transfer_info_t *TransferInfo;

    int FileFD;

    globus_result_t Result;
    globus_size_t   BlockSize;

    pthread_mutex_t Mutex;
    pthread_cond_t  Cond;

    globus_off_t  RangeLength; // Current range transfer length
    globus_bool_t Eof;

    int OptConnCnt;
    int ConnChkCnt;
    int CurConnCnt;

    globus_list_t *AllBufferList;
    globus_list_t *ReadyBufferList;
    globus_list_t *FreeBufferList;

} stor_info_t;

void
stor(globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo,
     bool                         UseUDAChecksums);

#endif /* HPSS_DSI_STOR_H */

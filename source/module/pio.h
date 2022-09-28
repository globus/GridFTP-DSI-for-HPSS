#ifndef HPSS_DSI_PIO_H
#define HPSS_DSI_PIO_H

/*
 * System includes
 */
#include <pthread.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * Local includes
 */
#include "hpss.h"

#define PIO_END_TRANSFER 0xDEADBEEF

typedef int (*pio_data_callout)(char *    Buffer,
                                uint32_t *Length, /* IN / OUT */
                                uint64_t  Offset,
                                void *    CallbackArg);

/*
 * Called as each range is completed. Either set Eot to 0 to signal
 * the transfer is complete or set Eot to 1 and set Offset and Length
 * to the new range to transfer.
 */
typedef void (*pio_range_complete_callback)(globus_off_t *Offset,
                                            globus_off_t *Length,
                                            int *         Eot,
                                            void *        UserArg);

typedef void (*pio_transfer_complete_callback)(globus_result_t Result,
                                               void *          UserArg);

// typedef enum {
//	PIO_OP_RETR,
//	PIO_OP_STOR,
//	PIO_OP_CKSM,
//} pio_op_type_t;

typedef struct
{
    int      FD;
    char *   Buffer;
    uint32_t BlockSize;
    uint64_t InitialOffset;
    uint64_t InitialLength;

    pio_data_callout               DataCO;
    pio_range_complete_callback    RngCmpltCB;
    pio_transfer_complete_callback XferCmpltCB;
    void *                         UserArg;

    globus_result_t CoordinatorResult;
    hpss_pio_grp_t  CoordinatorSG;
    hpss_pio_grp_t  ParticipantSG;
} pio_t;

/* Don't call for zero-length transfers. */
globus_result_t
pio_start(hpss_pio_operation_t           PioOpType,
          int                            FD,
          int                            FileStripeWidth,
          uint32_t                       BlockSize,
          globus_off_t                   Offset,
          globus_off_t                   Length,
          pio_data_callout               Callout,
          pio_range_complete_callback    RngCmpltCB,
          pio_transfer_complete_callback XferCmpltCB,
          void *                         UserArg);

globus_result_t
pio_end();

#endif /* HPSS_DSI_PIO_H */

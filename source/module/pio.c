/*
 * System includes
 */
#include <pthread.h>

/*
 * Local includes
 */
#include "logging.h"
#include "hpss.h"
#include "pio.h"

globus_result_t
pio_launch_detached(void *(*ThreadEntry)(void *Arg), void *Arg)
{
    int             rc      = 0;
    int             initted = 0;
    pthread_t       thread;
    pthread_attr_t  attr;
    globus_result_t result = GLOBUS_SUCCESS;

    /*
     * Launch a detached thread.
     */
    if ((rc = pthread_attr_init(&attr)) || !(initted = 1) ||
        (rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) ||
        (rc = pthread_create(&thread, &attr, ThreadEntry, Arg)))
    {
        result = GlobusGFSErrorSystemError("Launching get object thread", rc);
    }
    if (initted)
        pthread_attr_destroy(&attr);
    return result;
}

globus_result_t
pio_launch_attached(void *(*ThreadEntry)(void *Arg),
                    void *     Arg,
                    pthread_t *ThreadID)
{
    int rc = 0;

    /*
     * Launch a detached thread.
     */
    rc = pthread_create(ThreadID, NULL, ThreadEntry, Arg);
    if (rc)
        return GlobusGFSErrorSystemError("Launching get object thread", rc);
    return GLOBUS_SUCCESS;
}

void *
pio_coordinator_thread(void *Arg)
{
    int                rc          = 0;
    int                eot         = 0;
    pio_t *            pio         = Arg;
    globus_off_t       length      = pio->InitialLength;
    globus_off_t       offset      = pio->InitialOffset;
    uint64_t           bytes_moved = 0;
    hpss_pio_gapinfo_t gap_info;

// XXX we only support a single range except when we encounter a gap
// during RETR. So this can all be simplified.
    do
    {
        memset(&gap_info, 0, sizeof(gap_info));
#define NoValue64 0xDEADBEEF
        bytes_moved = NoValue64;

        /* Call pio execute. */
        rc = Hpss_PIOExecute(pio->FD,
                             offset,
                             length,
                             pio->CoordinatorSG,
                             &gap_info,
                             &bytes_moved);

        switch (bytes_moved)
        {
        case NoValue64:
            length = 0;
            WARN("Your HPSS installation does not support BZ4719 "
                  "and so restart markers are not supported on error "
                  "conditions.");
            break;

        default:
            length = bytes_moved + gap_info.Length;
            break;
        }

        if (gap_info.Length > 0)
            DEBUG("Gap in file. Offset:%llu Length:%llu",
                  gap_info.Offset, 
                  gap_info.Length);

        if (rc != 0)
            pio->CoordinatorResult = hpss_error_to_globus_result(rc);

        do
        {
            pio->RngCmpltCB(&offset, &length, &eot, pio->UserArg);
        } while (length == 0 && !eot && !rc);
    } while (!rc && !eot);

    rc = Hpss_PIOEnd(pio->CoordinatorSG);
    if (!pio->CoordinatorResult && rc != HPSS_E_NOERROR && hpss_error_status(rc) != PIO_END_TRANSFER)
        pio->CoordinatorResult = hpss_error_to_globus_result(rc);

    return NULL;
}

int
pio_register_callback(void *    UserArg,
                      uint64_t  Offset,
                      uint32_t *Length,
                      void **   Buffer)
{
    pio_t *pio = UserArg;
    /*
     * On STOR, this buffer comes up NULL the first time. On RETR,
     * it is not NULL but it isn't safe to exchange either.
     */
    if (!*Buffer)
        *Buffer = pio->Buffer;
    return pio->DataCO(*Buffer, Length, Offset, pio->UserArg);
}

void *
pio_thread(void *Arg)
{
    int             rc              = 0;
    pio_t *         pio             = Arg;
    globus_result_t result          = GLOBUS_SUCCESS;
    pthread_t       thread_id;
    char *          buffer = NULL;

    buffer = malloc(pio->BlockSize);
    if (!buffer)
    {
        result = GlobusGFSErrorMemory("pio buffer");
        goto cleanup;
    }

    /*
     * Save buffer into pio; the write callback shows up without
     * a buffer right after hpss_PIOExecute().
     */
    pio->Buffer = buffer;

    result = pio_launch_attached(pio_coordinator_thread, pio, &thread_id);
    if (result)
        goto cleanup;

    rc = Hpss_PIORegister(0,
                          NULL, /* DataNetSockAddr */
                          buffer,
                          pio->BlockSize,
                          pio->ParticipantSG,
                          pio_register_callback,
                          pio);

    if (rc != HPSS_E_NOERROR && hpss_error_status(rc) != PIO_END_TRANSFER)
        result = hpss_error_to_globus_result(rc);

    rc = Hpss_PIOEnd(pio->ParticipantSG);
    if (rc != HPSS_E_NOERROR && hpss_error_status(rc) != PIO_END_TRANSFER)
    {
        if (result == GLOBUS_SUCCESS)
            result = hpss_error_to_globus_result(rc);
    }

    pthread_join(thread_id, NULL);
cleanup:
    if (buffer)
        free(buffer);

    if (!result)
        result = pio->CoordinatorResult;

    pio->XferCmpltCB(result, pio->UserArg);
    free(pio);

    return NULL;
}

globus_result_t
pio_start(hpss_pio_operation_t           PioOpType,
          int                            FD,
          int                            FileStripeWidth,
          uint32_t                       BlockSize,
          globus_off_t                   Offset,
          globus_off_t                   Length,
          pio_data_callout               DataCO,
          pio_range_complete_callback    RngCmpltCB,
          pio_transfer_complete_callback XferCmpltCB,
          void *                         UserArg)
{
    globus_result_t   result = GLOBUS_SUCCESS;
    pio_t *           pio    = NULL;
    hpss_pio_params_t pio_params;
    void *            group_buffer  = NULL;
    unsigned int      buffer_length = 0;
    int               eot           = 0;

    /* No zero length transfers. */
    while (Length == 0)
    {
        RngCmpltCB(&Offset, &Length, &eot, UserArg);
        if (eot)
        {
            XferCmpltCB(GLOBUS_SUCCESS, UserArg);
            return GLOBUS_SUCCESS;
        }
    }

    /*
     * Allocate our structure.
     */
    pio = malloc(sizeof(pio_t));
    if (!pio)
    {
        result = GlobusGFSErrorMemory("pio_t");
        goto cleanup;
    }
    memset(pio, 0, sizeof(pio_t));
    pio->FD            = FD;
    pio->BlockSize     = BlockSize;
    pio->InitialOffset = Offset;
    pio->InitialLength = Length;
    pio->DataCO        = DataCO;
    pio->RngCmpltCB    = RngCmpltCB;
    pio->XferCmpltCB   = XferCmpltCB;
    pio->UserArg       = UserArg;

    /*
     * Don't use HPSS_PIO_HANDLE_GAP, it's bugged in HPSS 7.4.
     */
    pio_params.Operation       = PioOpType;
    pio_params.ClntStripeWidth = 1;
    pio_params.BlockSize       = BlockSize;
    pio_params.FileStripeWidth = FileStripeWidth;
    pio_params.IOTimeOutSecs   = 0;
    pio_params.Transport       = HPSS_PIO_TCPIP;
    pio_params.Options         = 0;

    int retval = Hpss_PIOStart(&pio_params, &pio->CoordinatorSG);
    if (retval != 0)
    {
        result = hpss_error_to_globus_result(retval);
        goto cleanup;
    }

    retval =
        Hpss_PIOExportGrp(pio->CoordinatorSG, &group_buffer, &buffer_length);
    if (retval != 0)
    {
        result = hpss_error_to_globus_result(retval);
        goto cleanup;
    }

    retval =
        Hpss_PIOImportGrp(group_buffer, buffer_length, &pio->ParticipantSG);
    if (retval != 0)
    {
        result = hpss_error_to_globus_result(retval);
        goto cleanup;
    }

    result = pio_launch_detached(pio_thread, pio);

    if (!result)
        return result;

cleanup:
    /* Can not clean up the stripe groups without crashing. */
    if (pio)
        free(pio);
    return result;
}

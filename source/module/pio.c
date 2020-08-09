/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2015 NCSA.  All rights reserved.
 *
 * Developed by:
 *
 * Storage Enabling Technologies (SET)
 *
 * Nation Center for Supercomputing Applications (NCSA)
 *
 * http://www.ncsa.illinois.edu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the .Software.),
 * to deal with the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 *    + Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimers.
 *
 *    + Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimers in the
 *      documentation and/or other materials provided with the distribution.
 *
 *    + Neither the names of SET, NCSA
 *      nor the names of its contributors may be used to endorse or promote
 *      products derived from this Software without specific prior written
 *      permission.
 *
 * THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS WITH THE SOFTWARE.
 */

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
            WARN(("Your HPSS installation does not support BZ4719 "
                  "and so restart markers are not supported on error "
                  "conditions.\n"));
            break;

        default:
            length = bytes_moved + gap_info.Length;
            break;
        }

        if (gap_info.Length > 0)
            INFO(("Gap in file. Offset:%llu Length:%llu\n",
                   gap_info.Offset, 
                   gap_info.Length));

        if (rc != 0)
            pio->CoordinatorResult =
                GlobusGFSErrorSystemError("hpss_PIOExecute", -rc);

        do
        {
            pio->RngCmpltCB(&offset, &length, &eot, pio->UserArg);
        } while (length == 0 && !eot && !rc);
    } while (!rc && !eot);

    rc = Hpss_PIOEnd(pio->CoordinatorSG);
    /*
     * The returned value from hpss_PIOEnd() is not very useful. It doesn't
     * have anything to do with the data we already transfered. If there is
     * an error from hpss_PIOEnd(), it _is_ because we passed a bad arg which
     * means a programming error and the transfer is likely to hang. All we 
     * can really do is log a warning and hope the admin sees it.
     */
    if (rc != HPSS_E_NOERROR)
        WARN(("hpss_PIOEnd() returned %d\n", rc));

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
    int             coord_launched  = 0;
    int             safe_to_end_pio = 0;
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
    coord_launched = 1;

    rc = Hpss_PIORegister(0,
                          NULL, /* DataNetSockAddr */
                          buffer,
                          pio->BlockSize,
                          pio->ParticipantSG,
                          pio_register_callback,
                          pio);
    if (rc != 0 && rc != PIO_END_TRANSFER)
        result = GlobusGFSErrorSystemError("hpss_PIORegister", -rc);
    safe_to_end_pio = 1;

cleanup:
    if (safe_to_end_pio)
    {
        rc = Hpss_PIOEnd(pio->ParticipantSG);
        if (rc != 0 && rc != PIO_END_TRANSFER && !result)
            result = GlobusGFSErrorSystemError("hpss_PIOEnd", -rc);
    }

    if (coord_launched)
        pthread_join(thread_id, NULL);
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
        result = GlobusGFSErrorSystemError("hpss_PIOStart", -retval);
        goto cleanup;
    }

    retval =
        Hpss_PIOExportGrp(pio->CoordinatorSG, &group_buffer, &buffer_length);
    if (retval != 0)
    {
        result = GlobusGFSErrorSystemError("hpss_PIOExportGrp", -retval);
        goto cleanup;
    }

    retval =
        Hpss_PIOImportGrp(group_buffer, buffer_length, &pio->ParticipantSG);
    if (retval != 0)
    {
        result = GlobusGFSErrorSystemError("hpss_PIOImportGrp", -retval);
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

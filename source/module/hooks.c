/*
 * System includes.
 */
#include <sys/types.h>
#include <pwd.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "hpss.h"
#include "test.h"

int
__real_hpss_PIOExecute(int                  Fd,
                       u_signed64           FileOffset,
                       u_signed64           Size,
                       hpss_pio_grp_t       StripeGroup,
                       hpss_pio_gapinfo_t * GapInfo,
                       u_signed64         * BytesMoved);

int
__wrap_hpss_PIOExecute(int                  Fd,
                       u_signed64           FileOffset,
                       u_signed64           Size,
                       hpss_pio_grp_t       StripeGroup,
                       hpss_pio_gapinfo_t * GapInfo,
                       u_signed64         * BytesMoved)
{
    u_signed64 bytes_moved_in = *BytesMoved;

    TEST_EVENT_PIO_RANGE_BEGIN(FileOffset, Size);
    int retval =  __real_hpss_PIOExecute(Fd,
                                         FileOffset,
                                         Size,
                                         StripeGroup,
                                         GapInfo,
                                         BytesMoved);
    TEST_EVENT_PIO_RANGE_COMPLETE(FileOffset,
                                  Size,
                                  bytes_moved_in,
                                  BytesMoved,
                                  retval);
    return retval;
}

void
__real_globus_gridftp_server_begin_transfer(globus_gfs_operation_t Op,
                                            int                    EventMask,
                                            void *                 EventArg);

void
__wrap_globus_gridftp_server_begin_transfer(globus_gfs_operation_t Op,
                                            int                    EventMask,
                                            void *                 EventArg)
{
    TEST_EVENT_TRANSFER_BEGIN();
    __real_globus_gridftp_server_begin_transfer(Op, EventMask, EventArg);
}

void
__real_globus_gridftp_server_finished_transfer(globus_gfs_operation_t Op,
                                               globus_result_t        Result);

void
__wrap_globus_gridftp_server_finished_transfer(globus_gfs_operation_t Op,
                                               globus_result_t        Result)
{
    TEST_EVENT_TRANSFER_FINISHED();
    __real_globus_gridftp_server_finished_transfer(Op, Result);
}

int
__real_getpwnam_r(const char *name,
                  struct passwd *pwd,
                  char *buf,
                  size_t buflen,
                  struct passwd **result);

int
__wrap_getpwnam_r(const char *name,
                  struct passwd *pwd,
                  char *buf,
                  size_t buflen,
                  struct passwd **result)
{
    int retval = __real_getpwnam_r(name, pwd, buf, buflen, result);
    TEST_EVENT_GETPWNAM(result);
    return retval;
}

int
__real_hpss_LoadDefaultThreadState(
    uid_t                   UserID,
    mode_t                  Umask,
    const char              *ClientFullName);

int
__wrap_hpss_LoadDefaultThreadState(
    uid_t                   UserID,
    mode_t                  Umask,
    const char              *ClientFullName)
{
    int retval = __real_hpss_LoadDefaultThreadState(UserID,
                                                    Umask,
                                                    ClientFullName);
    TEST_EVENT_LOAD_DEFAULT_THREAD_STATE(retval);
    return retval;
}

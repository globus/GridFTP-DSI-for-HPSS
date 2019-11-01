#include <hpss_api.h>
#include <mocking.h>

int
hpss_PIOExecute(
   int                Fd,
   u_signed64         FileOffset,
   u_signed64         Size,
   hpss_pio_grp_t     StripeGroup,
   hpss_pio_gapinfo_t *GapInfo,
   u_signed64         *BytesMoved)
{
    CHECK_PARAMS("FileOffset", UINT64, FileOffset, "Size", UINT64, Size);

    return GET_RETURN(INT, 0,
                      "GapInfo",    MEMORY, GapInfo,    sizeof(*GapInfo), 
                      "BytesMoved", MEMORY, BytesMoved, sizeof(*BytesMoved));
}

int
hpss_PIOEnd(hpss_pio_grp_t StripeGroup)
{
    return GET_RETURN(INT, 0);
}

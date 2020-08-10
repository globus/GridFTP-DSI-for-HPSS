#include "logging.h"

GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);

void
logging_init()
{
    GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS, INFO TRACE DEBUG WARN ERROR);
}

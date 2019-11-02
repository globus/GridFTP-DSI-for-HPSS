#include "logging.h"

#define INFO(message) \
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, TYPE_INFO, message)

#define TRACE(message) \
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, TYPE_TRACE, message)

#define DEBUG(message) \
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, TYPE_DEBUG, message)

#define WARN(message) \
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, TYPE_WARN, message)

#define ERROR(message) \
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, TYPE_ERROR, message)

GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);

void
logging_init()
{
    GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS, INFO TRACE DEBUG WARN ERROR);
}

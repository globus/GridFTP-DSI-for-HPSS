#ifndef _LOGGING_H_
#define _LOGGING_H_

#include <globus_common.h>

#define TYPE_INFO  1<<0
#define TYPE_TRACE 1<<1
#define TYPE_DEBUG 1<<2
#define TYPE_WARN  1<<3
#define TYPE_ERROR 1<<4

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

GlobusDebugDeclare(GLOBUS_GRIDFTP_SERVER_HPSS);

void
logging_init();

#endif /* _LOGGING_H_ */

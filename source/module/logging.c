#define _GNU_SOURCE /* See feature_test_macros(7) */
#include <stdio.h>
#include <globus_gridftp_server.h>
#include "logging.h"

GlobusDebugDeclare(GLOBUS_GRIDFTP_SERVER_HPSS);
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);

void
logging_init()
{
    /*
     * The GlobusDebug*() functions expect a numeric 'level'. For simplicity,
     * we are re using log_type_t. So make sure the levels listed here are
     * in the same order as listed in the log_type_t enum.
     */
    GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS, ERROR WARN INFO DEBUG TRACE);
}

void
log_message(log_type_t type, const char * format, ...)
{
    va_list ap;
    va_start (ap, format);

    char * message = NULL;
    const char * static_errmsg = "COULD NOT ALLOCATE MEMORY FOR LOG MESSAGE";
    int rc = vasprintf(&message, format, ap);
    if (rc == -1)
    {
        type = LOG_TYPE_ERROR;
        message = static_errmsg;
    }

    switch(type)
    {
    case LOG_TYPE_ERROR:
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s\n", message);
        break;
    case LOG_TYPE_WARN:
        globus_gfs_log_message(GLOBUS_GFS_LOG_WARN, "%s\n", message);
        break;
    case LOG_TYPE_INFO:
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s\n", message);
        break;
    case LOG_TYPE_DEBUG:
    case LOG_TYPE_TRACE:
        GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, type, (message));
        break;
    }

    if (message != static_errmsg)
        free(message);
    va_end(ap);
}

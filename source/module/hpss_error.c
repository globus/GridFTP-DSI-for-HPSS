/*
 * System includes
 */
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_String.h>
#include <hpss_errno.h>
#include <hpss_error.h>

#define MAX_HPSS_ERRORS 64
static hpss_error_t ErrorTable[MAX_HPSS_ERRORS];
static int ErrorTableIndex;
static pthread_mutex_t ErrorTableMutex = PTHREAD_MUTEX_INITIALIZER;

static pthread_once_t ErrorTableInitialized = PTHREAD_ONCE_INIT;

static void
error_table_init()
{
    // Can't use index 0
    ErrorTableIndex = 1;
    memset(ErrorTable, 0, sizeof(ErrorTable));
}

int
hpss_error_put(hpss_error_t he)
{
    pthread_once(&ErrorTableInitialized, error_table_init);

    int index;
    pthread_mutex_lock(&ErrorTableMutex);
    {
        index = ErrorTableIndex;
        ErrorTable[index] = he;
        // Can't use index 0
        ErrorTableIndex = ((ErrorTableIndex + 1) % (MAX_HPSS_ERRORS-1)) + 1;
    }
    pthread_mutex_unlock(&ErrorTableMutex);
    return -index;
}

hpss_error_t
hpss_error_get(int index)
{
    return ErrorTable[-index];
}

int
hpss_error_status(int error)
{
    return ErrorTable[-error].returned_value;
}

void
_errno_string(int err, char * buf, size_t buflen)
{
    const char * unknown_error_msg = "unknown error code";

    const char * error_msg = hpss_ErrnoName(-abs(err));
    if (strcmp(error_msg, "unknown error code") != 0)
    {
        strncpy(buf, error_msg, buflen);
        return;
    }

    strerror_r(abs(err), buf, buflen);
}

globus_result_t
hpss_error_to_globus_result(int error)
{
    if (error >= 0)
        return GLOBUS_SUCCESS;

    hpss_error_t he = ErrorTable[-error];

    if (he.returned_value == HPSS_E_NOERROR)
        return GLOBUS_SUCCESS;

    int code = 500;
    const char * type = "GENERAL_ERROR";

    switch (he.returned_value)
    {
    case -ENOENT:
        code = 404;
        type = "PATH_NOT_FOUND";
        break;
    case -EISDIR:
        code = 553;
        type = "IS_A_DIRECTORTY";
        break;
    case -ENOTDIR:
        code = 553;
        type = "NOT_A_DIRECTORY";
        break;
    case -ENOSPC:
        code = 451;
        type = "NO_SPACE_LEFT";
        break;
    case -EDQUOT:
        code = 451;
        type = "QUOTA_EXCEEDED";
        break;
    case -EPERM:
    case -EACCES:
        code = 550;
        type = "PERMISSION_DENIED";
        break;
    default:
        break;
    }

    const char * format = "HPSS-Reason: %s\n"
                          "HPSS-Function: %s";

    char rv_reason[64];
    char errno_reason[64];

    _errno_string(he.returned_value, rv_reason, sizeof(rv_reason));

    if (he.errno_state.hpss_errno != 0)
    {
        format = "HPSS-Reason: %s\n"
                 "HPSS-Function: %s\n"
                 "HPSS-Last-Error: %s\n"
                 "HPSS-Last-Function: %s";
        _errno_string(he.errno_state.hpss_errno, errno_reason, sizeof(errno_reason));
    }

    return globus_error_put(GlobusGFSErrorObj(
        NULL,
        code,
        type,
        format,
        rv_reason,
        he.function,
        errno_reason,
        he.errno_state.func));
}

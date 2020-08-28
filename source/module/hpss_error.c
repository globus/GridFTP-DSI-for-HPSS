/*
 * System includes.
 */
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_String.h>
#include <hpss_errno.h>
#include <hpss_error.h>

#define MAX_HPSS_ERRORS 64
static hpss_error_t ErrorTable[64];
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

    if (he.errno_state.hpss_errno != 0)
    {
        format = "HPSS-Reason: %s\n"
                 "HPSS-Function: %s\n"
                 "HPSS-Last-Error: %s\n"
                 "HPSS-Last-Function: ";
    }

    return globus_error_put(GlobusGFSErrorObj(
        NULL,
        code,
        type,
        format,
        hpss_ErrnoString(he.returned_value),
        he.function,
        hpss_ErrnoString(he.errno_state.hpss_errno),
        he.errno_state.func));
}

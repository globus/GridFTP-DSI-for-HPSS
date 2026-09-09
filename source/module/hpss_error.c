/*
 * System includes
 */
#include <pthread.h>
#include <stdbool.h>
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

/*
 * Local includes
 */
#include "hpss_log.h"
#include "logging.h"

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
    const char * error_msg = hpss_ErrnoName(-abs(err));
    if (error_msg != NULL && strcmp(error_msg, "unknown error code") != 0)
        strncpy(buf, error_msg, buflen);
    else
        snprintf(buf, buflen, "%d", -abs(err));
}

/*
 * The fields reported by HPSS_ERROR_FIELDS(), in that macro's argument order,
 * so both of its callers format the same values from one place.
 *
 * RequestID is sized for the fallback rendering: the UUID formatter prints
 * the raw member values when the HPSS string conversion is unavailable, which
 * is considerably longer than a 36-character UUID.
 *
 * LastFunction is a copy because hpss_errno_state_t.func is an array inside
 * the hpss_error_t that _build_error_fields() receives by value. A pointer
 * to it would dangle as soon as that function returned, and the client
 * message would show whatever the stack then held. Function is __func__, a
 * string literal, so a pointer to it stays valid.
 */
struct _error_fields {
    char         RequestID[256];
    char         Reason[64];
    char         LastError[64];
    const char * Function;
    char         LastFunction[64];
};

/*
 * HPSS_REQID_T() allocates through a pool, so the pool is created and
 * released here and the result copied into caller-owned storage. The absent
 * cases are spelled "none" rather than left empty or passed as NULL: the log
 * form always prints all five fields, an empty value is ambiguous to a
 * parser, and "%s" with NULL is undefined behavior that only happens to
 * print "(null)" under glibc.
 *
 * A request ID of zero means HPSS reported none, as when the call failed
 * before reaching an HPSS server or login had not completed. Formatting it
 * would print an all-zero ID that looks real and matches no HPSS alarm.
 */
static bool
_request_id_is_unset(hpss_reqid_t RequestID)
{
#if HPSS_MAJOR_VERSION < 8
    return RequestID == 0;
#else
    hpss_reqid_t zero;
    memset(&zero, 0, sizeof(zero));
    return memcmp(&RequestID, &zero, sizeof(zero)) == 0;
#endif
}

static void
_build_error_fields(hpss_error_t he, struct _error_fields * Fields)
{
    if (_request_id_is_unset(he.errno_state.requestId))
    {
        snprintf(Fields->RequestID, sizeof(Fields->RequestID), "none");
    }
    else
    {
        struct pool * pool = &(struct pool){NULL, 0, 0};
        pool_create(pool);
        snprintf(Fields->RequestID,
                 sizeof(Fields->RequestID),
                 "%s",
                 HPSS_REQID_T(he.errno_state.requestId));
        pool_destroy(pool);
    }

    _errno_string(he.returned_value, Fields->Reason, sizeof(Fields->Reason));

    if (he.errno_state.hpss_errno != 0)
        _errno_string(he.errno_state.hpss_errno,
                      Fields->LastError,
                      sizeof(Fields->LastError));
    else
        snprintf(Fields->LastError, sizeof(Fields->LastError), "none");

    Fields->Function     = he.function ? he.function : "none";
    // The precision states that a longer name is cut to fit on purpose.
    snprintf(Fields->LastFunction,
             sizeof(Fields->LastFunction),
             "%.*s",
             (int)sizeof(Fields->LastFunction) - 1,
             he.errno_state.func ? he.errno_state.func : "none");
}

/*
 * Logs at INFO, not ERROR. An HPSS error does not necessarily mean anything
 * on this endpoint needs an administrator's attention -- the client issues
 * commands it expects to fail, such as stat'ing a directory before creating
 * it -- and the HPSS server has already raised its own alarm for the failures
 * that do matter. This entry is not a second alert, it is the join key.
 *
 * INFO is also the severity that every correlating field already uses, such
 * as the path and the transfer parameters. Emitting the request ID at a
 * higher severity would let a site configure a log that holds the request ID
 * but not the context needed to interpret it.
 */
int
hpss_error_record(hpss_error_t he)
{
    struct _error_fields fields;
    _build_error_fields(he, &fields);

    INFO(HPSS_ERROR_FIELDS_LAST(" | "),
         fields.RequestID,
         fields.Reason,
         fields.Function,
         fields.LastError,
         fields.LastFunction);

    return hpss_error_put(he);
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
        code = 550;
        type = "PATH_NOT_FOUND";
        break;
    case -EISDIR:
        code = 553;
        type = "IS_A_DIRECTORY";
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

    struct _error_fields fields;
    _build_error_fields(he, &fields);

    /*
     * The last-error fields are still omitted when HPSS did not record one,
     * to keep the client-facing response as it was. Both variants take the
     * same argument order, so the shorter format simply ignores the trailing
     * arguments.
     */
    const char * format = HPSS_ERROR_FIELDS("\n");

    if (he.errno_state.hpss_errno != 0)
        format = HPSS_ERROR_FIELDS_LAST("\n");

    return globus_error_put(GlobusGFSErrorObj(
        NULL,
        code,
        type,
        format,
        fields.RequestID,
        fields.Reason,
        fields.Function,
        fields.LastError,
        fields.LastFunction));
}

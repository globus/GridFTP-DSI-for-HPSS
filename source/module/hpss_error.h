#ifndef _HPSS_ERROR_H_
#define _HPSS_ERROR_H_

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>

/*
 * Record of all available error information minus caller's context.
 */
typedef struct {
    int                returned_value;
    const char *       function;
    hpss_errno_state_t errno_state;
} hpss_error_t;

/*
 * An HPSS server that reports an ETRANSFER alarm identifies the failed work
 * only by its HPSS request ID. Correlating that back to a Globus transfer
 * previously required matching timestamps by hand, because the request ID
 * sits in hpss_errno_state_t and was printed only by the TRACE-level API
 * tracing, which is off in production.
 *
 * These two macros hold the one field list used to report an HPSS error, so
 * the request ID reaches both of its destinations from a single definition:
 *
 *   - hpss_error_record() logs it to the GridFTP log alongside the user and
 *     TaskID that every log entry carries, which is what makes the join
 *     exact. This covers every HPSS error, including ones a caller recovers
 *     from, discards in favor of another error, or converts into a success.
 *
 *   - hpss_error_to_globus_result() puts it in the error returned to the
 *     client, which is how the request ID reaches sites that do not run the
 *     GridFTP log at INFO severity. This covers only errors that propagate.
 *
 * The delimiter is a parameter because the client-facing response separates
 * fields with newlines, while a log entry must stay on one line or the
 * request ID is split from the TaskID that identifies it. The log form uses
 * " | " rather than a space because an error description may itself contain
 * spaces, which would leave the field boundaries ambiguous.
 *
 * HPSS-Request-ID leads the list so both variants take the same argument
 * order, which preserves the existing pattern of passing the superset of
 * arguments and letting the shorter format ignore the excess.
 */
#define HPSS_ERROR_FIELDS(D)       \
    "HPSS-Request-ID: %s"     D    \
    "HPSS-Reason: %s"         D    \
    "HPSS-Function: %s"

#define HPSS_ERROR_FIELDS_LAST(D)  \
    "HPSS-Request-ID: %s"     D    \
    "HPSS-Reason: %s"         D    \
    "HPSS-Function: %s"       D    \
    "HPSS-Last-Error: %s"     D    \
    "HPSS-Last-Function: %s"

/*
 * Stores the hpss_error_t internally and returns a value that can
 * be used by hpss_error_to_globus_result to lookup the error.
 */
int
hpss_error_put(hpss_error_t he);

/*
 * Logs the error for HPSS request ID correlation, then stores it with
 * hpss_error_put() and returns that value. This is the single hook used by
 * the HPSS_ERROR() macro, so every HPSS wrapper that reports an int error is
 * covered without per-wrapper changes.
 */
int
hpss_error_record(hpss_error_t he);

hpss_error_t
hpss_error_get(int);

/*
 * Takes the (int) returned from hpss_error_put() and returns the actual
 * return value of the HPSS function.
 */
int
hpss_error_status(int);

globus_result_t
hpss_error_to_globus_result(int);

#define CONF_ERR "The HPSS connector has a configuration issue that must be " \
                 "resolved by the endpoint administrator."

#define HPSSConfigurationError()                             \
  globus_error_put(                                          \
    GlobusGFSErrorObj(                                       \
      NULL,                                                  \
      500,                                                   \
      "CONFIGURATION_ERROR",                                 \
      "GridFTP-Message: " CONF_ERR "\r\n" \
      "GridFTP-JSON-Result: {"                               \
        "\"message\": \"" CONF_ERR "\""  \
      "}"                                                    \
    )                                                        \
  )

#define LOGIN_ERR "The HPSS connector could not log you into HPSS. " \
                  "The issue must be resolved by the endpoint administrator."

#define HPSSLoginDenied()                  \
  globus_error_put(                        \
    GlobusGFSErrorObj(                     \
      NULL,                                \
      500,                                 \
      "LOGIN_DENIED",                      \
      "GridFTP-Message: " LOGIN_ERR "\r\n" \
      "GridFTP-JSON-Result: {"             \
        "\"message\": \"" LOGIN_ERR "\""   \
      "}"                                  \
    )                                      \
  )

#define WRONG_VERSION_ERR "The HPSS connector was not built for this version of HPSS. " \
                          "The issue must be resolved by the endpoint administrator."

#define HPSSWrongVersion()                         \
  globus_error_put(                                \
    GlobusGFSErrorObj(                             \
      NULL,                                        \
      500,                                         \
      "WRONG_VERSION",                             \
      "GridFTP-Message: " WRONG_VERSION_ERR "\r\n" \
      "GridFTP-JSON-Result: {"                     \
        "\"message\": \"" WRONG_VERSION_ERR "\""   \
      "}"                                          \
    )                                              \
  )

#endif /* _HPSS_ERROR_H_ */

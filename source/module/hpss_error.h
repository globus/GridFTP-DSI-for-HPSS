#ifndef _HPSS_ERROR_H_
#define _HPSS_ERROR_H_

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
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
 * Stores the hpss_error_t internally and returns a value that can
 * be used by hpss_error_to_globus_result to lookup the error.
 */
int
hpss_error_put(hpss_error_t he);

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

#endif /* _HPSS_ERROR_H_ */

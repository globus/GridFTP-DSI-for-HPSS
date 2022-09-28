/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

const int GlobusError = 0x0123ABCD;

globus_result_t
globus_error_put(globus_object_t * error)
{
    return GlobusError;
}

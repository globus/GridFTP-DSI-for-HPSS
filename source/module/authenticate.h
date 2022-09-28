#ifndef HPSS_DSI_AUTHENTICATE_H
#define HPSS_DSI_AUTHENTICATE_H

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

globus_result_t
authenticate(char *LoginName,
             char *AuthenticationMech,
             char *Authenticator,
             char *UserName);

#endif /* HPSS_DSI_AUTHENTICATE_H */

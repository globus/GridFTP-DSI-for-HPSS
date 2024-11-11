#ifndef HPSS_DSI_CONFIG_H
#define HPSS_DSI_CONFIG_H

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

typedef struct config
{
    char *LoginName;
    char *AuthenticationMech;
    char *Authenticator;
    int   UDAChecksumSupport;
} config_t;

globus_result_t
config_init(globus_gfs_operation_t Operation, config_t **Config);

void
config_destroy(config_t *Config);

#endif /* HPSS_DSI_CONFIG_H */

#ifndef HPSS_DSI_CONFIG_H
#define HPSS_DSI_CONFIG_H

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

#define DEFAULT_CONFIG_FILE "/var/hpss/etc/gridftp_hpss_dsi.conf"

typedef struct config
{
    char *LoginName;
    char *AuthenticationMech;
    char *Authenticator;
    int   UDAChecksumSupport;
} config_t;

globus_result_t
config_init(config_t **Config);

void
config_destroy(config_t *Config);

#endif /* HPSS_DSI_CONFIG_H */

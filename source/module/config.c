/*
 * System includes
 */
#include <strings.h>
#include <stdlib.h>
#include <jansson.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * Local includes
 */
#include "logging.h"
#include "config.h"
#include "hpss.h"


static globus_result_t
config_parse_json(const char * JsonConfig, config_t * Config)
{
    globus_result_t                     result = GLOBUS_SUCCESS;
    int                                 rc = 0;
    json_t *                            json = NULL;
    json_error_t                        json_error = {0};
    const char *                        authentication_mech = NULL;
    const char *                        authenticator = NULL;
    const char *                        login_name = NULL;
    int                                 uda_checksum = 0;

    json = json_loads(JsonConfig, 0, &json_error);
    if (json == NULL)
    {
        const char *fmt = "Error loading collection storage policy: %s\n";
        size_t msg_len = snprintf(NULL, 0, fmt, json_error.text);
        char msg[msg_len+1];

        snprintf(msg, sizeof(msg), fmt, json_error.text);
        result = GlobusGFSErrorGeneric(msg);
        goto error_load;
    }

    rc = json_unpack_ex(
            json,
            &json_error,
            0,
            "{s:s, s:s, s:s, s:b}",
            "authentication_mech",
            &authentication_mech,
            "authenticator",
            &authenticator,
            "login_name",
            &login_name,
            "uda_checksum",
            &uda_checksum);

    if (rc != 0)
    {
        rc = json_unpack_ex(
                json,
                &json_error,
                0,
                "{s:s, s:s, s:b}",
                "authentication_mech",
                &authentication_mech,
                "authenticator",
                &authenticator,
                "uda_checksum",
                &uda_checksum);
    }

    if (rc != 0)
    {
        const char *fmt = "Error unpacking collection storage policy: %s\n";
        size_t msg_len = snprintf(NULL, 0, fmt, json_error.text);
        char msg[msg_len+1];

        snprintf(msg, sizeof(msg), fmt, json_error.text);

        result = GlobusGFSErrorInternalError(msg);

        goto unpack_fail;
    }

    // Allow renamed hpssftp accounts. This overrides the gateway setting as a
    // means to provide an upgrade path. On initial upgrade of gcsv5, the gateway
    // will be giving us the wrong login_name.
    const char * env_login_name = getenv("HPSS_DSI_LOGIN_NAME");
    if (env_login_name)
        login_name = env_login_name;

    // If there is no setting, fallback to the default.
    if (login_name == NULL)
        login_name = "hpssftp";

    Config->LoginName = strdup(login_name);
    Config->AuthenticationMech = strdup(authentication_mech);
    Config->Authenticator = strdup(authenticator);
    Config->UDAChecksumSupport = 0;
    Config->UDAChecksumSupport = uda_checksum;

    log_message(
        LOG_TYPE_DEBUG,
        "HPSS Connector loaded with config: %s=%s, %s=%s, %s=%s, %s=%s",
        "LoginName", Config->LoginName,
        "AuthenticationMech", Config->AuthenticationMech,
        "Authenticator", Config->Authenticator,
        "UDAChecksumSupport", Config->UDAChecksumSupport ? "True" : "False");

unpack_fail:
    json_decref(json);
error_load:

    return result;
}


/*
 * Right now, the only env vars we process affect HPSS's default
 * config, not ours.
 */
globus_result_t
config_process_env()
{
    int          retval    = 0;
    char *       env_value = NULL;
    api_config_t api_config;

    /*
     * Get the current HPSS client configuration.
     */
    retval = Hpss_GetConfiguration(&api_config);
    if (retval != 0)
        return hpss_error_to_globus_result(retval);

    if ((env_value = getenv("HPSS_API_DEBUG")))
        api_config.DebugValue = atoi(env_value);

    if ((env_value = getenv("HPSS_API_DEBUG_PATH")))
        strncpy(api_config.DebugPath, env_value, sizeof(api_config.DebugPath));

    /* Now set the current HPSS client configuration. */
    api_config.Flags = API_USE_CONFIG;
    retval           = Hpss_SetConfiguration(&api_config);
    if (retval != 0)
        return hpss_error_to_globus_result(retval);

    return GLOBUS_SUCCESS;
}

globus_result_t
config_init(globus_gfs_operation_t Operation, config_t **Config)
{
    char *          config_file_path = NULL;
    globus_result_t result           = GLOBUS_SUCCESS;

    /* Allocate the config struct */
    *Config = malloc(sizeof(config_t));
    if (!*Config)
    {
        result = GlobusGFSErrorMemory("config_t");
        goto cleanup;
    }
    memset(*Config, 0, sizeof(config_t));

    char * config_data = NULL;
    globus_gridftp_server_get_config_data(Operation, "config", &config_data);
    if (!config_data)
    {
        result = GlobusGFSErrorGeneric("Failed to retrieve collection storage policy.");
        goto cleanup;
    }

    result = config_parse_json(config_data, *Config);
    if (result)
        goto cleanup;

    result = config_process_env();
cleanup:
    if (config_file_path)
        free(config_file_path);
    if (result)
    {
        config_destroy(*Config);
        *Config = NULL;
    }
    return result;
}

void
config_destroy(config_t *Config)
{
    if (Config)
    {
        if (Config->LoginName)
            free(Config->LoginName);
        if (Config->AuthenticationMech)
            free(Config->AuthenticationMech);
        if (Config->Authenticator)
            free(Config->Authenticator);

        free(Config);
    }
}

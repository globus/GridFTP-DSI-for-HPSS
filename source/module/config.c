/*
 * System includes
 */
#include <strings.h>
#include <stdlib.h>
#ifdef GCSV5
#include <jansson.h>
#endif /* GCSV5 */

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

#ifndef GCSV5
/*
 * The config file search order is:
 *   1) env HPSS_DSI_CONFIG_FILE=<path>
 *   2) $HPSS_PATH_ETC/gridftp_hpss_dsi.conf
 *   3) DEFAULT_CONFIG_FILE (/var/hpss/etc/gridftp_hpss_dsi.conf)
 */
globus_result_t
config_find_config_file(char **ConfigFilePath)
{
    globus_result_t result        = GLOBUS_SUCCESS;
    char *          hpss_path_etc = NULL;
    int             retval        = 0;

    /* Initialize the return value. */
    *ConfigFilePath = NULL;

    if (getenv("HPSS_DSI_CONFIG_FILE"))
    {
        *ConfigFilePath = strdup(getenv("HPSS_DSI_CONFIG_FILE"));
        if (!*ConfigFilePath)
        {
            result = GlobusGFSErrorMemory("HPSS_DSI_CONFIG_FILE");
            goto cleanup;
        }

        /* Check if it exists and if we have access. */
        retval = access(*ConfigFilePath, R_OK);
        if (retval)
        {
            ERROR("Could not open config file %s", *ConfigFilePath);
            result = HPSSConfigurationError();
        }
        goto cleanup;
    }

    /* Check for HPSS_PATH_ETC in the environment. */
    hpss_path_etc = Hpss_Getenv("HPSS_PATH_ETC");

    /* If it exists... */
    if (hpss_path_etc != NULL)
    {
        /* Construct the full path. */
        *ConfigFilePath = globus_common_create_string(
            "%s/gridftp_hpss_dsi.conf", hpss_path_etc);
        if (!*ConfigFilePath)
        {
            result = GlobusGFSErrorMemory("config file path");
            goto cleanup;
        }

        /* Check if it exists and if we have access. */
        retval = access(*ConfigFilePath, R_OK);

        /* If we have access, we are done. */
        if (retval == 0)
            goto cleanup;

        /* Release the full path to the config file. */
        free(*ConfigFilePath);
        *ConfigFilePath = NULL;

        /* Otherwise, determine why we do not have access. */
        switch (errno)
        {
        /*
         * For all cases in which the file does not exist...
         */
        case ENOENT:
        case ENOTDIR:
            break;

        /*
         * All other cases indicate failure at some level.
         */
        default:
            ERROR("Can not access the config file: %s", strerror(errno));
            result = HPSSConfigurationError();
            goto cleanup;
        }
    }

    /*
     * No success from the environment, let's check the default.
     */

    /* Check if it exists and if we have access. */
    retval = access(DEFAULT_CONFIG_FILE, R_OK);

    /* All failures are error conditions at this stage. */
    if (retval != 0)
    {
        ERROR("Can not access the config file: %s", strerror(errno));
        result = HPSSConfigurationError();
        goto cleanup;
    }

    /* Copy out the config file. */
    *ConfigFilePath = globus_libc_strdup(DEFAULT_CONFIG_FILE);
    if (!*ConfigFilePath)
    {
        result = GlobusGFSErrorMemory("config file path");
        goto cleanup;
    }

cleanup:
    if (result != GLOBUS_SUCCESS && *ConfigFilePath)
        free(*ConfigFilePath);

    return result;
}

/*
 * Helper that removes leading whitespace, newlines, comments, etc.
 */
static void
config_find_next_word(char *Buffer, char **Word, int *Length)
{
    *Word   = NULL;
    *Length = 0;

    if (Buffer == NULL)
        return;

    /* Skip spacing. */
    while (isspace(*Buffer))
        Buffer++;

    /* Skip EOL */
    if (*Buffer == '\0' || *Buffer == '\n')
        return;

    /* Skip comments. */
    if (*Buffer == '#')
        return;

    /* Return the start of the found keep word */
    *Word = Buffer;

    /* Find the length of the word. */
    while (!isspace(*Buffer) && *Buffer != '\0' && *Buffer != '\n')
    {
        (*Length)++;
        Buffer++;
    }
}

int
config_get_bool_value(char *Value, int ValueLength)
{
    if (!strncasecmp(Value, "on", ValueLength) ||
        !strncasecmp(Value, "true", ValueLength) ||
        !strncasecmp(Value, "yes", ValueLength))
    {
        return 1;
    }

    return 0;
}

static globus_result_t
config_parse_file(char *ConfigFilePath, config_t *Config)
{
    int             tmp_length   = 0;
    int             key_length   = 0;
    int             value_length = 0;
    FILE *          config_f     = NULL;
    char *          tmp          = NULL;
    char *          key          = NULL;
    char *          value        = NULL;
    char            buffer[1024];
    globus_result_t result = GLOBUS_SUCCESS;

    /*
     * Open the config file.
     */
    config_f = fopen(ConfigFilePath, "r");
    if (!config_f)
    {
        ERROR("Can not open config file: %s", strerror(errno));
        result = HPSSConfigurationError();
        goto cleanup;
    }

    int l_no = 0;
    while (fgets(buffer, sizeof(buffer), config_f) != NULL)
    {
        l_no++;

        /* Locate the keyword */
        config_find_next_word(buffer, &key, &key_length);
        if (key == NULL)
            continue;

        /* Locate the value */
        config_find_next_word(key + key_length, &value, &value_length);
        if (value == NULL)
        {
            ERROR("Configuration error, no value given for option \"%.*s\" "
                  "on line %d", key_length, key, l_no);
            result = HPSSConfigurationError();
            goto cleanup;
        }

        /* Make sure the value was the last word. */
        config_find_next_word(value + value_length, &tmp, &tmp_length);
        if (tmp != NULL)
        {
            ERROR("Configuration error, found extra value on line %d", l_no);
            result = HPSSConfigurationError();
            goto cleanup;
        }

        /* Now match the directive. */
        if (key_length == strlen("LoginName") &&
            strncasecmp(key, "LoginName", key_length) == 0)
        {
            Config->LoginName = strndup(value, value_length);
        } else if (key_length == strlen("AuthenticationMech") &&
                   strncasecmp(key, "AuthenticationMech", key_length) == 0)
        {
            Config->AuthenticationMech = strndup(value, value_length);
        } else if (key_length == strlen("Authenticator") &&
                   strncasecmp(key, "Authenticator", key_length) == 0)
        {
            Config->Authenticator = strndup(value, value_length);
        } else if (key_length == strlen("UDAChecksumSupport") &&
                   strncasecmp(key, "UDAChecksumSupport", key_length) == 0)
        {
            Config->UDAChecksumSupport =
                config_get_bool_value(value, value_length);
        } else
        {
            ERROR("Configuration error, unsupported option \"%.*s\"",
                  key_length, key);
            result = HPSSConfigurationError();
            goto cleanup;
        }
    }

cleanup:
    if (config_f != NULL)
        fclose(config_f);

    return result;
}
#else /* GCSV5 */
static globus_result_t
config_parse_json(const char * JsonConfig, config_t * Config)
{
    globus_result_t                     result = GLOBUS_SUCCESS;
    int                                 rc = 0;
    json_t *                            json = NULL;
    json_error_t                        json_error = {0};
    char *                              authentication_mech = NULL;
    char *                              authenticator = NULL;
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
            "{s:s, s:s, s:b}",
            "authentication_mech",
            &authentication_mech,
            "authenticator",
            &authenticator,
            "uda_checksum",
            &uda_checksum);

    if (rc != 0)
    {
        const char *fmt = "Error unpacking collection storage policy: %s\n";
        size_t msg_len = snprintf(NULL, 0, fmt, json_error.text);
        char msg[msg_len+1];

        snprintf(msg, sizeof(msg), fmt, json_error.text);

        result = GlobusGFSErrorInternalError(msg);

        goto unpack_fail;
    }

    Config->LoginName = strdup("hpssftp");
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

#endif /* GCSV5 */

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

#ifndef GCSV5
    /* Find the config file. */
    result = config_find_config_file(&config_file_path);
    if (result != GLOBUS_SUCCESS)
        return result;

    result = config_parse_file(config_file_path, *Config);
    if (result)
        goto cleanup;
#else /* GCSV5 */
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
#endif /* GCSV5 */

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

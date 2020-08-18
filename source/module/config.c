/*
 * System includes
 */
#include <stdlib.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * Local includes
 */
#include "config.h"
#include "hpss.h"

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
            result = GlobusGFSErrorGeneric(
                "Could not open config file defined in environment");
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
            result =
                GlobusGFSErrorSystemError("Can not access config file", errno);
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
        result = GlobusGFSErrorSystemError("Can not access config file", errno);
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
        result = GlobusGFSErrorWrapFailed(
            "Attempting to open config file",
            GlobusGFSErrorSystemError("fopen()", errno));
        goto cleanup;
    }

    while (fgets(buffer, sizeof(buffer), config_f) != NULL)
    {
        /* Locate the keyword */
        config_find_next_word(buffer, &key, &key_length);
        if (key == NULL)
            continue;

        /* Locate the value */
        config_find_next_word(key + key_length, &value, &value_length);
        if (value == NULL)
        {
            result = GlobusGFSErrorWrapFailed("Parsing config options",
                                              GlobusGFSErrorGeneric(buffer));
            goto cleanup;
        }

        /* Make sure the value was the last word. */
        config_find_next_word(value + value_length, &tmp, &tmp_length);
        if (tmp != NULL)
        {
            result = GlobusGFSErrorWrapFailed("Parsing config options",
                                              GlobusGFSErrorGeneric(buffer));
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
            result = GlobusGFSErrorWrapFailed("Parsing config options",
                                              GlobusGFSErrorGeneric(buffer));
            goto cleanup;
        }
    }

cleanup:
    if (config_f != NULL)
        fclose(config_f);

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
        return GlobusGFSErrorSystemError("hpss_GetConfiguration", -retval);

    if ((env_value = getenv("HPSS_API_DEBUG")))
        api_config.DebugValue = atoi(env_value);

    if ((env_value = getenv("HPSS_API_DEBUG_PATH")))
        strncpy(api_config.DebugPath, env_value, sizeof(api_config.DebugPath));

    /* Now set the current HPSS client configuration. */
    api_config.Flags = API_USE_CONFIG;
    retval           = Hpss_SetConfiguration(&api_config);
    if (retval != 0)
        return GlobusGFSErrorSystemError("hpss_SetConfiguration", -retval);

    return GLOBUS_SUCCESS;
}

globus_result_t
config_init(config_t **Config)
{
    char *          config_file_path = NULL;
    globus_result_t result           = GLOBUS_SUCCESS;

    *Config = NULL;

    /* Find the config file. */
    result = config_find_config_file(&config_file_path);
    if (result != GLOBUS_SUCCESS)
        return result;

    /* Allocate the config struct */
    *Config = malloc(sizeof(config_t));
    if (!*Config)
    {
        result = GlobusGFSErrorMemory("config_t");
        goto cleanup;
    }
    memset(*Config, 0, sizeof(config_t));

    result = config_parse_file(config_file_path, *Config);
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

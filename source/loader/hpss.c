
/**************************************************************************
 *
 * IMPORTANT IMPORTANT IMPORTANT IMPORTANT IMPORTANT IMPORTANT
 *
 * Read README.xdr before trying to make changes to the loader.
 *
 **************************************************************************/

/*******************************************************************************
 * This loader now exports two interfaces:
 *   - the traditional gridftp dsi interface for transfers (GCSv4, GCSv5)
 *   - a new gcsv5 interface for access to HPSS API calls (GCSv5)
 ******************************************************************************/

/*
 * System includes
 */
#define _GNU_SOURCE         /* See feature_test_macros(7) */
#include <dlfcn.h>
#include <stdarg.h>
#include <stdlib.h>
#include <syslog.h>
#include <string.h>
#include <stdio.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "version.h"

// GlobusExtensionDeclareModule(MODULE_NAME);
globus_module_descriptor_t MODULE_NAME;

static const char * DSI_NAME = MODULE_NAME_STRING;
static const char * DSI_INTERFACE = "hpss_dsi_iface";

/* Our handle reference which we hold between activate / deactivate. */
static void *_real_module_handle = NULL;

/*******************************************************************************
 * Functions common to both interfaces
 ******************************************************************************/

static char *
build_string(const char * format, ...)
{
    va_list ap;
    va_start(ap, format);
    int rc = vsnprintf(NULL, 0, format, ap);
    va_end(ap);

    char * str = malloc(rc + 1);

    va_start(ap, format);
    vsnprintf(str, rc + 1, format, ap);
    va_end(ap);

    return str;
}


static void
init_logging(const char * Ident, int Facility)
{
    openlog(Ident, 0, Facility);
}

static void
loader_log_to_syslog(const char *Format, ...)
{
    va_list ap;

    openlog("globus-gridftp-server", 0, LOG_FTP);

    va_start(ap, Format);
    vsyslog(LOG_ERR, Format, ap);
    va_end(ap);
}

static char *
load_module()
{
    if (!_real_module_handle)
    {
        /* Clear any previous errors. */
        dlerror();

        /* Open our module. */
        _real_module_handle = dlopen("libglobus_gridftp_server_hpss_real.so",
                                     RTLD_NOW | RTLD_DEEPBIND);

        if (!_real_module_handle)
            return build_string(
                "Failed to open libglobus_gridftp_server_hpss_real.so: %s",
                dlerror());
    }

    return NULL;
}

static void
unload_module()
{
    if (_real_module_handle)
        dlclose(_real_module_handle);
    _real_module_handle = NULL;
}

static void *
find_symbol(const char * symbol_name, const char ** error_msg)
{
    /* Clear any previous errors. */
    dlerror();

    void * symbol_ptr = dlsym(_real_module_handle, symbol_name);
    if (!symbol_ptr)
        *error_msg = build_string("Failed to find symbol %s in "
                                  "libglobus_gridftp_server_hpss_real.so: %s",
                                  symbol_name,
                                  dlerror());
    return symbol_ptr;
}

/*******************************************************************************
 * THIS IS THE GRIDFTP DSI INTERFACE
 ******************************************************************************/

int
activate(void)
{
    init_logging("globus-gridftp-server", LOG_FTP);

    int rc = globus_module_activate(GLOBUS_COMMON_MODULE);
    if (rc != GLOBUS_SUCCESS)
        return rc;

    const char * error_msg = load_module();
    if (error_msg)
    {
        loader_log_to_syslog(error_msg);
        free((void *)error_msg);
        return GLOBUS_FAILURE;
    }

    globus_gfs_storage_iface_t * dsi_interface = find_symbol(DSI_INTERFACE, &error_msg);
    if (!dsi_interface)
    {
        loader_log_to_syslog(error_msg);
        free((void *)error_msg);
        return GLOBUS_FAILURE;
    }

    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY, (char *)DSI_NAME, &MODULE_NAME, dsi_interface);

    return GLOBUS_SUCCESS;
}

int
deactivate(void)
{
    globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, (char *)DSI_NAME);
    globus_module_deactivate(GLOBUS_COMMON_MODULE);

    unload_module();
    return GLOBUS_SUCCESS;
}

// GlobusExtensionDefineModule(MODULE_NAME) = {
globus_module_descriptor_t MODULE_NAME = {
    "globus_gridftp_server_" MODULE_NAME_STRING,
    activate,
    deactivate,
    GLOBUS_NULL,
    GLOBUS_NULL,
    &version
};

#ifdef GCSV5
/*******************************************************************************
 * THIS IS THE GCSV5 INTERFACE
 ******************************************************************************/

/*
 * To simplify passing char pointers back to the Python client, we use a callback
 * passed in as an arg. If we encounter an error, we call the callback with a
 * useful error message and return non zero. On success, we call the callback
 * with the home directory and return zero.
 *
 * When an error occurs, we return an error messages to the caller instead of
 * logging to syslog so that the message can end up in the GCSM log file.
 */

int
get_home_directory_ex(const char *  AuthenticationMech, // Location of credentials
                      const char *  Authenticator, // <type>:<path>
                      const char *  LoginName, // HPSS super user
                      const char *  UserName, // User to switch to
                      void          (*callback)(const char *))
{
    const char * error_msg = load_module();
    if (error_msg)
    {
        callback(error_msg);
        free((void *)error_msg);
        return GLOBUS_FAILURE;
    }

    int (*_get_home_directory)(const char *  AuthenticationMech,
                               const char *  Authenticator,
                               const char *  LoginName,
                               const char *  UserName,
                               const char ** HomeDirectory,
                               const char ** ErrorMsg);

    _get_home_directory = find_symbol("get_home_directory", &error_msg);
    if (!_get_home_directory)
    {
        callback(error_msg);
        free((void *)error_msg);
        return GLOBUS_FAILURE;
    }

    /*
     * Allow renamed hpssftp accounts. This overrides the gateway setting as a
     * means to provide an upgrade path. On initial upgrade of gcsv5, the gateway
     * will be giving us the wrong LoginName.
     */
    const char * env_login_name = getenv("HPSS_DSI_LOGIN_NAME");
    if (env_login_name)
        LoginName = env_login_name;

    const char * home_directory = NULL;
    error_msg = NULL;
    int rc = _get_home_directory(AuthenticationMech,
                                 Authenticator,
                                 LoginName,
                                 UserName,
                                 &home_directory,
                                 &error_msg);
    if (rc)
        callback(error_msg);
    else
        callback(home_directory);
    return rc;
}

/*
 * Older version which did not include LoginName.
 */
int
get_home_directory(const char *  AuthenticationMech, // Location of credentials
                   const char *  Authenticator, // <type>:<path>
                   const char *  UserName, // User to switch to
                   void          (*callback)(const char *))
{
    return get_home_directory_ex(AuthenticationMech,
                                 Authenticator,
                                 "hpssftp",
                                 UserName,
                                 callback);
}
#endif /* GCSV5 */

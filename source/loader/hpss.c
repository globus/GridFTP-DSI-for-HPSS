
/**************************************************************************
 *
 * IMPORTANT IMPORTANT IMPORTANT IMPORTANT IMPORTANT IMPORTANT
 *
 * Read README.xdr before trying to make changes to the loader.
 *
 **************************************************************************/

/*
 * System includes
 */
#include <dlfcn.h>
#include <stdarg.h>
#include <syslog.h>

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

/* Our handle reference which we hold between activate / deactivate. */
static void *_real_module_handle = NULL;

static void
loader_log_to_syslog(const char *Format, ...)
{
    va_list ap;

    openlog("globus-gridftp-server", 0, LOG_FTP);

    va_start(ap, Format);
    vsyslog(LOG_ERR, Format, ap);
    va_end(ap);
}

/*
 * Only allowed an int return, must log errors internally.
 */
int
loader_activate(const char *                DsiName,
                const char *                DsiInterface,
                globus_module_descriptor_t *Module)
{
    int                         rc;
    globus_gfs_storage_iface_t *dsi_interface = NULL;

    rc = globus_module_activate(GLOBUS_COMMON_MODULE);
    if (rc != GLOBUS_SUCCESS)
        return rc;

    /* Clear any previous errors. */
    dlerror();

    /* Open our module. */
    _real_module_handle = dlopen("libglobus_gridftp_server_hpss_real.so",
                                 RTLD_NOW | RTLD_DEEPBIND);

    if (!_real_module_handle)
    {
        loader_log_to_syslog(
            "Failed to open libglobus_gridftp_server_hpss_real.so: %s",
            dlerror());
        return GLOBUS_FAILURE;
    }

    dsi_interface =
        (globus_gfs_storage_iface_t *)dlsym(_real_module_handle, DsiInterface);
    if (!dsi_interface)
    {
        loader_log_to_syslog("Failed to find symbol %s in "
                             "libglobus_gridftp_server_hpss_real.so: %s",
                             DsiInterface,
                             dlerror());
        return GLOBUS_FAILURE;
    }

    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY, (char *)DsiName, Module, dsi_interface);

    return GLOBUS_SUCCESS;
}

/*
 * Only allowed an int return, must log errors internally.
 */
int
loader_deactivate(const char *DsiName)
{
    globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, (char *)DsiName);

    globus_module_deactivate(GLOBUS_COMMON_MODULE);

    if (_real_module_handle)
        dlclose(_real_module_handle);

    return GLOBUS_SUCCESS;
}

int
activate(void)
{
    return loader_activate(MODULE_NAME_STRING,
                           "hpss_dsi_iface",
                           // GlobusExtensionMyModule(MODULE_NAME)
                           &MODULE_NAME);
}

int
deactivate(void)
{
    return loader_deactivate(MODULE_NAME_STRING);
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

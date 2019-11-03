/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2015 NCSA.  All rights reserved.
 *
 * Developed by:
 *
 * Storage Enabling Technologies (SET)
 *
 * Nation Center for Supercomputing Applications (NCSA)
 *
 * http://www.ncsa.illinois.edu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the .Software.),
 * to deal with the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 *    + Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimers.
 *
 *    + Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimers in the
 *      documentation and/or other materials provided with the distribution.
 *
 *    + Neither the names of SET, NCSA
 *      nor the names of its contributors may be used to endorse or promote
 *      products derived from this Software without specific prior written
 *      permission.
 *
 * THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS WITH THE SOFTWARE.
 */

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

GlobusExtensionDeclareModule(globus_gridftp_server_hpss_local);

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
    return loader_activate("hpss_local",
                           "hpss_dsi_iface",
                           GlobusExtensionMyModule(globus_gridftp_server_hpss_local));
}

int
deactivate(void)
{
    return loader_deactivate("hpss_local");
}

GlobusExtensionDefineModule(globus_gridftp_server_hpss_local) = {
    "globus_gridftp_server_hpss_local",
    activate,
    deactivate,
    GLOBUS_NULL,
    GLOBUS_NULL,
    &version
};

/*
 * System includes.
 */
#include <dlfcn.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "version.h"

/*
 * This is used to define the debug print statements.
 */
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);


static int local_activate(void);
static int local_deactivate(void);

GlobusExtensionDefineModule(globus_gridftp_server_hpss_local) =
{
	"globus_gridftp_server_hpss_local",
	local_activate,
	local_deactivate,
	GLOBUS_NULL,
	GLOBUS_NULL,
	&local_version
};

static void * _real_module_handle = NULL;

static int
local_activate(void)
{
	int rc;
	char * error = NULL;
	globus_gfs_storage_iface_t * local_dsi_iface = NULL;

	rc = globus_module_activate(GLOBUS_COMMON_MODULE);
	if(rc != GLOBUS_SUCCESS)
		return rc;

	dlerror();
	_real_module_handle = dlopen(/*"/usr/local/gridftp_hpss_dsi/*/"libglobus_gridftp_server_hpss_real_local.so", /*RTLD_LAZY*/RTLD_NOW|RTLD_DEEPBIND);
	error = dlerror();
	assert(_real_module_handle);

	local_dsi_iface = dlsym(_real_module_handle, "local_dsi_iface");
	assert(local_dsi_iface);

	globus_extension_registry_add(
	    GLOBUS_GFS_DSI_REGISTRY,
	    "hpss_local",
	    GlobusExtensionMyModule(globus_gridftp_server_hpss_local),
	    local_dsi_iface);

	GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS,
	    ERROR WARNING TRACE INTERNAL_TRACE INFO STATE INFO_VERBOSE);

	return GLOBUS_SUCCESS;
}

static int
local_deactivate(void)
{
    globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, "hpss_local");

    globus_module_deactivate(GLOBUS_COMMON_MODULE);

	if (_real_module_handle)
		dlclose(_real_module_handle);

    return GLOBUS_SUCCESS;
}


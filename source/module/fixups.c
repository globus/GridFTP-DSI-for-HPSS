/*
 * System includes.
 */
#include <string.h>
#include <stdlib.h>

/*
 * Local includes.
 */
#include "logging.h"
#include "fixups.h"
#include "stat.h"
#include "hpss.h"

globus_result_t
fixup_stat_object(char              * Pathname,
                  globus_result_t     Result, 
                  globus_gfs_stat_t * GFSStat)
{
    if (Result == GLOBUS_SUCCESS)
        return Result;

    globus_gfs_stat_t gfs_lstat_buf;
    globus_result_t result = stat_link(Pathname, &gfs_lstat_buf);
    if (result != GLOBUS_SUCCESS)
        return Result;

    stat_destroy(GFSStat);
    memcpy(GFSStat, &gfs_lstat_buf, sizeof(gfs_lstat_buf));
    return GLOBUS_SUCCESS;
}

char *
_build_path(const char * parent, const char * target)
{
    if (target[0] == '/')
        return strdup(target);

    char * path = malloc(strlen(parent) + strlen(target) + 2);
    sprintf(path, "%s/%s", parent, target);
    return path;
}

globus_result_t
fixup_stat_directory(char              * Pathname,
                     globus_gfs_stat_t * GFSstatArray,
                     uint32_t          * CountOut)
{
    for (int i = (*CountOut)-1; i >= 0; i--)
    {
        if (! S_ISLNK(GFSstatArray[i].mode) )
            continue;

        char * path = _build_path(Pathname, GFSstatArray[i].name);
        globus_gfs_stat_t gfs_stat_buf;
        globus_result_t result = stat_object(path, &gfs_stat_buf);
        free(path);

        // When this fails, we assume this is a broken symbolic link so we
        // drop it from the array and pretend like it isn't here.
        if (result != GLOBUS_SUCCESS)
        {
            gfs_stat_buf = GFSstatArray[i];
            GFSstatArray[i] = GFSstatArray[(*CountOut)-1];
            GFSstatArray[--(*CountOut)] = gfs_stat_buf;
            continue;
        }

         // If this is a good link, we want the targets's attributes saved in
         // the stat buf and record the symlink target.
        gfs_stat_buf.symlink_target = strdup(GFSstatArray[i].symlink_target);
        stat_destroy(&GFSstatArray[i]);
        memcpy(&(GFSstatArray[i]), &gfs_stat_buf, sizeof(globus_gfs_stat_t));
    }
    return GLOBUS_SUCCESS;
}

struct _stat_dir_cb_arg {
    char * Pathname;
    hpss_fileattr_t DirAttrs;
    int DirIsAllBrokenSymlinks;
};

static globus_result_t
_stat_dir_cb_check_links(globus_gfs_stat_t * GFSStatArray,
                         uint32_t            ArrayLength,
                         uint32_t            End,
                         void              * CallbackArg)
{
    struct _stat_dir_cb_arg * cb_arg = CallbackArg;

    // Short-circuit if we previously discovered this to not be the case
    if (cb_arg->DirIsAllBrokenSymlinks != 1)
        return GLOBUS_SUCCESS;

    for (int i = 0; i < ArrayLength; i++)
    {
        if (strcmp(GFSStatArray[i].name, ".") == 0)
            continue;
        if (strcmp(GFSStatArray[i].name, "..") == 0)
            continue;
        if (! S_ISLNK(GFSStatArray[i].mode) )
        {
            cb_arg->DirIsAllBrokenSymlinks = 0;
            return GLOBUS_SUCCESS;
        }

        char * path = _build_path(cb_arg->Pathname, GFSStatArray[i].name);
        globus_gfs_stat_t gfs_stat_buf;
        globus_result_t result = stat_object(path, &gfs_stat_buf);
        free(path);

        if (result == GLOBUS_SUCCESS)
        {
            stat_destroy(&gfs_stat_buf);
            cb_arg->DirIsAllBrokenSymlinks = 0;
            return GLOBUS_SUCCESS;
        }
    }

    return GLOBUS_SUCCESS;
}

static globus_result_t
_stat_dir_cb_rm_links(globus_gfs_stat_t * GFSStatArray,
                      uint32_t            ArrayLength,
                      uint32_t            End,
                      void              * CallbackArg)
{
    struct _stat_dir_cb_arg * cb_arg = CallbackArg;

    for (int i = 0; i < ArrayLength; i++)
    {
        if (! S_ISLNK(GFSStatArray[i].mode) )
            continue;
        int retval = Hpss_UnlinkHandle(&cb_arg->DirAttrs.ObjectHandle,
                                       GFSStatArray[i].name,
                                       NULL);
        if (retval != 0)
            return GlobusGFSErrorSystemError("hpss_UnlinkHandle", -retval);

    }
    return GLOBUS_SUCCESS;
}

globus_result_t
fixup_rmd(char * Pathname, globus_result_t Result)
{
    if (Result == GLOBUS_SUCCESS)
        return Result;

    struct _stat_dir_cb_arg cb_arg;
    cb_arg.Pathname = Pathname;

    int retval = Hpss_FileGetAttributes(Pathname, &cb_arg.DirAttrs);
    if (retval != HPSS_E_NOERROR)
        return GlobusGFSErrorSystemError("hpss_FileGetAttributes", -retval);

    cb_arg.DirIsAllBrokenSymlinks = 1;
    globus_result_t result = stat_directory(Pathname,
                                            _stat_dir_cb_check_links,
                                            &cb_arg);

    if (result != GLOBUS_SUCCESS)
        return result;

    /* If the directory isn't all broken symlinks, return the original error. */
    if (cb_arg.DirIsAllBrokenSymlinks == 0)
        return Result;

    result = stat_directory(Pathname, _stat_dir_cb_rm_links, &cb_arg);
    if (result != GLOBUS_SUCCESS)
        return result;

    retval = Hpss_Rmdir(Pathname);
    if (retval != 0)
        return GlobusGFSErrorSystemError("hpss_Rmdir", -retval);
    return GLOBUS_SUCCESS;
}

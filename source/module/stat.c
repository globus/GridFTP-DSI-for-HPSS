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

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>
#include <hpss_stat.h>
#include <u_signed64.h>

/*
 * Local includes
 */
#include "stat.h"

globus_result_t
stat_translate_stat(char *             Pathname,
                    hpss_stat_t *      HpssStat,
                    globus_gfs_stat_t *GFSStat)
{
    GlobusGFSName(stat_translate_stat);

    GFSStat->mode  = HpssStat->st_mode;
    GFSStat->nlink = HpssStat->st_nlink;
    GFSStat->uid   = HpssStat->st_uid;
    GFSStat->gid   = HpssStat->st_gid;
    GFSStat->dev   = 0;

    GFSStat->atime = HpssStat->hpss_st_atime;
    GFSStat->mtime = HpssStat->hpss_st_mtime;
    GFSStat->ctime = HpssStat->hpss_st_ctime;
    GFSStat->ino   = cast32m(HpssStat->st_ino);
    CONVERT_U64_TO_LONGLONG(HpssStat->st_size, GFSStat->size);

    /* If it is a symbolic link... */
    if (S_ISLNK(HpssStat->st_mode))
    {
        char symlink_target[HPSS_MAX_PATH_NAME];
        /* Read the target. */
        int retval =
            hpss_Readlink(Pathname, symlink_target, sizeof(symlink_target));

        if (retval < 0)
        {
            stat_destroy(GFSStat);
            return GlobusGFSErrorSystemError("hpss_Readlink", -retval);
        }

        /* Copy out the symlink target. */
        GFSStat->symlink_target = globus_libc_strdup(symlink_target);
        if (GFSStat->symlink_target == NULL)
        {
            stat_destroy(GFSStat);
            return GlobusGFSErrorMemory("SymlinkTarget");
        }
    }

    /* Copy out the base name. */
    char *basename = strrchr(Pathname, '/');
    switch (basename == NULL)
    {
    case 0:
        GFSStat->name = globus_libc_strdup(basename + 1);
        break;
    default:
        GFSStat->name = globus_libc_strdup(Pathname);
        break;
    }

    if (!GFSStat->name)
    {
        stat_destroy(GFSStat);
        return GlobusGFSErrorMemory("GFSStat->name");
    }

    return GLOBUS_SUCCESS;
}

/*
 * It is possible to have a broken symbolic link and we don't want
 * that to cause complex operations to fail. So, we'll return
 * symlink information if the link is broken.
 */
globus_result_t
stat_object(char *Pathname, globus_gfs_stat_t *GFSStat)
{
    GlobusGFSName(stat_object);

    memset(GFSStat, 0, sizeof(globus_gfs_stat_t));

    hpss_stat_t hpss_stat_buf;
    int         retval = hpss_Lstat(Pathname, &hpss_stat_buf);
    if (retval)
        return GlobusGFSErrorSystemError("hpss_Lstat", -retval);

    if (S_ISLNK(hpss_stat_buf.st_mode))
    {
        retval = hpss_Stat(Pathname, &hpss_stat_buf);
        if (retval && retval != -ENOENT)
            return GlobusGFSErrorSystemError("hpss_Stat", -retval);
    }

    return stat_translate_stat(Pathname, &hpss_stat_buf, GFSStat);
}

globus_result_t
stat_link(char *Pathname, globus_gfs_stat_t *GFSStat)
{
    GlobusGFSName(stat_link);

    memset(GFSStat, 0, sizeof(globus_gfs_stat_t));

    hpss_stat_t hpss_stat_buf;
    int         retval = hpss_Lstat(Pathname, &hpss_stat_buf);
    if (retval)
        return GlobusGFSErrorSystemError("hpss_Lstat", -retval);

    return stat_translate_stat(Pathname, &hpss_stat_buf, GFSStat);
}

globus_result_t
stat_translate_dir_entry(ns_ObjHandle_t *   ParentObjHandle,
                         ns_DirEntry_t *    DirEntry,
                         globus_gfs_stat_t *GFSStat)
{
    GlobusGFSName(stat_translate_dir_entry);

    GFSStat->mode = 0;
    if (DirEntry->Attrs.UserPerms & NS_PERMS_RD)
        GFSStat->mode |= S_IRUSR;
    if (DirEntry->Attrs.UserPerms & NS_PERMS_WR)
        GFSStat->mode |= S_IWUSR;
    if (DirEntry->Attrs.UserPerms & NS_PERMS_XS)
        GFSStat->mode |= S_IXUSR;
    if (DirEntry->Attrs.GroupPerms & NS_PERMS_RD)
        GFSStat->mode |= S_IRGRP;
    if (DirEntry->Attrs.GroupPerms & NS_PERMS_WR)
        GFSStat->mode |= S_IWGRP;
    if (DirEntry->Attrs.GroupPerms & NS_PERMS_XS)
        GFSStat->mode |= S_IXGRP;
    if (DirEntry->Attrs.OtherPerms & NS_PERMS_RD)
        GFSStat->mode |= S_IROTH;
    if (DirEntry->Attrs.OtherPerms & NS_PERMS_WR)
        GFSStat->mode |= S_IWOTH;
    if (DirEntry->Attrs.OtherPerms & NS_PERMS_XS)
        GFSStat->mode |= S_IXOTH;
    if (DirEntry->Attrs.ModePerms & NS_PERMS_RD)
        GFSStat->mode |= S_ISUID;
    if (DirEntry->Attrs.ModePerms & NS_PERMS_WR)
        GFSStat->mode |= S_ISGID;
    if (DirEntry->Attrs.ModePerms & NS_PERMS_XS)
        GFSStat->mode |= S_ISVTX;

    switch (DirEntry->Attrs.Type)
    {
    case NS_OBJECT_TYPE_FILE:
    case NS_OBJECT_TYPE_HARD_LINK:
        GFSStat->mode |= S_IFREG;
        break;

    case NS_OBJECT_TYPE_DIRECTORY:
    case NS_OBJECT_TYPE_JUNCTION:
    case NS_OBJECT_TYPE_FILESET_ROOT:
        GFSStat->mode |= S_IFDIR;
        break;

    case NS_OBJECT_TYPE_SYM_LINK:
        GFSStat->mode |= S_IFLNK;
        break;
    }

    GFSStat->nlink = DirEntry->Attrs.LinkCount;
    GFSStat->uid   = DirEntry->Attrs.UID;
    GFSStat->gid   = DirEntry->Attrs.GID;
    GFSStat->dev   = 0;

    timestamp_sec_t hpss_atime;
    timestamp_sec_t hpss_mtime;
    timestamp_sec_t hpss_ctime;

    API_ConvertTimeToPosixTime(
        &DirEntry->Attrs, &hpss_atime, &hpss_mtime, &hpss_ctime);

    GFSStat->atime = hpss_atime;
    GFSStat->mtime = hpss_mtime;
    GFSStat->ctime = hpss_ctime;

    GFSStat->ino  = 0; // XXX
    GFSStat->size = DirEntry->Attrs.DataLength;

    GFSStat->name = globus_libc_strdup(DirEntry->Name);
    if (!GFSStat->name)
        return GlobusGFSErrorMemory("GFSStat->name");

    /* If it is a symbolic link... */
    if (DirEntry->Attrs.Type == NS_OBJECT_TYPE_SYM_LINK)
    {
        char symlink_target[HPSS_MAX_PATH_NAME];
        /* Read the target. */
        int retval = hpss_ReadlinkHandle(ParentObjHandle,
                                         DirEntry->Name,
                                         symlink_target,
                                         sizeof(symlink_target),
                                         NULL);

        if (retval < 0)
        {
            stat_destroy(GFSStat);
            return GlobusGFSErrorSystemError("hpss_ReadlinkHandle", -retval);
        }

        /* Copy out the symlink target. */
        GFSStat->symlink_target = globus_libc_strdup(symlink_target);
        if (GFSStat->symlink_target == NULL)
        {
            stat_destroy(GFSStat);
            return GlobusGFSErrorMemory("SymlinkTarget");
        }
    }
    return GLOBUS_SUCCESS;
}

globus_result_t
stat_directory_entries(ns_ObjHandle_t *   ObjHandle,      // IN
                       uint64_t           OffsetIn,       // IN
                       uint32_t           GFSStatCountIn, // IN
                       uint32_t *         End,            // OUT
                       uint64_t *         OffsetOut,      // OUT
                       globus_gfs_stat_t *GFSStatArray,   // OUT
                       uint32_t *         GFSStatCountOut)         // OUT
{
    globus_result_t result;
    int             i;
    int             retval;

    GlobusGFSName(stat_directory_entries);

    ns_DirEntry_t *dir_entry_buffer = NULL;

    memset(GFSStatArray, 0, sizeof(globus_gfs_stat_t) * GFSStatCountIn);

    dir_entry_buffer = malloc(sizeof(ns_DirEntry_t) * GFSStatCountIn);
    if (!dir_entry_buffer)
        return GlobusGFSErrorMemory("ns_DirEntry_t array");

    retval = hpss_ReadAttrsHandle(ObjHandle,
                                  OffsetIn,
                                  NULL,
                                  sizeof(ns_DirEntry_t) * GFSStatCountIn,
                                  TRUE,
                                  End,
                                  OffsetOut,
                                  dir_entry_buffer);
    if (retval < 0)
    {
        free(dir_entry_buffer);
        return GlobusGFSErrorSystemError("hpss_ReadAttrsHandle", -retval);
    }

    *GFSStatCountOut = retval;
    for (i = 0; i < *GFSStatCountOut; i++)
    {
        result = stat_translate_dir_entry(
            ObjHandle, &dir_entry_buffer[i], &GFSStatArray[i]);
        if (result)
        {
            stat_destroy_array(GFSStatArray, i);
            free(dir_entry_buffer);
            return result;
        }
    }

    return GLOBUS_SUCCESS;
}

void
stat_destroy(globus_gfs_stat_t *GFSStat)
{
    if (GFSStat)
    {
        if (GFSStat->symlink_target != NULL)
            free(GFSStat->symlink_target);
        if (GFSStat->name != NULL)
            free(GFSStat->name);
    }
    return;
}

void
stat_destroy_array(globus_gfs_stat_t *GFSStatArray, int Count)
{
    int i;
    for (i = 0; i < Count; i++)
    {
        stat_destroy(&(GFSStatArray[i]));
    }
}

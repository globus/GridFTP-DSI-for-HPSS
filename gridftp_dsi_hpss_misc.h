/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2012 NCSA.  All rights reserved.
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

#ifndef GRIDFTP_DSI_HPSS_MISC_H
#define GRIDFTP_DSI_HPSS_MISC_H

/*
 * System includes.
 */
#include <grp.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>
#include <globus_common.h>
#include <globus_debug.h>

/*
 * HPSS includes.
 */
#include <hpss_api.h>

/*
 * This is used to define the debug print statements.
 */
GlobusDebugDeclare(GLOBUS_GRIDFTP_SERVER_HPSS);

#define GlobusGFSHpssDebugPrintf(level, message)                             \
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS, level, message)

#define GlobusGFSHpssDebugEnter()                                            \
    GlobusGFSHpssDebugPrintf(                                                \
        GLOBUS_GFS_DEBUG_TRACE,                                              \
        ("[%s] Entering\n", _gfs_name))

#define GlobusGFSHpssDebugExit()                                             \
    GlobusGFSHpssDebugPrintf(                                                \
        GLOBUS_GFS_DEBUG_TRACE,                                              \
        ("[%s] Exiting\n", _gfs_name))

#define GlobusGFSHpssDebugExitWithError()                                    \
    GlobusGFSHpssDebugPrintf(                                                \
        GLOBUS_GFS_DEBUG_TRACE,                                              \
        ("[%s] Exiting with error\n", _gfs_name))


globus_result_t
misc_gfs_stat(char *  Path,
              globus_bool_t        FileOnly,
              globus_bool_t        UseSymlinkInfo,
              globus_bool_t        IncludePathStat,
              globus_gfs_stat_t ** GfsStatArray,
              int               *  GfsStatCount);

void
misc_destroy_gfs_stat_array(globus_gfs_stat_t * GfsStatArray,
                            int                 GfsStatCount);

globus_result_t
misc_file_archived(char          * Path,
                   globus_bool_t * Archived,
                   globus_bool_t * TapeOnly);

globus_result_t
misc_get_file_size(char * Path, globus_off_t * FileSize);


/*
 * Passwd file translations.
 */
globus_result_t
misc_username_to_home(char * UserName, char ** HomeDirectory);

globus_result_t
misc_username_to_uid(char * UserName, int * Uid);

globus_result_t
misc_uid_to_username(int Uid, char ** UserName);


globus_result_t
misc_groupname_to_gid(char * GroupName, gid_t * Gid);

globus_result_t
misc_gid_to_groupname(gid_t Gid, char  ** GroupName);

globus_bool_t
misc_is_user_in_group(char * UserName, char * GroupName);

/*
 * Helper to release memory associated with errors.
 */
void
misc_destroy_result(globus_result_t Result);

char *
misc_strndup(char * String, int Length);

#endif /* GRIDFTP_DSI_HPSS_MISC_H */

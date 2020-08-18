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
#ifndef HPSS_DSI_COMMANDS_H
#define HPSS_DSI_COMMANDS_H

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

enum
{
    GLOBUS_GFS_HPSS_CMD_SITE_STAGE = GLOBUS_GFS_MIN_CUSTOM_CMD,
};

globus_result_t
commands_init(globus_gfs_operation_t Operation);

typedef void (*commands_callback)(globus_gfs_operation_t Operation,
                                  globus_result_t        Result,
                                  char *                 CommandResponse);

globus_result_t
commands_mkdir(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_rmdir(char * Pathname);

globus_result_t
commands_unlink(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_rename(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_chmod(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_chgrp(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_utime(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_symlink(globus_gfs_command_info_t *CommandInfo);

globus_result_t
commands_truncate(globus_gfs_command_info_t *CommandInfo);

#endif /* HPSS_DSI_COMMANDS_H */

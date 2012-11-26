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

#ifndef GLOBUS_GRIDFTP_SERVER_HPSS_CONFIG_H
#define GLOBUS_GRIDFTP_SERVER_HPSS_CONFIG_H

/*
 * Globus includes.
 */
#include <globus_common.h>

#define DEFAULT_CONFIG_FILE   "/var/hpss/etc/gridftp.conf"

typedef struct {
	char * LoginName;
	char * KeytabFile;
	char * ProjectFile;
	char * FamilyFile;
	char * CosFile;
	char * AdminList;
} config_t;

globus_result_t
globus_l_gfs_hpss_config_init(char * ConfigFile);

void
globus_l_gfs_hpss_config_destroy();

char *
globus_l_gfs_hpss_config_get_login_name();

char *
globus_l_gfs_hpss_config_get_keytab();

/*
 *  Family can be an integer or the name.
 */
globus_bool_t
globus_l_gfs_hpss_config_can_user_use_family(char * UserName,
                                             char * Family);

int
globus_l_gfs_hpss_config_get_family_id(char * Family);

char *
globus_l_gfs_hpss_config_get_family_name(char * Family);

char *
globus_l_gfs_hpss_config_get_my_families(char * UserName);


/*
 * Cos can be an integer or the name.
 */
globus_bool_t
globus_l_gfs_hpss_config_can_user_use_cos(char * UserName,
                                          char * Cos);

int
globus_l_gfs_hpss_config_get_cos_id(char * Cos);

globus_bool_t
globus_l_gfs_hpss_config_is_user_admin(char * UserName);

char *
globus_l_gfs_hpss_config_get_my_cos(char * UserName);
#endif /* GLOBUS_GRIDFTP_SERVER_HPSS_CONFIG_H */

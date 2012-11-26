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

#ifndef GRIDFTP_DSI_HPSS_CONFIG_H
#define GRIDFTP_DSI_HPSS_CONFIG_H

/*
 * Globus includes.
 */
#include <globus_common.h>

#define DEFAULT_CONFIG_FILE   "/var/hpss/etc/gridftp.conf"

#define CONFIG_NO_FAMILY_ID -1
#define CONFIG_NO_COS_ID    -1

typedef struct config_handle config_handle_t;

/*
 * ConfigFile is dup'ed.
 */
globus_result_t
config_init(char * ConfigFile, config_handle_t ** ConfigHandle);

void
config_destroy(config_handle_t * ConfigHandle);

/*
 * The returned value is not a dup.
 */
char *
config_get_login_name(config_handle_t * ConfigHandle);

/*
 * The returned value is not a dup.
 */
char *
config_get_keytab_file(config_handle_t * ConfigHandle);

/*
 * Cos is the name.
 */
int
config_get_cos_id(config_handle_t * ConfigHandle, char * Cos);

/*
 * The returned value is not dup'ed.
 */
char *
config_get_cos_name(config_handle_t * ConfigHandle, int CosID);

globus_bool_t
config_user_use_cos(config_handle_t * ConfigHandle, char * UserName, int CosID);

globus_result_t
config_get_user_cos_list(config_handle_t *   ConfigHandle, 
                         char            *   UserName, 
                         char            *** CosList);

/*
 * Family is the name.
 */
int
config_get_family_id(config_handle_t * ConfigHandle, char * Family);

/*
 * The returned value is not dup'ed.
 */
char *
config_get_family_name(config_handle_t * ConfigHandle, int FamilyID);

globus_bool_t
config_user_use_family(config_handle_t * ConfigHandle, char * UserName, int FamilyID);

globus_result_t
config_get_user_family_list(config_handle_t *   ConfigHandle, 
                            char            *   UserName, 
                            char            *** FamilyList);

globus_bool_t
config_user_is_admin(config_handle_t * ConfigHandle, char * UserName);

#endif /* GRIDFTP_DSI_HPSS_CONFIG_H */

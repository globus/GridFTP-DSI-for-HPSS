/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2012-2014 NCSA.  All rights reserved.
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

#ifndef GRIDFTP_DSI_HPSS_SESSION_H
#define GRIDFTP_DSI_HPSS_SESSION_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/* Define our session handle before including other local files. */
typedef struct session_handle session_handle_t;

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_commands.h"
#include "gridftp_dsi_hpss_msg.h"

#define SESSION_NO_FAMILY_ID -1
#define SESSION_NO_COS_ID    -1

globus_result_t
session_init(globus_gfs_session_info_t *  SessionInfo,
             session_handle_t          ** SessionHandle);

void
session_destroy(session_handle_t * SessionHandle);

globus_result_t
session_authenticate(session_handle_t * SessionHandle);

/*
 * A copy of the pointer to our session_info is returned; 
 * nothing is dup'ed.
 */
globus_gfs_session_info_t *
session_get_session_info(session_handle_t * SessionHandle);

/*
 * Returned string is not a dup, original is sent.
 */
char *
session_get_username(session_handle_t * SessionHandle);

void
session_pref_set_family_id(session_handle_t * SessionHandle,
                           int                FamilyID);

int
session_pref_get_family_id(session_handle_t * SessionHandle);

void
session_pref_set_cos_id(session_handle_t * SessionHandle,
                        int                CosID);

int
session_pref_get_cos_id(session_handle_t * SessionHandle);

/*
 * Translates Cos to the id. Cos can be the name or the id.
 * Returns SESSION_NO_COS_ID if cos is not found.
 */
int
session_get_cos_id(session_handle_t * SessionHandle, char * Cos);

globus_bool_t
session_can_user_use_cos(session_handle_t * SessionHandle, int CosID);

/*
 * comma-separated list of cos names. Null if empty. CosList
 * must be free'd by the caller.
 */
globus_result_t
session_get_user_cos_list(session_handle_t *  SessionHandle,
                          char             ** CosList);

/*
 * Translates Family to the id. Cos can be the name or the id.
 * Returns SESSION_NO_FAMILY_ID if family is not found.
 */
int
session_get_family_id(session_handle_t * SessionHandle, char * Family);

globus_bool_t
session_can_user_use_family(session_handle_t * SessionHandle, int FamilyID);

/*
 * comma-separated list of family names. Null if empty. FamilyList
 * must be free'd by the caller.
 */
globus_result_t
session_get_user_family_list(session_handle_t *  SessionHandle,
                             char             ** FamilyList);

#endif /* GRIDFTP_DSI_HPSS_SESSION_H */

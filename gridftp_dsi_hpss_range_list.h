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

#ifndef GRIDFTP_DSI_HPSS_RANGE_LIST_H
#define GRIDFTP_DSI_HPSS_RANGE_LIST_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

#define RANGE_LIST_MAX_LENGTH (0x7FFFFFFFFFFFFFFFULL)

typedef struct range_list range_list_t;

globus_result_t
range_list_init(range_list_t ** RangeList);

void
range_list_destroy(range_list_t * RangeList);

/*
 * Ignore lengths of 0.
 */
globus_result_t
range_list_insert(range_list_t * RangeList,
                  globus_off_t   Offset,
                  globus_off_t   Length);

globus_bool_t
range_list_empty(range_list_t * RangeList);

void
range_list_delete(range_list_t * RangeList,
                  globus_off_t   Offset,
                  globus_off_t   Length);

void
range_list_peek(range_list_t * RangeList,
                globus_off_t * Offset,
                globus_off_t * Length);

void
range_list_pop(range_list_t * RangeList,
               globus_off_t * Offset,
               globus_off_t * Length);

globus_off_t
range_list_get_transfer_offset(range_list_t * FileRangeList, 
                               globus_off_t   FileOffset);

globus_off_t
range_list_get_file_offset(range_list_t * FileRangeList, 
                           globus_off_t   TransferOffset);

globus_result_t
range_list_fill_stor_range(range_list_t               * RangeList,
                           globus_gfs_transfer_info_t * TransferInfo);

globus_result_t
range_list_fill_retr_range(range_list_t               * RangeList,
                           globus_gfs_transfer_info_t * TransferInfo);

globus_result_t
range_list_fill_cksm_range(range_list_t              * RangeList,
                           globus_gfs_command_info_t * CommandInfo);

#endif /* GRIDFTP_DSI_HPSS_RANGE_LIST_H */

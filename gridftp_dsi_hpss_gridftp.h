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
#ifndef GRIDFTP_DSI_HPSS_GRIDFTP_H
#define GRIDFTP_DSI_HPSS_GRIDFTP_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_transfer_data.h"
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_misc.h"

typedef struct gridftp gridftp_t;

typedef enum {
	GRIDFTP_OP_TYPE_RETR,
	GRIDFTP_OP_TYPE_STOR,
} gridftp_op_type_t;

/*
 * This implementation should only callback once.
 */
typedef void
(*gridftp_eof_callback_t)(void          * CallbackArg,
                          globus_result_t Result);

typedef void
(*gridftp_buffer_pass_t) (void       * CallbackArg,
                          char       * Buffer,
                          globus_off_t Offset,
                          globus_off_t Length);

globus_result_t
gridftp_init(gridftp_op_type_t             OpType,
             globus_gfs_operation_t        Operation,
             globus_gfs_transfer_info_t *  TransferInfo,
             buffer_handle_t            *  BufferHandle,
             msg_handle_t               *  MsgHandle,
             gridftp_eof_callback_t        EofCallbackFunc,
             void                       *  EofCallbackArg,
             gridftp_t                  ** GridFTP);

void
gridftp_set_buffer_pass_func(gridftp_t              * GridFTP,
                              gridftp_buffer_pass_t   BufferPassFunc,
                              void                  * BufferPassArg);

void
gridftp_destroy(gridftp_t * GridFTP);

void
gridftp_buffer(void         * CallbackArg,
               char         * Buffer,
               globus_off_t   Offset,
               globus_off_t   Length);

void
gridftp_flush(gridftp_t * GridFTP);

void
gridftp_stop(gridftp_t * GridFTP);


#endif /* GRIDFTP_DSI_HPSS_GRIDFTP_H */

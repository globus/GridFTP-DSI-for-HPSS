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

#ifndef GRIDFTP_DSI_HPSS_MSG_H
#define GRIDFTP_DSI_HPSS_MSG_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

typedef struct msg_handle msg_handle_t;

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_msg_types.h"

typedef enum {
	MSG_ID_PIO_DATA = 0,
	MSG_ID_MIN_ID   = 0, /* Listed second so debuggers don't use it. */
	MSG_ID_PIO_CONTROL,
	MSG_ID_IPC_CONTROL,
	MSG_ID_TRANSFER_DATA,
	MSG_ID_TRANSFER_CONTROL,
	MSG_ID_DATA_RANGES,
	MSG_ID_GRIDFTP,
	MSG_ID_MAIN,
	MSG_ID_MAX_ID,
} msg_id_t;

typedef globus_result_t (*msg_recv_func_t) (void     * CallbackArg,
                                            int        NodeIndex,
                                            msg_id_t   DestinationID,
                                            msg_id_t   SourceID,
                                            int        MsgType,
                                            int        MsgLen,
                                            void     * Msg);

globus_result_t
msg_init(msg_handle_t ** MsgHandle);

void
msg_destroy(msg_handle_t * MsgHandle);

void
msg_register_recv(msg_handle_t    * MsgHandle,
                  msg_id_t          DestinationID,
                  msg_recv_func_t   MsgRecvFunc,
                  void            * CallbackArg);

void
msg_unregister_recv(msg_handle_t * MsgHandle,
                    msg_id_t       DestinationID);

/*
 * We pass DestinationID for routers like IPC.
 * We pass SourceID so receipents can decipher the message.
 *
 * NodeIndex is zero except for when the control side sends
 * messages for striped transfers.
 */
globus_result_t
msg_send(msg_handle_t * MsgHandle,
         int            NodeIndex,
         msg_id_t       DestinationID,
         msg_id_t       SourceID,
         int            MsgType,
         int            MsgLen,
         void         * Msg);

#endif /* GRIDFTP_DSI_HPSS_MSG_H */

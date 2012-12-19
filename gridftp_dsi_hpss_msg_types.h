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

#ifndef GRIDFTP_DSI_HPSS_MSG_TYPES_H
#define GRIDFTP_DSI_HPSS_MSG_TYPES_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*******************************************
 *
 * THIS IS THE NEW TYPE-BASED MSG INTERFACE
 *
 ******************************************/

typedef enum {
	MSG_TYPE_RANGE_COMPLETE,
	MSG_TYPE_RANGE_RECEIVED,
	MSG_TYPE_MAX,
} msg_type_t;

/* MSG_TYPE_RANGES_COMPLETE */
typedef struct {
	globus_off_t Offset;
	globus_off_t Length;
} msg_range_complete_t;

/* MSG_TYPE_RANGE_RECEIVED */
typedef struct {
	globus_off_t Offset;
	globus_off_t Length;
} msg_range_received_t;

typedef int msg_register_id_t;

#define MSG_REGISTER_ID_NONE ((msg_register_id_t)(-1))

typedef globus_result_t (*msg_recv_msg_type_func_t) (void       * CallbackArg,
                                                     msg_type_t   MsgType,
                                                     int          MsgLen,
                                                     void       * Msg);

msg_register_id_t
msg_register_for_type(msg_handle_t           * MsgHandle,
                      msg_type_t               MsgType,
                      msg_recv_msg_type_func_t MsgRecvMsgTypeFunc,
                      void                   * CallbackArg);

void
msg_unregister_for_type(msg_handle_t      * MsgHandle,
                        msg_type_t          MsgType,
                        msg_register_id_t   MsgRegisterID);

globus_result_t
msg_send_by_type(msg_handle_t * MsgHandle,
                 msg_type_t     MsgType,
                 int            MsgLen,
                 void         * Msg);

#endif /* GRIDFTP_DSI_HPSS_MSG_TYPES_H */

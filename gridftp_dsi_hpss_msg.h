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
 * IDs that identify individual components.
 */
typedef enum {
	MSG_COMP_ID_NONE                      = 0x0000,
	MSG_COMP_ID_TRANSFER_CONTROL          = 0x0001,
	MSG_COMP_ID_TRANSFER_CONTROL_PIO      = 0x0002,
	MSG_COMP_ID_TRANSFER_DATA             = 0x0004,
	MSG_COMP_ID_TRANSFER_DATA_GRIDFTP     = 0x0008,
	MSG_COMP_ID_TRANSFER_DATA_RANGES      = 0x0010,
	MSG_COMP_ID_TRANSFER_DATA_CHECKSUM    = 0x0020,
	MSG_COMP_ID_TRANSFER_DATA_PIO         = 0x0040,
	MSG_COMP_ID_IPC_CONTROL               = 0x0080,
	MSG_COMP_ID_IPC_DATA                  = 0x0100,
	MSG_COMP_ID_MASK                      = 0x01FF,

	/*
	 * Keep this out of the mask.
	 */
	MSG_COMP_ID_ANY                       = 0x0200,
} msg_comp_id_t;

#define MSG_COMP_ID_COUNT (9)

typedef struct msg_register_id * msg_register_id_t;
#define MSG_REGISTER_ID_NONE NULL

/*
 * Msg will be a duplicate of the original but the receiver should
 * not free it; the msg code will do that for your.
 */
typedef void (*msg_recv_func_t) (void          * CallbackArg,
                                 msg_comp_id_t   DstMsgCompID,
                                 msg_comp_id_t   SrcMsgCompID,
                                 int             MsgType,
                                 int             MsgLen,
                                 void          * Msg);

/*
 * Initialize the message handle. The handle is shared by
 * all components.
 */
globus_result_t
msg_init(msg_handle_t ** MsgHandle);

/*
 * Destroy the message handle.
 */
void
msg_destroy(msg_handle_t * MsgHandle);

/*
 * Caller will receive messages that:
 *   1) Sender specifies SrcMsgCompID in SrcMsgCompIDs and DstMsgCompID is MSG_COMP_ID_ANY
 *   2) Sender specifies DstMsgCompID in DstMsgCompIDs
 *
 * Either SrcMsgCompIDs or DstMsgCompIDs can be MSG_COMP_ID_NONE
 */
globus_result_t
msg_register(msg_handle_t      * MsgHandle,
             msg_comp_id_t       SrcMsgCompIDs,
             msg_comp_id_t       DstMsgCompIDs,
             msg_recv_func_t     MsgRecvFunc,
             void              * MsgRecvFuncArg,
             msg_register_id_t * MsgRegisterID);

/*
 * No longer receive messages.
 */
void
msg_unregister(msg_handle_t      * MsgHandle,
               msg_register_id_t   MsgRegisterID);


/*
 * Caller must give a valid SrcMsgCompID (not ANY or NONE). Message will
 * go to handler that:
 *  1) Specified SrcMsgCompID in SrcMsgCompIDs if DstMsgCompIDs is ANY
 *  2) Specified DstMsgCompID in DstMsgCompIDs
 *
 * Msg must not contain pointers unless it points to an external object.
 * msg_send() will duplicate Msg for each recipient.
 */
globus_result_t
msg_send(msg_handle_t * MsgHandle,
         msg_comp_id_t  DstMsgCompIDs, /* Can be mask of multiple destinations. */
         msg_comp_id_t  SrcMsgCompID,
         int            MsgType,
         int            MsgLen,
         void         * Msg);

#endif /* GRIDFTP_DSI_HPSS_MSG_H */

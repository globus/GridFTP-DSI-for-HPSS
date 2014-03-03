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

#ifndef GRIDFTP_DSI_HPSS_PIO_CONTROL_H
#define GRIDFTP_DSI_HPSS_PIO_CONTROL_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_api.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_range_list.h"
#include "gridftp_dsi_hpss_session.h"
#include "gridftp_dsi_hpss_msg.h"

typedef struct pio_control pio_control_t;

typedef enum {
    /*
     * PIO_CONTROL_MSG_TYPE_STRIPE_GROUP:
     *   Sent to MSG_ID_PIO_DATA whenever a new stripe group
     *   is created. Send this message just before this node
     *   should call hpss_PIORegister(). (set index first)
     *   MsgLen: sizeof stripe group buffer
     *   Msg: stripe group buffer
     */
	PIO_CONTROL_MSG_TYPE_STRIPE_GROUP = 1,

    /*
     * PIO_CONTROL_MSG_TYPE_STRIPE_INDEX:
     *   Sent to MSG_ID_PIO_DATA whenever a new stripe group
     *   is created to indicate the stripe group index
     *   MsgLen: length of char buffer
     *   Msg: char buffer with index value
     */
	PIO_CONTROL_MSG_TYPE_STRIPE_INDEX,
} pio_control_msg_type_t;

typedef void
(*pio_control_transfer_range_callback_t)(void          * CallbackArg,
                                         globus_result_t Result);

globus_result_t
pio_control_stor_init(msg_handle_t               *  MsgHandle,
                      session_handle_t           *  Session,
                      globus_gfs_transfer_info_t *  TransferInfo,
                      pio_control_t              ** PioControl);

globus_result_t
pio_control_retr_init(msg_handle_t               *  MsgHandle,
                      globus_gfs_transfer_info_t *  TransferInfo,
                      pio_control_t              ** PioControl);

globus_result_t
pio_control_cksm_init(msg_handle_t              *  MsgHandle,
                      globus_gfs_command_info_t *  CommandInfo,
                      pio_control_t             ** PioControl);

void
pio_control_destroy(pio_control_t * PioControl);

void
pio_control_transfer_ranges(pio_control_t                         * PioControl,
                            unsigned32                              ClntStripeWidth,
                            globus_off_t                            StripeBlockSize,
                            range_list_t                          * RangeList,
                            pio_control_transfer_range_callback_t   Callback,
                            void                                  * CallbackArg);

#endif /* GRIDFTP_DSI_HPSS_PIO_CONTROL_H */

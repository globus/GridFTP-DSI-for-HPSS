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

#ifndef GRIDFTP_DSI_HPSS_TRANSFER_DATA_H
#define GRIDFTP_DSI_HPSS_TRANSFER_DATA_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_pio_control.h"
#include "gridftp_dsi_hpss_msg.h"

typedef struct transfer_data transfer_data_t;

typedef enum {
	TRANSFER_DATA_MSG_TYPE_READY = 1,
} transfer_data_msg_type_t;

globus_result_t
transfer_data_stor_init(msg_handle_t               *  MsgHandle,
                        globus_gfs_operation_t        Operation,
                        globus_gfs_transfer_info_t *  TransferInfo,
                        transfer_data_t            ** TransferData);

globus_result_t
transfer_data_retr_init(msg_handle_t               *  MsgHandle,
                        globus_gfs_operation_t        Operation,
                        globus_gfs_transfer_info_t *  TransferInfo,
                        transfer_data_t            ** TransferData);

globus_result_t
transfer_data_cksm_init(msg_handle_t              *  MsgHandle,
                        globus_gfs_operation_t       Operation,
                        globus_gfs_command_info_t *  CommandInfo,
                        transfer_data_t           ** TransferData);

void
transfer_data_destroy(transfer_data_t * TransferData);

globus_result_t
transfer_data_run(transfer_data_t * TransferData);

globus_result_t
transfer_data_checksum(transfer_data_t *  TransferData,
                       char            ** Checksum);

#endif /* GRIDFTP_DSI_HPSS_TRANSFER_DATA_H */

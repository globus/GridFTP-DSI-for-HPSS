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

#ifndef GRIDFTP_DSI_HPSS_PIO_DATA
#define GRIDFTP_DSI_HPSS_PIO_DATA

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_transfer_data.h"
#include "gridftp_dsi_hpss_pio_control.h"
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_msg.h"

typedef struct pio_data pio_data_t;

typedef struct {
	globus_off_t Offset;
	globus_off_t Length;
} pio_data_bytes_written_t;

typedef enum {
	/*
	 * PIO_DATA_MSG_TYPE_BYTES_WRITTEN:
	 *   Indicates that the range has been written to HPSS
	 *   MsgLen:sizeof(pio_data_bytes_written_t)
	 *   Msg:pio_data_bytes_written_t
	 */
	PIO_DATA_MSG_TYPE_BYTES_WRITTEN,
} pio_data_msg_type_t;

/* XXX move these to buffer.h */
typedef void
(*pio_data_buffer_pass_t)(void       * CallbackArg,
                          char       * Buffer,
                          globus_off_t Offset,
                          globus_off_t Length);

/*
 * This implementation should only callback once.
 */
typedef void
(*pio_data_eof_callback_t) (void          * CallbackArg,
                            globus_result_t Result);


globus_result_t
pio_data_stor_init(buffer_handle_t         *  BufferHandle,
                   msg_handle_t            *  MsgHandle,
                   pio_data_eof_callback_t    EofCallbackFunc,
                   void                    *  EofCallbackArg,
                   pio_data_t              ** PioData);

globus_result_t
pio_data_retr_init(buffer_handle_t            *  BufferHandle,
                   msg_handle_t               *  MsgHandle,
                   globus_gfs_transfer_info_t *  TransferInfo,
                   pio_data_eof_callback_t       EofCallbackFunc,
                   void                       *  EofCallbackArg,
                   pio_data_t                 ** PioData);

globus_result_t
pio_data_cksm_init(buffer_handle_t            *  BufferHandle,
                   msg_handle_t               *  MsgHandle,
                   globus_gfs_command_info_t  *  CommandInfo,
                   pio_data_eof_callback_t       EofCallbackFunc,
                   void                       *  EofCallbackArg,
                   pio_data_t                 ** PioData);

void
pio_data_set_buffer_pass_func(pio_data_t             *  PioData,
                              pio_data_buffer_pass_t    BufferPassFunc,
                              void                   *  BufferPassArg);

void
pio_data_destroy(pio_data_t * PioData);

void
pio_data_buffer(void         * CallbackArg,
                char         * Buffer,
                globus_off_t   Offset,
                globus_off_t   Length);

void
pio_data_flush(pio_data_t * PioData);

void
pio_data_stop(pio_data_t * PioData);

#endif /* GRIDFTP_DSI_HPSS_PIO_DATA */

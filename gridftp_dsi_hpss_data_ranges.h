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
#ifndef GRIDFTP_DSI_HPSS_SEND_RANGES_H
#define GRIDFTP_DSI_HPSS_SEND_RANGES_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_msg.h"

typedef struct data_ranges data_ranges_t;

typedef enum {
	DATA_RANGE_MODE_RANGE_COMPLETE     = 0x1,
	DATA_RANGE_MODE_RANGE_RECEIVED     = 0x2,
} data_ranges_mode_t;

/*
 * This implementation should only callback once.
 */
typedef void
(*data_ranges_eof_callback_t) (void          * CallbackArg,
                               globus_result_t Result);

typedef void
(*data_ranges_buffer_pass_t) (void       * CallbackArg,
                              char       * Buffer,
                              globus_off_t Offset,
                              globus_off_t Length);

globus_result_t
data_ranges_init(data_ranges_mode_t    Mode,
                 msg_handle_t       *  MsgHandle,
                 data_ranges_t      ** SendRanges);

void
data_ranges_destroy(data_ranges_t * SendRanges);

void
data_ranges_buffer(void         * CallbackArg,
                   char         * Buffer,
                   globus_off_t   Offset,
                   globus_off_t   Length);

void
data_ranges_flush(data_ranges_t * SendRanges);

void
data_ranges_stop(data_ranges_t * SendRanges);

void
data_ranges_set_buffer_pass_func(data_ranges_t             * SendRanges,
                                 data_ranges_buffer_pass_t   BufferPassFunc,
                                 void                      * BufferPassArg);

#endif /* GRIDFTP_DSI_HPSS_SEND_RANGES_H */

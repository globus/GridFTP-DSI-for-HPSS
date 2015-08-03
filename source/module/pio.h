/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2015 NCSA.  All rights reserved.
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
#ifndef HPSS_DSI_PIO_H
#define HPSS_DSI_PIO_H

/*
 * System includes
 */
#include <pthread.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>

typedef int
(*pio_data_callout)(char     * Buffer,
                    uint32_t * Length, /* IN / OUT */
                    uint64_t   Offset,
                    void     * CallbackArg);

/*
 * Called as each range is completed. Either set Eot to 0 to signal
 * the transfer is complete or set Eot to 1 and set Offset and Length
 * to the new range to transfer.
 */
typedef void
(*pio_range_complete_callback) (globus_off_t * Offset,
                                globus_off_t * Length,
                                int          * Eot,
                                void         * UserArg);

typedef void
(*pio_transfer_complete_callback) (globus_result_t Result,
                                   void          * UserArg);

//typedef enum {
//	PIO_OP_RETR,
//	PIO_OP_STOR,
//	PIO_OP_CKSM,
//} pio_op_type_t;

typedef struct {
	int           FD;
	char        * Buffer;
	uint32_t      BlockSize;
	uint64_t      InitialOffset;
	uint64_t      InitialLength;

	pio_data_callout               DataCO;
	pio_range_complete_callback    RngCmpltCB;
	pio_transfer_complete_callback XferCmpltCB;
	void                         * UserArg;

	globus_result_t CoordinatorResult;
	hpss_pio_grp_t  CoordinatorSG;
	hpss_pio_grp_t  ParticipantSG;
} pio_t;
    
/* Don't call for zero-length transfers. */
globus_result_t
pio_start(hpss_pio_operation_t           PioOpType,
          int                            FD,
          int                            FileStripeWidth,
          uint32_t                       BlockSize,
          globus_off_t                   Offset,
          globus_off_t                   Length,
          pio_data_callout               Callout,
          pio_range_complete_callback    RngCmpltCB,
          pio_transfer_complete_callback XferCmpltCB,
          void                         * UserArg);

globus_result_t
pio_end();

#endif /* HPSS_DSI_PIO_H */

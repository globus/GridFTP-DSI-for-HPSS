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
(*pio_write_callout)(char     * Buffer,
                     uint32_t * Length, /* IN / OUT */
                     uint64_t   Offset,
                     void     * CallbackArg);

typedef void
(*pio_completion_callback) (globus_result_t Result, void * UserArg);

typedef struct {
	int           FD;
	uint32_t      BlockSize;
	uint64_t      FileSize;
	char        * Buffer;

	pio_write_callout       WriteCO;
	pio_completion_callback CompletionCB;
	void                  * UserArg;

	globus_result_t CoordinatorResult;
	hpss_pio_grp_t  CoordinatorSG;
	hpss_pio_grp_t  ParticipantSG;
} pio_t;
    

globus_result_t
pio_start(int                     FD,
          int                     FileStripeWidth,
          uint32_t                BlockSize,
          uint64_t                FileSize,
          pio_write_callout       Callout,
          pio_completion_callback CompletionCB,
          void                  * UserArg);

#endif /* HPSS_DSI_PIO_H */

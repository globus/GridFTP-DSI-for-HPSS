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

#ifndef GRIDFTP_DSI_HPSS_LIST_H
#define GRIDFTP_DSI_HPSS_LIST_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

typedef struct list list_t;

typedef enum {
	LIST_ITERATE_CONTINUE = 1,
	LIST_ITERATE_DONE = 2,
	LIST_ITERATE_REMOVE = 4,
} list_iterate_t;

typedef list_iterate_t (*iterate_func_t) (void * Data, void * CallbackArg);

globus_result_t
list_init(list_t ** List);

void
list_destroy(list_t * List);

globus_result_t
list_insert(list_t * List,
            void   * Data);

globus_result_t
list_insert_at_end(list_t * List,
                   void   * Data);

globus_result_t
list_insert_before(list_t * List,
                   void   * NewData,
                   void   * ExistingData);

void
list_move_to_end(list_t * List,
                 void   * Data);

void
list_move_before(list_t * List,
                 void   * DataToMove,
                 void   * NextData);

void *
list_remove_head(list_t  * List);

void *
list_peek_head(list_t * List);

void
list_iterate(list_t         * List,
             iterate_func_t   SearchFunc,
             void           * CallbackArg);

#endif /* GRIDFTP_DSI_HPSS_LIST_H */

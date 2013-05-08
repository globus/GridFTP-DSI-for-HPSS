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
#ifndef GRIDFTP_DSI_HPSS_HPSS_BUFFER_H
#define GRIDFTP_DSI_HPSS_HPSS_BUFFER_H

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>
#include <globus_list.h>

typedef struct buffer_handle buffer_handle_t;
typedef int buffer_priv_id_t;

globus_result_t
buffer_init(int                BufferAllocSize,
            buffer_handle_t ** BufferHandle);

buffer_priv_id_t
buffer_create_private_list(buffer_handle_t * BufferHandle);

void
buffer_destroy(buffer_handle_t * BufferHandle);

int
buffer_get_alloc_size(buffer_handle_t * BufferHandle);

/* Mark inuse. */
globus_result_t
buffer_allocate_buffer(buffer_handle_t *  BufferHandle,
                       buffer_priv_id_t   PrivateID,
                       char            ** Buffer,
                       globus_off_t    *  BufferLength);

/* Mark inuse. */
void
buffer_get_free_buffer(buffer_handle_t *  BufferHandle,
                       buffer_priv_id_t   PrivateID,
                       char            ** Buffer,
                       globus_off_t    *  BufferLength);

/* Mark not inuse. */
void
buffer_store_free_buffer(buffer_handle_t  * BufferHandle,
                         buffer_priv_id_t   PrivateID,
                         char             * Buffer);

/* Mark inuse. */
void
buffer_get_ready_buffer(buffer_handle_t  *  BufferHandle,
                        buffer_priv_id_t    PrivateID,
                        char             ** Buffer,
                        globus_off_t        Offset,
                        globus_off_t     *  Length);

/* XXX Deprecated. */
void
buffer_get_next_ready_buffer(buffer_handle_t  *  BufferHandle,
                             buffer_priv_id_t    PrivateID,
                             char             ** Buffer,
                             globus_off_t     *  Offset,
                             globus_off_t     *  Length);

/* Mark not inuse. */
void
buffer_store_ready_buffer(buffer_handle_t  * BufferHandle,
                          buffer_priv_id_t   PrivateID,
                          char             * Buffer,
                          globus_off_t       Offset,
                          globus_off_t       Length);

void
buffer_set_buffer_ready(buffer_handle_t  * BufferHandle,
                        buffer_priv_id_t   PrivateID,
                        char             * Buffer,
                        globus_off_t       Offset,
                        globus_off_t       Length);

void
buffer_set_buffer_free(buffer_handle_t  * BufferHandle,
                       buffer_priv_id_t   PrivateID,
                       char             * Buffer,
                       globus_off_t     * Length);

void
buffer_set_private_id(buffer_handle_t  * BufferHandle,
                      buffer_priv_id_t   PrivateID,
                      char             * Buffer);

void
buffer_flag_buffer(buffer_handle_t  * BufferHandle,
                   buffer_priv_id_t   PrivateID,
                   char             * Buffer);

void
buffer_get_flagged_buffer(buffer_handle_t  *  BufferHandle,
                          buffer_priv_id_t    PrivateID,
                          char             ** Buffer,
                          globus_off_t     *  Offset,
                          globus_off_t     *  Length);

void
buffer_clear_flag(buffer_handle_t  * BufferHandle,
                  buffer_priv_id_t   PrivateID,
                  char             * Buffer);

int
buffer_get_ready_buffer_count(buffer_handle_t  * BufferHandle,
                              buffer_priv_id_t   PrivateID);

void
buffer_set_offset_length(buffer_handle_t  * BufferHandle,
                         buffer_priv_id_t   PrivateID,
                         char             * Buffer,
                         globus_off_t       Offset,
                         globus_off_t       Length);
void
buffer_store_offset_length(buffer_handle_t  * BufferHandle,
                           buffer_priv_id_t   PrivateID,
                           char             * Buffer,
                           globus_off_t       Offset,
                           globus_off_t       Length);

void
buffer_get_stored_offset_length(buffer_handle_t  * BufferHandle,
                                buffer_priv_id_t   PrivateID,
                                char             * Buffer,
                                globus_off_t     * Offset,
                                globus_off_t     * Length);

void
buffer_clear_stored_offset_length(buffer_handle_t  * BufferHandle,
                                  buffer_priv_id_t   PrivateID,
                                  char             * Buffer);
#endif /* GRIDFTP_DSI_HPSS_HPSS_BUFFER_H */

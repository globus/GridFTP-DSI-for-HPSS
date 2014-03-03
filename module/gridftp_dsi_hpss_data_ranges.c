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

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_data_ranges.h"
#include "gridftp_dsi_hpss_range_list.h"
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_msg.h"

struct data_ranges {
	data_ranges_mode_t          Mode;
	msg_handle_t              * MsgHandle;
	globus_mutex_t              Lock;
	globus_cond_t               Cond;
	globus_bool_t               Stop;
	int                         ThreadCount;
	data_ranges_buffer_pass_t   BufferPassFunc;
	void                      * BufferPassArg;
};

globus_result_t
data_ranges_init(data_ranges_mode_t    Mode,
                 msg_handle_t       *  MsgHandle,
                 data_ranges_t      ** DataRanges)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*DataRanges = (data_ranges_t *) globus_calloc(1, sizeof(data_ranges_t));
	if (*DataRanges == NULL)
	{
		result = GlobusGFSErrorMemory("data_ranges_t");
		goto cleanup;
	}

	globus_mutex_init(&(*DataRanges)->Lock, NULL);
	globus_cond_init(&(*DataRanges)->Cond, NULL);
	(*DataRanges)->Mode         = Mode;
	(*DataRanges)->MsgHandle    = MsgHandle;
	(*DataRanges)->Stop         = GLOBUS_FALSE;
	(*DataRanges)->ThreadCount  = 0;

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

void
data_ranges_destroy(data_ranges_t * DataRanges)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (DataRanges != NULL)
	{
		globus_mutex_destroy(&DataRanges->Lock);
		globus_cond_destroy(&DataRanges->Cond);

		/* Destroy our handle. */
		globus_free(DataRanges);
	}

	GlobusGFSHpssDebugExit();
}

static void
data_ranges_send_range_complete(data_ranges_t * DataRanges,
                                globus_off_t    Offset,
                                globus_off_t    Length)
{
	globus_result_t result = GLOBUS_SUCCESS;
	data_ranges_msg_range_complete_t range_complete;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Prepare the range message. */
	range_complete.Offset = Offset;
	range_complete.Length = Length;

	/* Send this message to anyone that is listening. */
	result = msg_send(DataRanges->MsgHandle,
	                  MSG_COMP_ID_ANY,
	                  MSG_COMP_ID_TRANSFER_DATA_RANGES,
	                  DATA_RANGES_MSG_TYPE_RANGE_COMPLETE,
	                  sizeof(range_complete),
	                  &range_complete);

	/* XXX */
	globus_assert(result == GLOBUS_SUCCESS);

	GlobusGFSHpssDebugExit();
}

static void
data_ranges_send_range_received(data_ranges_t * DataRanges,
                                globus_off_t    Offset,
                                globus_off_t    Length)
{
	globus_result_t result = GLOBUS_SUCCESS;
	data_ranges_msg_range_received_t range_received;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Prepare the range message. */
	range_received.Offset = Offset;
	range_received.Length = Length;

	/* Send this message to anyone that is listening. */
	result = msg_send(DataRanges->MsgHandle,
	                  MSG_COMP_ID_ANY,
	                  MSG_COMP_ID_TRANSFER_DATA_RANGES,
	                  DATA_RANGES_MSG_TYPE_RANGE_RECEIVED,
	                  sizeof(range_received),
	                  &range_received);

	/* XXX */
	globus_assert(result == GLOBUS_SUCCESS);

	GlobusGFSHpssDebugExit();
}

void
data_ranges_buffer(void         * CallbackArg,
                   char         * Buffer,
                   globus_off_t   Offset,
                   globus_off_t   Length)
{
	data_ranges_t * data_ranges = (data_ranges_t *) CallbackArg;
	globus_bool_t   stopping    = GLOBUS_FALSE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&data_ranges->Lock);
	{
		if ((stopping = data_ranges->Stop) == GLOBUS_FALSE)
		{
			/* Indicate that we are still here. */
			data_ranges->ThreadCount++;
		}
	}
	globus_mutex_unlock(&data_ranges->Lock);

	if (stopping == GLOBUS_TRUE)
		goto cleanup;

	if (data_ranges->Mode & DATA_RANGE_MODE_RANGE_COMPLETE)
	{
		data_ranges_send_range_complete(data_ranges, Offset, Length);
	}

	if (data_ranges->Mode & DATA_RANGE_MODE_RANGE_RECEIVED)
	{
		data_ranges_send_range_received(data_ranges, Offset, Length);
	}

	/* Pass the buffer forward. */
	data_ranges->BufferPassFunc(data_ranges->BufferPassArg,
	                             Buffer,
	                             Offset,
	                             Length);

	globus_mutex_lock(&data_ranges->Lock);
	{
		/* Indicate that we are done here. */
		data_ranges->ThreadCount--;

		if (data_ranges->ThreadCount == 0)
			globus_cond_signal(&data_ranges->Cond);
	}
	globus_mutex_unlock(&data_ranges->Lock);

cleanup:
	GlobusGFSHpssDebugExit();
}

void
data_ranges_flush(data_ranges_t * DataRanges)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&DataRanges->Lock);
	{
		while (DataRanges->ThreadCount > 0)
			globus_cond_wait(&DataRanges->Cond, &DataRanges->Lock);
	}
	globus_mutex_unlock(&DataRanges->Lock);

	GlobusGFSHpssDebugExit();
}

void
data_ranges_stop(data_ranges_t * DataRanges)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&DataRanges->Lock);
	{
		DataRanges->Stop = GLOBUS_TRUE;
	}
	globus_mutex_unlock(&DataRanges->Lock);

	GlobusGFSHpssDebugExit();
}

void
data_ranges_set_buffer_pass_func(data_ranges_t            * DataRanges,
                                 data_ranges_buffer_pass_t  BufferPassFunc,
                                 void                     * BufferPassArg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&DataRanges->Lock);
	{
		DataRanges->BufferPassFunc = BufferPassFunc;
		DataRanges->BufferPassArg  = BufferPassArg;
	}
	globus_mutex_unlock(&DataRanges->Lock);

	GlobusGFSHpssDebugExit();
}


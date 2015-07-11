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

/*
 * System includes
 */
#include <pthread.h>

/*
 * Local includes
 */
#include "pio.h"

void *
pio_coordinator(void * Arg)
{
	pio_t * pio = Arg;
	int      retval;
	uint64_t range_offset;
	uint64_t range_length;
	uint64_t bytes_moved;
	hpss_pio_gapinfo_t gap_info;

	GlobusGFSName(pio_coordinator);

//	while (range_list_empty(pio->Coordinator.RangeList) == GLOBUS_FALSE)
	{
//		range_list_pop(pio_control->PioExecute.RangeList, &range_offset, &range_length);

		/* Initialize bytes_moved. */
		bytes_moved = cast64(0);

		do {
			range_offset = add64m(range_offset, bytes_moved);
			range_length = sub64m(range_length, bytes_moved);
			bytes_moved = cast64(0);
			memset(&gap_info, 0, sizeof(gap_info));

			/* Call pio execute. */
			retval = hpss_PIOExecute(pio->FD,
			                         range_offset,
			                         range_length,
			                         pio->Coordinator.StripeGroup,
			                         &gap_info,
			                         &bytes_moved);

			/*
			 * It appears that gap_info.offset is relative to range_offset. So you
			 * must add the two to get the real offset of the gap. Also, if
			 * there is a gap, bytes_moved = gap_info.offset since bytes_moved
			 * is also relative to range_offset.
			 */

			/* Add in any hole we may have found. */
			if (neqz64m(gap_info.Length))
				bytes_moved = add64m(gap_info.Offset, gap_info.Length);

		} while (retval == 0 && !eq64(bytes_moved, range_length));

		if (retval != 0)
		{
			pio->Coordinator.Result = GlobusGFSErrorSystemError("hpss_PIOExecute", -retval);
//			break;
		}
	}

	retval = hpss_PIOEnd(pio->Coordinator.StripeGroup);
	if (retval && pio->Coordinator.Result == GLOBUS_SUCCESS)
		pio->Coordinator.Result = GlobusGFSErrorSystemError("hpss_PIOEnd", -retval);

// XXX Close file

	return NULL;
}

void *
pio_participant(void * Arg)
{
	pio_t * pio = Arg;

	GlobusGFSName(pio_participant);

	char * buffer = globus_malloc(pio->BlockSize);
	if (!buffer)
	{
		pio->Participant.Result = GlobusGFSErrorMemory("globus_malloc");
		return NULL;
	}

	int retval = hpss_PIORegister(0,
	                              NULL, /* DataNetSockAddr */
	                              buffer,
	                              pio->BlockSize,
	                              pio->Participant.StripeGroup,
	                              pio->Participant.Callout,
	                              pio->Participant.CalloutArg);
	if (retval)
		pio->Participant.Result = GlobusGFSErrorSystemError("hpss_PIORegister", -retval);

	retval = hpss_PIOEnd(pio->Participant.StripeGroup);
	if (retval && pio->Participant.Result == GLOBUS_SUCCESS)
		pio->Participant.Result = GlobusGFSErrorSystemError("hpss_PIOEnd", -retval);
	globus_free_buffer(buffer);

	return NULL;
}

globus_result_t
pio_open_file_for_writing(globus_gfs_operation_t       Operation,
                          globus_gfs_transfer_info_t * TransferInfo,
                          int                        * FD,
                          int                        * FileStripeWidth)
{
	return GLOBUS_SUCCESS;
}

globus_result_t
pio_open_file_for_reading(globus_gfs_operation_t       Operation,
                          globus_gfs_transfer_info_t * TransferInfo,
                          int                        * FD,
                          int                        * FileStripeWidth)
{
	return GLOBUS_SUCCESS;
}

globus_result_t
pio_init(globus_gfs_operation_t       Operation,
         globus_gfs_transfer_info_t * TransferInfo,
         hpss_pio_operation_t         OperationType,
         int                          BlockSize,
         hpss_pio_cb_t                PioCallout,
         void                       * PioCalloutArg,
         pio_t                     ** Pio)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(pio_init);

	*Pio = globus_malloc(sizeof(pio_t));
	if (!*Pio)
		return GlobusGFSErrorMemory("pio_t");
	memset(*Pio, 0, sizeof(pio_t));
	(*Pio)->FD = -1;
	(*Pio)->BlockSize = -1;
	(*Pio)->Coordinator.Result  = GLOBUS_SUCCESS;
	(*Pio)->Participant.Result  = GLOBUS_SUCCESS;
	(*Pio)->Participant.Callout = PioCallout;
	(*Pio)->Participant.CalloutArg = PioCalloutArg;

	int file_stripe_width;

	switch (OperationType)
	{
	case HPSS_PIO_WRITE:
		result = pio_open_file_for_writing(Operation, TransferInfo, &(*Pio)->FD, &file_stripe_width);
		break;
	case HPSS_PIO_READ:
		result = pio_open_file_for_reading(Operation, TransferInfo, &(*Pio)->FD, &file_stripe_width);
		break;
	}

	if (result != GLOBUS_SUCCESS)
		return result;

	/*
	 * Don't use HPSS_PIO_HANDLE_GAP, it's bugged in HPSS 7.4.
	 */
	hpss_pio_params_t pio_params;
	pio_params.Operation       = OperationType;
	pio_params.ClntStripeWidth = 1;
	pio_params.BlockSize       = BlockSize;
	pio_params.FileStripeWidth = file_stripe_width;
	pio_params.IOTimeOutSecs   = 0;
	pio_params.Transport       = HPSS_PIO_MVR_SELECT;
	pio_params.Options         = 0;

	int retval = hpss_PIOStart(&pio_params, &(*Pio)->Coordinator.StripeGroup);
	if (retval != 0)
		return GlobusGFSErrorSystemError("hpss_PIOStart", -retval);

	void         * group_buffer  = NULL;
	unsigned int   buffer_length = 0;

	retval = hpss_PIOExportGrp((*Pio)->Coordinator.StripeGroup,
	                           &group_buffer,
	                           &buffer_length);
	if (retval != 0)
		return GlobusGFSErrorSystemError("hpss_PIOExportGrp", -retval);

	retval = hpss_PIOImportGrp(group_buffer,
                               buffer_length,
                               &(*Pio)->Participant.StripeGroup);
	if (retval != 0)
		return GlobusGFSErrorSystemError("hpss_PIOImportGrp", -retval);

	return GLOBUS_SUCCESS;
}

globus_result_t
pio_start(pio_t * Pio)
{
	GlobusGFSName(pio_start);

	int retval = pthread_create(&Pio->Coordinator.ThreadID, NULL, pio_coordinator, Pio);
	if (retval)
		return GlobusGFSErrorSystemError("pthread_create", retval);

	retval = pthread_create(&Pio->Participant.ThreadID, NULL, pio_participant, Pio);
	if (retval)
		return GlobusGFSErrorSystemError("pthread_create", retval);

	return GLOBUS_SUCCESS;
}

void
pio_destroy(pio_t * Pio)
{
	if (Pio)
	{
		if (Pio->FD >= 0)
			hpss_Close(Pio->FD);
		globus_free(Pio);
	}
}


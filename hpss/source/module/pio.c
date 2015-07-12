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

globus_result_t
pio_launch_detached(void * (*ThreadEntry)(void * Arg), void * Arg)
{
	int             rc      = 0;
	int             initted = 0;
	pthread_t       thread;
	pthread_attr_t  attr;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(pio_launch_detached);

	/*
	 * Launch a detached thread.
	 */
	if ((rc = pthread_attr_init(&attr)) || !(initted = 1) ||
	    (rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) ||
	    (rc = pthread_create(&thread, &attr, ThreadEntry, Arg)))
	{
		result = GlobusGFSErrorSystemError("Launching get object thread", rc);
	}
	if (initted) pthread_attr_destroy(&attr);
	return result;
}

globus_result_t
pio_launch_attached(void *   (* ThreadEntry)(void * Arg),
                    void      * Arg,
                    pthread_t * ThreadID)
{
	int rc = 0;

	GlobusGFSName(pio_launch_attached);

	/*
	 * Launch a detached thread.
	 */
	rc = pthread_create(ThreadID, NULL, ThreadEntry, Arg);
	if (rc)
		return GlobusGFSErrorSystemError("Launching get object thread", rc);
	return GLOBUS_SUCCESS;
}

void *
pio_coordinator_thread(void * Arg)
{
	pio_t *  pio = Arg;
	int      rc  = 0;
	uint64_t length      = pio->FileSize;
	uint64_t offset      = 0;
	uint64_t bytes_moved = 0;
	hpss_pio_gapinfo_t gap_info;

	GlobusGFSName(pio_coordinator_thread);

	do {
		bytes_moved = 0;
		memset(&gap_info, 0, sizeof(gap_info));

		/* Call pio execute. */
		rc = hpss_PIOExecute(pio->FD,
		                     offset,
		                     length,
		                     pio->CoordinatorSG,
		                     &gap_info,
		                     &bytes_moved);

		if (rc != 0)
		{
			pio->CoordinatorResult = GlobusGFSErrorSystemError("hpss_PIOExecute", -rc);
			break;
		}

		/*
		 * It appears that gap_info.offset is relative to offset. So you
		 * must add the two to get the real offset of the gap. Also, if
		 * there is a gap, bytes_moved = gap_info.offset since bytes_moved
		 * is also relative to range_offset.
		 */

		/* Add in any hole we may have found. */
		if (neqz64m(gap_info.Length))
			bytes_moved = add64m(gap_info.Offset, gap_info.Length);

		offset += bytes_moved;
		length -= bytes_moved;

	} while (rc == 0 && offset < length);

	rc = hpss_PIOEnd(pio->CoordinatorSG);
	if (rc && pio->CoordinatorResult == GLOBUS_SUCCESS)
		pio->CoordinatorResult = GlobusGFSErrorSystemError("hpss_PIOEnd", -rc);

	return NULL;
}

int
pio_register_callback(void     *  UserArg,
                      uint64_t    Offset,
                      uint32_t *  Length,
                      void     ** Buffer)
{
	pio_t * pio = UserArg;
	*Buffer = pio->Buffer;
	return pio->WriteCO(*Buffer, Length, Offset, pio->UserArg);
}

void *
pio_thread(void * Arg)
{
	int               rc     = 0;
	pio_t           * pio    = Arg;
	globus_result_t   result = GLOBUS_SUCCESS;
	int               coord_launched  = 0;
	int               safe_to_end_pio = 0;
	pthread_t         thread_id;
	char            * buffer = NULL;

	GlobusGFSName(pio_thread);

	buffer = malloc(pio->BlockSize);
	if (!buffer)
	{
		result = GlobusGFSErrorMemory("pio buffer");
		goto cleanup;
	}

	/*
	 * Save buffer into pio; the write callback shows up without
	 * a buffer right after hpss_PIOExecute().
	 */
	pio->Buffer = buffer;

	result = pio_launch_attached(pio_coordinator_thread, pio, &thread_id);
	if (result)
		goto cleanup;
	coord_launched = 1;

	rc = hpss_PIORegister(0,
	                      NULL, /* DataNetSockAddr */
	                      buffer,
	                      pio->BlockSize,
	                      pio->ParticipantSG,
	                      pio_register_callback,
	                      pio);
	if (rc)
		result = GlobusGFSErrorSystemError("hpss_PIORegister", -rc);
	safe_to_end_pio = 1;

cleanup:
	if (safe_to_end_pio)
	{
		rc = hpss_PIOEnd(pio->ParticipantSG);
		if (rc && !result)
			result = GlobusGFSErrorSystemError("hpss_PIOEnd", -rc);
	}

	if (coord_launched) pthread_join(thread_id, NULL);
	if (buffer) free(buffer);

	if (!result) result = pio->CoordinatorResult;

	pio->CompletionCB(result, pio->UserArg);
	free(pio);

	return NULL;
}

globus_result_t
pio_start(int                     FD,
          int                     FileStripeWidth,
          uint32_t                BlockSize,
          uint64_t                FileSize,
          pio_write_callout       WriteCO,
          pio_completion_callback CompletionCB,
          void                  * UserArg)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	pio_t           * pio    = NULL;
	hpss_pio_params_t pio_params;
	void            * group_buffer  = NULL;
	unsigned int      buffer_length = 0;

	GlobusGFSName(pio_start);

	/*
	 * Allocate our structure.
	 */
	pio = malloc(sizeof(pio_t));
	if (!pio)
	{
		result = GlobusGFSErrorMemory("pio_t");
		goto cleanup;
	}
	memset(pio, 0, sizeof(pio_t));
	pio->FD           = FD;
	pio->BlockSize    = BlockSize;
	pio->FileSize     = FileSize;
	pio->WriteCO      = WriteCO;
	pio->CompletionCB = CompletionCB;
	pio->UserArg      = UserArg;

	/*
	 * Don't use HPSS_PIO_HANDLE_GAP, it's bugged in HPSS 7.4.
	 */
	pio_params.Operation       = HPSS_PIO_WRITE;
	pio_params.ClntStripeWidth = 1;
	pio_params.BlockSize       = BlockSize;
	pio_params.FileStripeWidth = FileStripeWidth;
	pio_params.IOTimeOutSecs   = 0;
	pio_params.Transport       = HPSS_PIO_MVR_SELECT;
	pio_params.Options         = 0;

	int retval = hpss_PIOStart(&pio_params, &pio->CoordinatorSG);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_PIOStart", -retval);
		goto cleanup;
	}

	retval = hpss_PIOExportGrp(pio->CoordinatorSG, &group_buffer, &buffer_length);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_PIOExportGrp", -retval);
		goto cleanup;
	}

	retval = hpss_PIOImportGrp(group_buffer, buffer_length, &pio->ParticipantSG);
	if (retval != 0)
	{
		result = GlobusGFSErrorSystemError("hpss_PIOImportGrp", -retval);
		goto cleanup;
	}

	result = pio_launch_detached(pio_thread, pio);

	if (!result) return result;

cleanup:
	/* Can not clean up the stripe groups without crashing. */
	if (pio) free(pio);
	return result;
}

#ifdef NOT
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

#endif /* NOT */

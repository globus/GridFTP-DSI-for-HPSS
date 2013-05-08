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
#include "gridftp_dsi_hpss_range_list.h"
#include "gridftp_dsi_hpss_misc.h"

#ifdef DMALLOC
/*
 * Dmalloc
 */
#include <dmalloc.h>
#endif /* DMALLOC */

typedef struct range {
	globus_off_t   Offset;
	globus_off_t   Length;
	struct range * Next;
	struct range * Prev;
} range_t;

struct range_list {
	globus_mutex_t   Lock;
	struct range   * Head;
	struct range   * Tail;
};

globus_result_t
range_list_init(range_list_t ** RangeList)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	*RangeList = (range_list_t *) globus_calloc(1, sizeof(range_list_t));
	if (*RangeList == NULL)
	{
		result = GlobusGFSErrorMemory("range_list_t");
		goto cleanup;
	}

	/* Allocate the lock. */
	globus_mutex_init(&(*RangeList)->Lock, NULL);

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
range_list_destroy(range_list_t * RangeList)
{
	range_t * range = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (RangeList != NULL)
	{
		/* Release the list. */
		while ((range = RangeList->Head) != NULL)
		{
			/* Unlink this range. */
			RangeList->Head = range->Next;

			/* Free the range. */
			globus_free(range);
		}

		/* Release the lock. */
		globus_mutex_destroy(&RangeList->Lock);

		/* Release the range list. */
		globus_free(RangeList);
	}

	GlobusGFSHpssDebugExit();
}

/*
 * Ignore lengths of 0.
 */
globus_result_t
range_list_insert(range_list_t * RangeList,
                  globus_off_t   Offset,
                  globus_off_t   Length)
{
	range_t         * new_range     = NULL;
	range_t         * next_range    = NULL;
	range_t         * current_range = NULL;
	globus_result_t   result        = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Ignore 0 lengths. */
	if (Length == 0)
		goto cleanup;

	globus_mutex_lock(&RangeList->Lock);
	{
		/* Find the range we extend or come before. */
		for (current_range  = RangeList->Head; 
		     current_range != NULL; 
		     current_range  = current_range->Next)
		{
			/* If we go before current_range... */
			if (Offset < current_range->Offset)
			{
				/* There should never be overlap. */
				globus_assert((Offset + Length) <= current_range->Offset);

				break;
			}

			/* There should never be a collision. */
			globus_assert(Offset != current_range->Offset);
			/* Make sure there's no overlap. */
			globus_assert(Offset >= (current_range->Offset + current_range->Length));

			/* See if we extend the back of current_range. */
			if (Offset == (current_range->Offset + current_range->Length))
				break;
		}

		if (current_range && (Offset + Length) == current_range->Offset)
		{
			/* Extend the front. */
			current_range->Offset  = Offset;
			current_range->Length += Length;
		} else if (current_range && (current_range->Offset + current_range->Length) == Offset)
		{
			/* Extend the back. */
			current_range->Length += Length;

			/* Do we reach the next range? */
			if (current_range->Next != NULL)
			{
				next_range = current_range->Next;

				if ((current_range->Length + current_range->Offset) == next_range->Offset)
				{
					/* Merge in the next range. */
					current_range->Length += next_range->Length;

					/* Unlink the next range. */
					current_range->Next = next_range->Next;
					if (next_range->Next != NULL)
						next_range->Next->Prev = current_range;
					else
						RangeList->Tail = current_range;
					/* Deallocate the unlinked range. */
					globus_free(next_range);
				}
			}
		} else
		{
			/* Allocate a new entry. */
			new_range = (range_t *) globus_calloc(1, sizeof(range_t));
			if (new_range == NULL)
			{
				result = GlobusGFSErrorMemory("range_t");
				goto unlock;
			}

			new_range->Offset = Offset;
			new_range->Length = Length;

			/* Put it on the list. */
			if (current_range != NULL)
			{
				/* Put it before current_range. */
				new_range->Next = current_range;
				new_range->Prev = current_range->Prev;
				if (current_range->Prev != NULL)
					current_range->Prev->Next = new_range;
				else
					RangeList->Head = new_range;

				current_range->Prev = new_range;
			} else
			{
				/* Put it on the tail. */
				if (RangeList->Tail != NULL)
				{
					RangeList->Tail->Next = new_range;
					new_range->Prev = RangeList->Tail;
					RangeList->Tail = new_range;
				} else
				{
					RangeList->Head = new_range;
					RangeList->Tail = new_range;
				}
			}
		}
	}
unlock:
	globus_mutex_unlock(&RangeList->Lock);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

globus_bool_t
range_list_empty(range_list_t * RangeList)
{
	globus_bool_t empty = GLOBUS_TRUE;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&RangeList->Lock);
	{
		empty = (RangeList->Head == NULL);
	}
	globus_mutex_unlock(&RangeList->Lock);

	GlobusGFSHpssDebugExit();
	return empty;
}

void
range_list_delete(range_list_t * RangeList,
                  globus_off_t   Offset,
                  globus_off_t   Length)
{
	range_t * range     = NULL;
	range_t * tmp_range = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&RangeList->Lock);
	{
		/* For each range. */
		for (range = RangeList->Head; range != NULL && Length > 0; )
		{
			/* If our offset starts before the next range... */
			if (Offset < range->Offset)
			{
				/* If our range completes before this range... */
				if ((Offset + Length) <= range->Offset)
					break;

				/* Truncate our range. */
				Length -= range->Offset - Offset;
				Offset  = range->Offset;

				continue;
			}

			/* If this range doesn't extend to the end of the file... */
			if (range->Length != RANGE_LIST_MAX_LENGTH)
			{
				/* And our range is after this range... */
				if (Offset > (range->Offset + range->Length))
				{
					range = range->Next;
					continue;
				}
			}

			/*
			 * Our range is in this range.
			 */
			if (range->Offset == Offset)
			{
				/*
				 * The ranges start on the same offset.
				 */
				if (Length >= range->Length)
				{
					/*
					 * We are larger than this range.
					 */

					/* Update our range. */
					Offset += range->Length;
					Length -= range->Length;

					/* Remove this range. */
					tmp_range = range;
					range = range->Next;
					if (range == NULL)
						RangeList->Tail = tmp_range->Prev;
					else
						range->Prev = tmp_range->Prev;

					if (tmp_range->Prev == NULL)
						RangeList->Head  = range;
					else
						tmp_range->Prev->Next = range;

					/* Deallocate the range. */
					globus_free(tmp_range);
				} else
				{
					/*
					 * We are shorter than this range.
					 */

					/* Shorten that range. */
					range->Offset += Length;
					if (range->Length != RANGE_LIST_MAX_LENGTH)
						range->Length -= Length;

					/* Update our range. */
					Offset += Length;
					Length  = 0;
				}
			} else
			{
				/* 
				 * Our range starts after theirs.
				 */

				/* Split this range. */
				tmp_range = (range_t *) globus_calloc(1, sizeof(range_t));

				tmp_range->Offset = range->Offset;
				tmp_range->Length = Offset - tmp_range->Offset;

				range->Offset  = Offset;
				range->Length -= tmp_range->Length;

				tmp_range->Next = range;
				tmp_range->Prev = range->Prev;
				range->Prev     = tmp_range;

				if (tmp_range->Prev == NULL)
					RangeList->Head = tmp_range;
				else
					tmp_range->Prev->Next = tmp_range;
			}
		}
	}
	globus_mutex_unlock(&RangeList->Lock);

	GlobusGFSHpssDebugExit();
}

void
range_list_peek(range_list_t * RangeList,
                globus_off_t * Offset,
                globus_off_t * Length)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&RangeList->Lock);
	{
		/* Make sure we have something to return. */
		globus_assert(RangeList->Head != NULL);

		/* Copy out the values. */
		*Offset = RangeList->Head->Offset;
		*Length = RangeList->Head->Length;
	}
	globus_mutex_unlock(&RangeList->Lock);

	GlobusGFSHpssDebugExit();
}

void
range_list_pop(range_list_t * RangeList,
               globus_off_t * Offset,
               globus_off_t * Length)
{
	range_t * range = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return. */
	*Offset = 0;
	*Length = 0;

	globus_mutex_lock(&RangeList->Lock);
	{
		/* Make sure we have something to return. */
		globus_assert(RangeList->Head != NULL);

		/* Save this range. */
		range = RangeList->Head;

		/* Move the head forward. */
		RangeList->Head = range->Next;

		if (RangeList->Head != NULL)
			RangeList->Head->Prev = NULL;

		/* Update the tail if needed. */
		if (RangeList->Head == NULL)
			RangeList->Tail = NULL;
	}
	globus_mutex_unlock(&RangeList->Lock);

	/* Copy out the values. */
	*Offset = range->Offset;
	*Length = range->Length;

	/* Release the range. */
	globus_free(range);

	GlobusGFSHpssDebugExit();
}

/*
 * With the way file ranges work (it's the expected ranges for
 * this transfer in terms of file offsets), FileOffset must
 * fall within a range in FileRangeList.
 */
globus_off_t
range_list_get_transfer_offset(range_list_t * FileRangeList,
                               globus_off_t   FileOffset)
{
	range_t      * range           = NULL;
	globus_off_t   transfer_offset = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&FileRangeList->Lock);
	{
		/* For each range. */
		for (range = FileRangeList->Head; range != NULL; range = range->Next)
		{
			/* If FileOffset came before this range, it was invalid. */
			globus_assert(FileOffset >= range->Offset);

			/* If FileOffset falls on this range... */
			if (FileOffset <= (range->Offset + range->Length))
			{
				transfer_offset += FileOffset - range->Offset;
				break;
			}

			/* FileOffset comes after this range, add the entire length. */
			transfer_offset += range->Length;
		}

		/* File offset should have fallen within a range. */
		globus_assert(range != NULL);
	}
	globus_mutex_unlock(&FileRangeList->Lock);

	GlobusGFSHpssDebugExit();

	return transfer_offset;
}

/*
 * With the way file ranges work (it's the expected ranges for
 * this transfer in terms of file offsets), the returned file offset
 * must fall within a range in FileRangeList.
 */
globus_off_t
range_list_get_file_offset(range_list_t * FileRangeList,
                           globus_off_t   TransferOffset)
{
	range_t      * range       = NULL;
	globus_off_t   file_offset = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&FileRangeList->Lock);
	{
		/* For each range. */
		for (range = FileRangeList->Head; range != NULL; range = range->Next)
		{
			if (TransferOffset < range->Length)
			{
				/* Add in the remaining transfer offset. */
				file_offset = range->Offset + TransferOffset;
				break;
			}

			/* Subtract from our TransferOffset. */
			TransferOffset -= range->Length;
		}

		globus_assert(range != NULL);
	}
	globus_mutex_unlock(&FileRangeList->Lock);

	GlobusGFSHpssDebugExit();

	return file_offset;
}

/******************************************************************************
 *
 * NOTE ABOUT RESTART MARKERS, TRANSFER OFFSETS, FILE OFFSETS, PARTIAL OFFSETS
 *
 * The inital offset of any NEW transfer is 0 regardless of partial offsets
 * or restart markers.
 *
 * Range markers are in terms of transfer offsets
 *  PUT file
 *  111 Range Marker 0-1046478848
 *  111 Range Marker 1046478848-2217738240
 *  111 Range Marker 2217738240-3312451584
 *
 *  PPUT 1048576 16106127360 file
 *  111 Range Marker 0-1280311296
 *  111 Range Marker 1280311296-250085376
 *
 * Restart markers are in terms of transfer offsets, not file offsets. So:
 *  PUT file
 *  111 Range Marker 0-1046478848
 *  111 Range Marker 1046478848-2217738240
 *  111 Range Marker 2217738240-3312451584
 *
 *  REST 0-3312451584
 *  PUT file
 *  111 Range Marker 3312451584-4543479808
 *  111 Range Marker 4543479808-5715787776
 *  111 Range Marker 5715787776-6896484352
 *
 *  PPUT 1048576 16106127360 file
 *  111 Range Marker 0-1280311296
 *  111 Range Marker 1280311296-250085376
 *
 *  REST 0-250085376
 *  PPUT 1048576 16106127360 file
 *  111 Range Marker 250085376-345832957 (+1048576 to get file offset)
 *
 *  Partial offsets are not reflected in transfer offsets. So:
 *    File Offset = TransferOffset + Partial Offset
 *
 *  Restart markers are the portion of the transfer offsets to skip on
 *  the next transfer.
 *
 *****************************************************************************/

/*
 * We need to populate RangeList with the File Offsets to transfer.
 * TransferInfo->partial_length will be  = -1.
 * TransferInfo->partial_offset will be >= 0
 * TransferInfo->alloc_size = the amount of incoming data
 * 
 * RangeList is the inverse of the restart markers; it is the transfer
 * offsets that should be transferred. The last entry will have a length
 * of -1 meaning end of file.
 */
globus_result_t
range_list_fill_stor_range(range_list_t               * RangeList,
                           globus_gfs_transfer_info_t * TransferInfo)
{
	int             index                 = 0;
	globus_off_t    range_offset          = 0;
	globus_off_t    range_length          = 0;
	globus_off_t    remaining_file_length = 0;
	globus_off_t    starting_file_offset  = 0;
	globus_result_t result                = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Get the starting file offset. */
	starting_file_offset = TransferInfo->partial_offset;

	/* Get the remaining file length. */
	remaining_file_length = TransferInfo->alloc_size;

	/* For each range in the restart list. */
	for (index = 0;
	     index < globus_range_list_size(TransferInfo->range_list);
	     index++)
	{
		globus_range_list_at(TransferInfo->range_list, index, &range_offset, &range_length);

		/* Convert from transfer offset to file offset. */
		range_offset += TransferInfo->partial_offset;

		/* Skip zero-length ranges. */
		if (range_length == 0)
			continue;

		/* -1 is code for 'remainder of transfer'. */
		if (range_length == -1)
			range_length = remaining_file_length;

		/* Truncate the range to fit the alloc_size. */
		if (range_length > remaining_file_length)
			range_length = remaining_file_length;

		/* Insert this range into the list. */
		result = range_list_insert(RangeList, range_offset, range_length);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Decrement the remaining file length to transfer. */
		remaining_file_length -= range_length;

		/* Bail if there is nothing to transfer. */
		if (remaining_file_length == 0)
			break;
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * We need to populate RangeList with the File Offsets to transfer.
 * TransferInfo->partial_length will be >= -1.
 * TransferInfo->partial_offset will be >= 0
 * 
 * RangeList is the inverse of the restart markers; it is the transfer
 * offsets that should be transferred. The last entry will have a length
 * of -1 meaning end of file.
 */
globus_result_t
range_list_fill_retr_range(range_list_t               * RangeList,
                           globus_gfs_transfer_info_t * TransferInfo)
{
	int             index                = 0;
	globus_off_t    range_offset         = 0;
	globus_off_t    range_length         = 0;
	globus_off_t    file_length          = 0;
	globus_off_t    starting_file_offset = 0;
	globus_off_t    ending_file_offset   = 0;
	globus_result_t result               = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Get the length of the file. */
	result = misc_get_file_size(TransferInfo->pathname, &file_length);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* starting and ending are file offsets. */
	starting_file_offset = TransferInfo->partial_offset;
	ending_file_offset   = starting_file_offset + TransferInfo->partial_length;

	/* -1 is shorthand for 'to end of file' */
	if (TransferInfo->partial_length == -1)
	{
		/* Adjust the ending offset. */
		ending_file_offset = file_length - starting_file_offset;
	}

	/* Truncate the ending file offset to fit the file length. */
	if (ending_file_offset > file_length)
		ending_file_offset = file_length;

	/* For each range in the restart list. */
	for (index = 0;
	     index < globus_range_list_size(TransferInfo->range_list);
	     index++)
	{
		globus_range_list_at(TransferInfo->range_list, index, &range_offset, &range_length);

		/* Convert from transfer offset to file offset. */
		range_offset += TransferInfo->partial_offset;

		/* Skip anything at the end or past the end of the file. */
		if (range_offset >= ending_file_offset)
			continue;

		/* Adjust the length. */
		if (range_length == -1)
			range_length = ending_file_offset - range_offset;

		/* Truncate the length to fit the file. */
		if ((range_length + range_offset) > ending_file_offset)
			range_length = ending_file_offset - range_offset;

		/* Insert this range into the list. */
		result = range_list_insert(RangeList, range_offset, range_length);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

globus_result_t
range_list_fill_cksm_range(range_list_t              * RangeList,
                           globus_gfs_command_info_t * CommandInfo)
{
	globus_off_t    length = CommandInfo->cksm_length;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* -1 is shorthand for 'to end of file' */
	if (length == -1)
	{
		result = misc_get_file_size(CommandInfo->pathname, &length);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Adjust the length component of the range. */
		length -= CommandInfo->cksm_offset;
	}

	/* Populate the range list. */
	result = range_list_insert(RangeList, CommandInfo->cksm_offset, length);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}


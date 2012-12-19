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
			} else
			{
				/* Put it on the tail. */
				if (RangeList->Tail != NULL)
				{
					RangeList->Tail->Prev = new_range;
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

	globus_mutex_lock(&RangeList->Lock);
	{
		/* Make sure we have something to return. */
		globus_assert(RangeList->Head != NULL);

		/* Save this range. */
		range = RangeList->Head;

		/* Move the head forward. */
		RangeList->Head = range->Next;

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

static globus_result_t
range_list_fill_range(range_list_t        * RangeList,
                      globus_off_t          OffsetAdjustment,
                      globus_off_t          Length,
                      globus_range_list_t   RestartRangeList)
{
	int               index            = 0;
	globus_off_t      remaining_length = Length;
	globus_off_t      tmp_offset       = 0;
	globus_off_t      tmp_length       = 0;
	globus_result_t   result           = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* For each range in the restart list. */
	for (index = 0;
	     index < globus_range_list_size(RestartRangeList);
	     index++)
	{
		globus_range_list_at(RestartRangeList, index, &tmp_offset, &tmp_length);

		/* Make sure we don't add more than 'Length' bytes. */
		if (tmp_length == -1 || tmp_length > Length)
			tmp_length = Length;

		result = range_list_insert(RangeList, tmp_offset+OffsetAdjustment, tmp_length);
		if (result != GLOBUS_SUCCESS)
			break;

		/* Adjust the remaining_length. */
		remaining_length -= tmp_length;

		/* If we've run out of 'Length' bytes... */
		if (remaining_length == 0)
			break;
	}

#ifdef NOT
	/*
 	* Populate the transfer range list.
 	*/
	result = range_list_insert(RangeList, Offset, Length);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	if (RestartRangeList != NULL)
	{
		/* Create the 'exclude' range list. */
		result = range_list_init(&exclude_range_list);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Seed this exclude range list with the maximum length. */
		result = range_list_insert(exclude_range_list, 0, RANGE_LIST_MAX_LENGTH);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/*
		 * Now delete the ranges in RestartRangeList from our exclude range
		 * list. It includes portions of the file NOT covered by restart markers.
		 */
		for (index = 0;
		     index < globus_range_list_size(RestartRangeList);
		     index++)
		{
			globus_range_list_at(RestartRangeList, index, &tmp_offset, &tmp_length);

			/* -1 is shorthand for 'to end of file' */
			if (tmp_length == -1)
				tmp_length = RANGE_LIST_MAX_LENGTH;

			range_list_delete(exclude_range_list, tmp_offset, tmp_length);
		}

		/*
		 * Now for each range in the exlude list, remove it from our transfer list.
		 */

		while (!range_list_empty(exclude_range_list))
		{
			/* Pop the next excluded range. */
			range_list_pop(exclude_range_list, &tmp_offset, &tmp_length);

			/* Delete it from our transfer range list. */
			range_list_delete(RangeList, tmp_offset, tmp_length);
		}
	}

#endif /* NOT */
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
range_list_fill_stor_range(range_list_t               * RangeList,
                           globus_gfs_transfer_info_t * TransferInfo)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	result = range_list_fill_range(RangeList,
	                               TransferInfo->partial_offset,
	                               TransferInfo->alloc_size,
	                               TransferInfo->range_list);

	GlobusGFSHpssDebugExit();
	return result;
}

globus_result_t
range_list_fill_retr_range(range_list_t               * RangeList,
                           globus_gfs_transfer_info_t * TransferInfo)
{
	globus_off_t    length = TransferInfo->partial_length;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* -1 is shorthand for 'to end of file' */
	if (length == -1)
	{
		result = misc_get_file_size(TransferInfo->pathname, &length);
		if (result != GLOBUS_SUCCESS)
			goto cleanup;

		/* Adjust the length component of the range. */
		length -= TransferInfo->partial_offset;
	}

	result = range_list_fill_range(RangeList,
	                               TransferInfo->partial_offset,
	                               length,
	                               TransferInfo->range_list);

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


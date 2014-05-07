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
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_list.h"

#ifdef DMALLOC
/*
 * Dmalloc
 */
#include <dmalloc.h>
#endif /* DMALLOC */

typedef struct entry {
	struct entry * Prev;
	struct entry * Next;
	void         * Data;
} entry_t;

struct list {
	globus_mutex_t Lock;
	entry_t      * Head;
	entry_t      * Tail;
};

globus_result_t
list_init(list_t ** List)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the list. */
	*List = (list_t *) globus_calloc(1, sizeof(list_t));
	if (*List == NULL)
	{
		result = GlobusGFSErrorMemory("list_t");
		goto cleanup;
	}

	/* Initialize the list. */
	globus_mutex_init(&(*List)->Lock, NULL);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return SUCCESS;
}

void
list_destroy(list_t * List)
{
	entry_t * entry_save = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (List != NULL)
	{
		while ((entry_save = List->Head) != NULL)
		{
			/* Unlink this entry. */
			List->Head = List->Head->Next;
			/* Deallocate the entry. */
			globus_free(entry_save);
		}
		/* Deallocate the list. */
		globus_free(List);
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
list_insert(list_t * List,
            void   * Data)
{
	entry_t         * entry  = NULL;
	globus_result_t   result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the new entry. */
	entry = (entry_t *) globus_calloc(1, sizeof(entry_t));
	if (entry == NULL)
	{
		result = GlobusGFSErrorMemory("entry_t");
		goto cleanup;
	}

	/* Save the entry's data. */
	entry->Data = Data;

	/* Now put it on the list. */
	globus_mutex_lock(&List->Lock);
	{
		/* Let's insert on the tail assuming a FIFO usage. */
		entry->Prev = List->Tail;
		if (List->Tail != NULL)
			List->Tail->Next = entry;
		List->Tail = entry;
		if (List->Head == NULL)
			List->Head = entry;
	}
	globus_mutex_unlock(&List->Lock);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return SUCCESS;
}

globus_result_t
list_insert_at_end(list_t * List,
                   void   * Data)
{
	globus_result_t   result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/*
	 * Currently, all inserts are at the tail.
	 */
	result = list_insert(List, Data);

	GlobusGFSHpssDebugExit();

	return result;
}

globus_result_t
list_insert_before(list_t * List,
                   void   * NewData,
                   void   * ExistingData)
{
	entry_t         * new_entry      = NULL;
	entry_t         * existing_entry = NULL;
	globus_result_t   result         = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the new entry. */
	new_entry = (entry_t *) globus_calloc(1, sizeof(entry_t));
	if (new_entry == NULL)
	{
		result = GlobusGFSErrorMemory("entry_t");
		goto cleanup;
	}

	/* Save the new entry's data. */
	new_entry->Data = NewData;

	/* Now put it on the list. */
	globus_mutex_lock(&List->Lock);
	{
		/* Find the existing entry. */
		for (existing_entry  = List->Head; 
		     existing_entry != NULL; 
		     existing_entry  = existing_entry->Next)
		{
			if (existing_entry->Data == ExistingData)
				break;
		}

		/* Make sure we found something. */
		globus_assert(existing_entry != NULL);

		/* Insert before the existing entry. */
		new_entry->Prev      = existing_entry->Prev;
		existing_entry->Prev = new_entry;
		new_entry->Next      = existing_entry;
		if (new_entry->Prev == NULL)
			List->Head = new_entry;
		else
			new_entry->Prev->Next = new_entry;
	}
	globus_mutex_unlock(&List->Lock);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return SUCCESS;
}

void
list_move_to_end(list_t * List,
                 void   * Data)
{
	entry_t * entry  = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Now put it on the list. */
	globus_mutex_lock(&List->Lock);
	{
		/* Find the existing entry. */
		for (entry = List->Head; entry != NULL; entry = entry->Next)
		{
			if (entry->Data == Data)
				break;
		}

		/* Make sure we found something. */
		globus_assert(entry != NULL);

		/* Remove it from the list. */
		if (entry->Prev != NULL)
			entry->Prev->Next = entry->Next;
		else
			List->Head = entry->Next;

		if (entry->Next != NULL)
			entry->Next->Prev = entry->Prev;
		else
			List->Tail = entry->Prev;

		/* Now insert on the tail. */
		entry->Next = NULL;
		entry->Prev = List->Tail;
		if (List->Tail != NULL)
			List->Tail->Next = entry;
		List->Tail = entry;
		if (List->Head == NULL)
			List->Head = entry;
	}
	globus_mutex_unlock(&List->Lock);

	GlobusGFSHpssDebugExit();
}

void
list_move_before(list_t * List,
                 void   * DataToMove,
                 void   * NextData)
{
	entry_t * entry_to_move = NULL;
	entry_t * next_entry    = NULL;
	entry_t * entry         = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Now put it on the list. */
	globus_mutex_lock(&List->Lock);
	{
		/* Find the entries. */
		for (entry = List->Head; entry != NULL; entry = entry->Next)
		{
			if (entry->Data == DataToMove)
			{
				/* Make sure we haven't found it twice. */
				globus_assert(entry_to_move == NULL);

				/* Save it. */
				entry_to_move = entry;
			}

			if (entry->Data == NextData)
			{
				/* Make sure we haven't found it twice. */
				globus_assert(next_entry == NULL);

				/* Save it. */
				next_entry = entry;
			}

			if (next_entry != NULL && entry_to_move != NULL)
				break;
		}

		/* Make sure we found something. */
		globus_assert(next_entry != NULL && entry_to_move != NULL);

		/* Remove it from the list. */
		if (entry_to_move->Prev != NULL)
			entry_to_move->Prev->Next = entry_to_move->Next;
		else
			List->Head = entry_to_move->Next;

		if (entry_to_move->Next != NULL)
			entry_to_move->Next->Prev = entry_to_move->Prev;
		else
			List->Tail = entry_to_move->Prev;

		/* Now insert before next_entry. */
		entry_to_move->Next = next_entry;
		entry_to_move->Prev = next_entry->Prev;
		next_entry->Prev = entry_to_move;
		if (entry_to_move->Prev != NULL)
			entry_to_move->Prev->Next = entry_to_move;
		else
			List->Head = entry_to_move;
	}
	globus_mutex_unlock(&List->Lock);

	GlobusGFSHpssDebugExit();
}

void *
list_remove_head(list_t * List)
{
	void    * data  = NULL;
	entry_t * entry = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (List == NULL)
		goto cleanup;

	globus_mutex_lock(&List->Lock);
	{
		if (List->Head != NULL)
		{
			/* Save the entry. */
			entry = List->Head;
			/* Unlink it. */
			List->Head = List->Head->Next;
			if (List->Head != NULL)
				List->Head->Prev = NULL;
			if (List->Head == NULL)
				List->Tail = NULL;
			/* Save the data. */
			data = entry->Data;
			/* Release the entry */
			globus_free(entry);
		}
	}
	globus_mutex_unlock(&List->Lock);

cleanup:
	GlobusGFSHpssDebugExit();
	return data;
}

void *
list_peek_head(list_t * List)
{
	void * data = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (List == NULL)
		goto cleanup;

	globus_mutex_lock(&List->Lock);
	{
		if (List->Head != NULL)
			data = List->Head->Data;
	}
	globus_mutex_unlock(&List->Lock);

cleanup:
	GlobusGFSHpssDebugExit();
	return data;
}

void
list_iterate(list_t         * List,
             iterate_func_t   SearchFunc,
             void           * CallbackArg)
{
	entry_t      * entry         = NULL;
	entry_t      * next_entry    = NULL;
	list_iterate_t iterate_value = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&List->Lock);
	{
		entry = List->Head;
		while (entry != NULL)
		{
			iterate_value = SearchFunc(entry->Data, CallbackArg);

			if (iterate_value & LIST_ITERATE_REMOVE)
			{
/*
* Remove the entry from the list.
 */
				if (entry->Prev != NULL)
					entry->Prev->Next = entry->Next;
				else
					List->Head = entry->Next;

				if (entry->Next != NULL)
					entry->Next->Prev = entry->Prev;
				else
					List->Tail = entry->Prev;

				/* Save the next entry. */
				next_entry = entry->Next;

				/* Deallocate the entry. */
				globus_free(entry);

				/* Now use the next entry. */
				entry = next_entry;
			} else
				entry = entry->Next;

			if (iterate_value & LIST_ITERATE_DONE)
				break;
		}
	}
	globus_mutex_unlock(&List->Lock);

	GlobusGFSHpssDebugExit();
}

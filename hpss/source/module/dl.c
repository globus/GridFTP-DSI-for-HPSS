/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2014-2015 NCSA.  All rights reserved.
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
 * Dynamic loader related functions.
 */

/*
 * System includes
 */
#include <stdlib.h>
#include <dlfcn.h>

/*
 * Local includes
 */
#include "dl.h"

int
dl_symbol_avail(const char * Symbol)
{
	void * dl_handle    = NULL;
	void * dl_sym       = NULL;
	int    symbol_found = 0;

	dl_handle = dlopen(NULL, RTLD_NOW);
	if (dl_handle)
	{
		dl_sym = dlsym(dl_handle, Symbol);
		if (dl_sym)
			symbol_found = 1;

		dlclose(dl_handle);
	}

	return symbol_found;
}

void *
dl_find_symbol(const char * Symbol)
{
	void * dl_handle    = NULL;
	void * dl_sym       = NULL;

	dl_handle = dlopen(NULL, RTLD_NOW);
	if (dl_handle)
		dl_sym = dlsym(dl_handle, Symbol);

	return dl_sym;
}

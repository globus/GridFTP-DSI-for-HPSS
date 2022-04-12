#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>

#include <testing.h>
#include "driver.h"

static void * Module = NULL;

extern struct test_suite TEST_SUITE;
extern void * TEST_SUITE_ARG;

void *
lookup_symbol(const char * symbol_name)
{
    dlerror();
    void * handle = dlsym(Module, symbol_name);
    if (!handle)
    {
        printf("Failed to find %s: %s\n", symbol_name, dlerror());
        exit(1);
    }
    return handle;
}

int
main()
{
    //
    // Load the DSI for testing.
    //
    dlerror();
    Module = dlopen(MODULE, RTLD_LAZY);

    if (!Module)
    {
        printf("Failed to open %s: %s\n", MODULE, dlerror());
        return 1;
    }

    return !(run_suite(&TEST_SUITE, TEST_SUITE_ARG) == TEST_SUCCESS);
}

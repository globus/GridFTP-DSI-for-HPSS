#ifndef HPSS_DSI_TEST_DRIVER_H
#define HPSS_DSI_TEST_DRIVER_H

#include <testing.h>

// Helper that can be used by tests to find DSI entrypoints that the need.
// This function will call print an error message and call exit(1) if the
// symbol fails to resolve. This function will not return NULL.
void *
lookup_symbol(const char * symbol_name);

// The driver contains main() which will:
// - load the DSI module
// - run your test suite with your test suite arg
//
// You can use test_setup() to load DSI symbols.


// Test Suite Definition and Test Suite Arg
//
// Define the following global variables in your test files:
// - struct test_suite TEST_SUITE
// - void * TEST_SUITE_ARG = <some_value_as_needed>

#endif /* HPSS_DSI_TEST_DRIVER_H */

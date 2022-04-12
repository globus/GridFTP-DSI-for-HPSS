#ifndef _TESTING_H_
#define _TESTING_H_

#include <stdbool.h>

#ifndef TEST_ARG_TYPE
#define TEST_ARG_TYPE void *
#endif /* TEST_ARG_TYPE */

typedef enum {TEST_SUCCESS, TEST_FAILURE} test_status_t;

typedef test_status_t (*test_fixture)(TEST_ARG_TYPE Arg);
typedef void (*test_function)(TEST_ARG_TYPE Arg);

struct test_case {
    const char *  name;
    test_function test;
};

struct test_suite {
    test_fixture       setup; // Run before each test case
    test_fixture       teardown; // Run after each test case
    struct test_case * test_cases;
};

test_status_t
run_suite(const struct test_suite * TS, TEST_ARG_TYPE Arg);

#define ASSERT(X) Assert(__FILE__, __LINE__, __func__, #X, X)

test_status_t
Assert(const char * File,
       const int    Line,
       const char * Function,
       const char * Expression,
       const bool   Value);

#endif /* _TESTING_H_ */

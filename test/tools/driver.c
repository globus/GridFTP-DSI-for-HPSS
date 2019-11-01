#include <stdlib.h>
#include <assert.h>
#include "testing.h"
#include "mocking.h"

void
test_success(void * Arg)
{
    ASSERT(1);
}

void
test_failure(void * Arg)
{
    ASSERT(0);
}

void
test_fail_success(void * Arg)
{
    ASSERT(0);
    ASSERT(1);
}

test_status_t
fixture_success(void * Arg)
{
    return TEST_SUCCESS;
}

test_status_t
fixture_failure(void * Arg)
{
    return TEST_FAILURE;
}

int
main()
{
// test_success
    struct test_case * tc = (struct test_case[]) {
        {"test_success", test_success},
        {.name = NULL}
    };

    struct test_suite ts = {
        .setup = NULL,
        .teardown = NULL,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_SUCCESS);

// test_failure
    tc = (struct test_case[]) {
        {"test_failure", test_failure},
        {.name = NULL}
    };

    ts = (struct test_suite){
        .setup = NULL,
        .teardown = NULL,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_FAILURE);

// test_fail_success
    tc = (struct test_case[]) {
        {"test_fail_success", test_fail_success},
        {.name = NULL}
    };

    ts = (struct test_suite){
        .setup = NULL,
        .teardown = NULL,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_FAILURE);

// test_success, test_failure
    tc = (struct test_case[]) {
        {"test_success", test_success},
        {"test_failure", test_failure},
        {.name = NULL}
    };

    ts = (struct test_suite){
        .setup = NULL,
        .teardown = NULL,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_FAILURE);

// test_failure, test_success
    tc = (struct test_case[]) {
        {"test_failure", test_failure},
        {"test_success", test_success},
        {.name = NULL}
    };

    ts = (struct test_suite){
        .setup = NULL,
        .teardown = NULL,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_FAILURE);

// setup fixture_failure
    tc = (struct test_case[]) {
        {"test_success", test_success},
        {.name = NULL}
    };

    ts = (struct test_suite){
        .setup = fixture_failure,
        .teardown = NULL,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_FAILURE);

// cleanup fixture_failure
    tc = (struct test_case[]) {
        {"test_success", test_success},
        {.name = NULL}
    };

    ts = (struct test_suite){
        .setup = NULL,
        .teardown = fixture_failure,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_FAILURE);

// test_failure, setup/cleanup fixture_success
    tc = (struct test_case[]) {
        {"test_failure", test_failure},
        {.name = NULL}
    };

    ts = (struct test_suite){
        .setup = fixture_success,
        .teardown = fixture_success,
	.test_cases = tc
    };
    assert(run_suite(&ts, NULL) == TEST_FAILURE);

    return 0;
}

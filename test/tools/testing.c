#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "testing.h"
#include "_events.h"
#include "_display.h"

#define SEND_SUITE_BEGIN(TS) send_event(               \
             (struct test_event)                       \
             {                                         \
                 .event_type = TEST_EVENT_SUITE_BEGIN, \
             })

#define SEND_SUITE_FINISH(TS) send_event(               \
             (struct test_event)                        \
             {                                          \
                 .event_type = TEST_EVENT_SUITE_FINISH, \
             })

#define SEND_TESTCASE_BEGIN(TC) send_event(               \
             (struct test_event)                          \
             {                                            \
                 .event_type = TEST_EVENT_TESTCASE_BEGIN, \
                 ._u.test_case.Name = TC->name            \
             })

#define SEND_TESTCASE_FINISH(TS) send_event(               \
             (struct test_event)                           \
             {                                             \
                 .event_type = TEST_EVENT_TESTCASE_FINISH, \
                 ._u.test_case.Name = TC->name             \
             })

#define SEND_ASSERTION(FILE, LINE, FUNCTION, EXPRESSION, SUCCESS)  \
             send_event((struct test_event)                        \
             {                                                     \
                 .event_type = TEST_EVENT_ASSERTION,               \
                 ._u.assertion.File = FILE,                        \
                 ._u.assertion.Line = LINE,                        \
                 ._u.assertion.Function = FUNCTION,                \
                 ._u.assertion.Expression = EXPRESSION,            \
                 ._u.assertion.Success = SUCCESS                   \
             })

#define SEND_SETUP_FIXTURE_FAILED(TEST_NAME)                    \
             send_event((struct test_event)                     \
             {                                                  \
                 .event_type = TEST_EVENT_SETUP_FIXTURE_FAILED, \
                 ._u.fixture.TestName = TEST_NAME               \
             })

#define SEND_TEARDOWN_FIXTURE_FAILED(TEST_NAME)                    \
             send_event((struct test_event)                        \
             {                                                     \
                 .event_type = TEST_EVENT_TEARDOWN_FIXTURE_FAILED, \
                 ._u.fixture.TestName = TEST_NAME                  \
             })

static test_status_t
run_setup_fixture(test_fixture  Setup,
		  const char  * TestName,
		  TEST_ARG_TYPE Arg)
{
    test_status_t status = TEST_SUCCESS;

    if (Setup)
    {
        if (Setup(Arg) != TEST_SUCCESS)
        {
            SEND_SETUP_FIXTURE_FAILED(TestName);
            status = TEST_FAILURE;
        }
    }
    return status;
}

static test_status_t
run_teardown_fixture(test_fixture  Teardown,
		     const char  * TestName, 
		     TEST_ARG_TYPE Arg)
{
    test_status_t status = TEST_SUCCESS;

    if (Teardown)
    {
        if (Teardown(Arg) != TEST_SUCCESS)
        {
            SEND_TEARDOWN_FIXTURE_FAILED(TestName);
            status = TEST_FAILURE;
        }
    }
    return status;
}

typedef enum {GET_ASSERTION, SET_ASSERTION, RESET_ASSERTION} assertion_op_t;

static bool
assertion_status_op(assertion_op_t Op, bool Value)
{
    static bool all_assertions_successful;

    switch (Op)
    {
    case GET_ASSERTION:
        break;
    case SET_ASSERTION:
        all_assertions_successful = Value;
        break;
    case RESET_ASSERTION:
        all_assertions_successful = true;
        break;
    }
    return all_assertions_successful;
}

static void
reset_assertion_status()
{
    assertion_status_op(RESET_ASSERTION, 0);
}

static bool
get_assertion_status()
{
    return assertion_status_op(GET_ASSERTION, 0);
}

static void
update_assertion_status(bool AssertionSucceeded)
{
    if (!AssertionSucceeded)
        assertion_status_op(SET_ASSERTION, false);
}

static void
event_handler(const struct test_event * TE)
{
    if (TE->event_type == TEST_EVENT_ASSERTION)
        update_assertion_status(TE->_u.assertion.Success);
}

static test_status_t
run_test(const struct test_case * TC, TEST_ARG_TYPE Arg)
{
    SEND_TESTCASE_BEGIN(TC);

    reset_assertion_status();
    // Run the user's test case function
    TC->test(Arg);

    test_status_t status = TEST_SUCCESS;
    if (get_assertion_status() == false)
        status = TEST_FAILURE;

    SEND_TESTCASE_FINISH(TC);
    return status;
}

test_status_t
run_suite(const struct test_suite * TS, TEST_ARG_TYPE Arg)
{
    test_status_t suite_status = TEST_SUCCESS;

    // XXX I really don't like putting these here
    set_event_listener(display_event);
    set_event_listener(event_handler);

    SEND_SUITE_BEGIN(TS);

    for (int i = 0; TS->test_cases[i].name; i++)
    {
        test_status_t test_status;

        test_status = run_setup_fixture(TS->setup, TS->test_cases[i].name, Arg);
        if (test_status != TEST_SUCCESS)
        {
            suite_status = TEST_FAILURE;
            break;
        }

        if (run_test(&TS->test_cases[i], Arg) != TEST_SUCCESS)
            suite_status = TEST_FAILURE;

        test_status = run_teardown_fixture(TS->teardown,
                                           TS->test_cases[i].name,
                                           Arg);
        if (test_status != TEST_SUCCESS)
        {
            suite_status = TEST_FAILURE;
            break;
        }
    }

    SEND_SUITE_FINISH(TS);

    // XXX doesn't fit here
    clear_event_listeners();

    return suite_status;
}

test_status_t
Assert(const char * File,
       const int    Line,
       const char * Function,
       const char * Expression,
       const bool   Value)
{
    SEND_ASSERTION(File, Line, Function, Expression, Value == true);
    return (Value == true) ? TEST_SUCCESS : TEST_FAILURE;
}

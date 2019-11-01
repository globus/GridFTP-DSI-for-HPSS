#include <unistd.h>
#include <stdio.h>
#include "_events.h"

static const char * RED_TEXT    = "\033[0;31m";
static const char * GREEN_TEXT  = "\033[0;32m";
static const char * NORMAL_TEXT = "\033[0m";

static const char *
ErrorColor()
{
        if (isatty(1))
                return RED_TEXT;
        return "";
}

static const char *
SuccessColor()
{
        if (isatty(1))
                return GREEN_TEXT;
        return "";
}

static const char *
ResetColor()
{
        if (isatty(1))
                return NORMAL_TEXT;
        return "";
}

static void
print_status(const bool Success)
{
    if (Success)
        printf("%sOK%s", SuccessColor(), ResetColor());
    else
        printf("%sFAIL%s", ErrorColor(), ResetColor());
}

void
display_event(const struct test_event * TE)
{
    switch (TE->event_type)
    {
    case TEST_EVENT_SUITE_BEGIN:
        printf("=== Test Suite Begin ===\n");
        break;
    case TEST_EVENT_SETUP_FIXTURE_FAILED:
        printf("    Setup fixture failed\n");
        break;
    case TEST_EVENT_TESTCASE_BEGIN:
        printf("%s ... Running\n", TE->_u.test_case.Name);
        break;
    case TEST_EVENT_ASSERTION:
        printf("    %s: ", TE->_u.assertion.Expression);
	print_status(TE->_u.assertion.Success);
	printf("\n");
        break;
    case TEST_EVENT_TESTCASE_FINISH:
        break;
    case TEST_EVENT_TEARDOWN_FIXTURE_FAILED:
        printf("    Teardown fixture failed\n");
        break;
    case TEST_EVENT_SUITE_FINISH:
        printf("=== Test Suite complete ===\n");
        break;
    }
}

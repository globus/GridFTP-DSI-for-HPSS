#ifndef _EVENTS_H_
#define _EVENTS_H_

#include<stdbool.h>

typedef enum {
    TEST_EVENT_SUITE_BEGIN,
    TEST_EVENT_SUITE_FINISH,
    TEST_EVENT_TESTCASE_BEGIN,
    TEST_EVENT_TESTCASE_FINISH,
    TEST_EVENT_ASSERTION,
    TEST_EVENT_SETUP_FIXTURE_FAILED,
    TEST_EVENT_TEARDOWN_FIXTURE_FAILED,
} test_event_type_t;

struct test_event {
    test_event_type_t event_type;

    union {
        struct {
            const char * Name;
        } test_case;

        struct {
            const char * File;
            const int    Line;
            const char * Function;
            const char * Expression;
            const bool   Success;
        } assertion;

        struct {
            const char * TestName;
        } fixture;
    } _u;
};

void
set_event_listener(void (*handler)(const struct test_event *));

void
clear_event_listeners();

void
send_event(const struct test_event TE);

#endif /* _EVENTS_H_ */

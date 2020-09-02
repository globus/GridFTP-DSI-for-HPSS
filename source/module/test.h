#ifndef _TEST_EVENT_H_
#define _TEST_EVENT_H_

#include <inttypes.h>

typedef enum {
    TEST_EVENT_TYPE_TRANSFER_BEGIN,
    TEST_EVENT_TYPE_TRANSFER_FINISHED,
    TEST_EVENT_TYPE_PIO_RANGE_BEGIN,
    TEST_EVENT_TYPE_PIO_RANGE_COMPLETE,
    TEST_EVENT_TYPE_GETPWNAM,
    TEST_EVENT_TYPE_LOAD_DEFAULT_THREAD_STATE,
} test_event_type_t;

struct test_event {
    test_event_type_t TestEventType;

    union {
        struct pio_range_begin {
            uint64_t   Offset;
            uint64_t * Length;
        } PioRangeBegin;

        struct pio_range_complete {
            uint64_t   Offset;
            uint64_t   Length;
            uint64_t   BytesMovedIn;
            uint64_t * BytesMovedOut;
            int      * ReturnValue;
        } PioRangeComplete;

        struct passwd ** Getpwnam;
        int           *  ReturnValue;
    } _u;
};

#define TEST_EVENT_TRANSFER_BEGIN()                              \
    TestEventHandler(&(struct test_event){                       \
                 .TestEventType = TEST_EVENT_TYPE_TRANSFER_BEGIN \
              })

#define TEST_EVENT_TRANSFER_FINISHED()                              \
    TestEventHandler(&(struct test_event){                          \
                 .TestEventType = TEST_EVENT_TYPE_TRANSFER_FINISHED \
              })

#define TEST_EVENT_PIO_RANGE_BEGIN(OFFSET, LENGTH)                 \
    TestEventHandler(&(struct test_event){                         \
                 .TestEventType = TEST_EVENT_TYPE_PIO_RANGE_BEGIN, \
                 ._u.PioRangeBegin.Offset = OFFSET,                \
                 ._u.PioRangeBegin.Length = &LENGTH                \
              })

#define TEST_EVENT_PIO_RANGE_COMPLETE(OFF,LEN,BYTES_IN,BYTES_OUT,RETVAL) \
    TestEventHandler(&(struct test_event){                               \
                 .TestEventType = TEST_EVENT_TYPE_PIO_RANGE_COMPLETE,    \
                 ._u.PioRangeComplete.Offset = OFF,                      \
                 ._u.PioRangeComplete.Length = LEN,                      \
                 ._u.PioRangeComplete.BytesMovedIn = BYTES_IN,           \
                 ._u.PioRangeComplete.BytesMovedOut = BYTES_OUT,         \
                 ._u.PioRangeComplete.ReturnValue = &RETVAL              \
              })

#define TEST_EVENT_GETPWNAM(PWD)                            \
    TestEventHandler(&(struct test_event){                  \
                 .TestEventType = TEST_EVENT_TYPE_GETPWNAM, \
                 ._u.Getpwnam = PWD                         \
              })

#define TEST_EVENT_LOAD_DEFAULT_THREAD_STATE(result)                         \
    TestEventHandler(&(struct test_event){                                   \
                 .TestEventType = TEST_EVENT_TYPE_LOAD_DEFAULT_THREAD_STATE, \
                 ._u.ReturnValue = &result                                   \
              })

void
TestEventHandler(struct test_event * TestEvent);

#endif /* _TEST_EVENT_H_ */

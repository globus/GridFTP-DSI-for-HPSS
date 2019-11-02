#ifndef _TEST_EVENT_H_
#define _TEST_EVENT_H_

#include <inttypes.h>

typedef enum {
    TEST_EVENT_TYPE_TRANSFER_BEGIN,
    TEST_EVENT_TYPE_TRANSFER_FINISHED,
    TEST_EVENT_TYPE_PIO_RANGE_BEGIN,
    TEST_EVENT_TYPE_PIO_RANGE_COMPLETE,
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
            uint64_t * BytesMoved;
            int      * ReturnValue;
        } PioRangeComplete;
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

#define TEST_EVENT_PIO_RANGE_COMPLETE(OFFSET,LENGTH,BYTES_MOVED,RETURN_VALUE) \
    TestEventHandler(&(struct test_event){                                    \
                 .TestEventType = TEST_EVENT_TYPE_PIO_RANGE_COMPLETE,         \
                 ._u.PioRangeComplete.Offset = OFFSET,                        \
                 ._u.PioRangeComplete.Length = LENGTH,                        \
                 ._u.PioRangeComplete.BytesMoved = BYTES_MOVED,               \
                 ._u.PioRangeComplete.ReturnValue = &RETURN_VALUE             \
              })

void
TestEventHandler(struct test_event * TestEvent);

#endif /* _TEST_EVENT_H_ */

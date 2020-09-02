/* System includes. */
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

/* HPSS includes. */
#include <hpss_errno.h>

/* Local includes. */
#include "logging.h"
#include "test.h"

#define INJECTED_ERROR_VALUE HPSS_EFAULT

struct {
    bool TestsEnabled;
    bool TransferInProgress;
    bool InjectThisTransfer;

    enum {
        INJECT_TRANSFER_FAILURE = 1,
        INJECT_BAD_RESTART_MARKER,
        INJECT_BZ4719,
        INJECT_UNKNOWN_ACCOUNT,
        INJECT_LOGIN_FAILED
    } Flags;
} static TestPlan;

static pthread_once_t TestsInitialized = PTHREAD_ONCE_INIT;

static uint64_t
Random64()
{
    return ((((uint64_t)rand()) << 32) | ((uint64_t)rand()));
}

static uint64_t
Random64InRange(uint64_t Offset, uint64_t Length)
{
    return ((Random64() % (Length + 1)) + Offset);
}

static void
TestInit()
{
    srand(time(NULL));

    const char * plan = getenv("HPSS_DSI_TEST");
    if (!plan)
        return;

    TestPlan.Flags = 0;

    if (strcmp(plan, "INJECT_TRANSFER_FAILURE") == 0)
        TestPlan.Flags = INJECT_TRANSFER_FAILURE;
    else if (strcmp(plan, "INJECT_BAD_RESTART_MARKER") == 0)
        TestPlan.Flags = INJECT_BAD_RESTART_MARKER;
    else if (strcmp(plan, "INJECT_BZ4719") == 0)
        TestPlan.Flags = INJECT_BZ4719;
    else if (strcmp(plan, "INJECT_UNKNOWN_ACCOUNT") == 0)
        TestPlan.Flags = INJECT_UNKNOWN_ACCOUNT;
    else if (strcmp(plan, "INJECT_LOGIN_FAILED") == 0)
        TestPlan.Flags = INJECT_LOGIN_FAILED;

    if (TestPlan.Flags != 0)
    {
        TestPlan.TestsEnabled = true;
        WARN("!!!DSI testing is enabled!!!");
    }
}

static bool
IsRestartAttempt(uint64_t Offset, uint64_t Length)
{
    return (Offset != 0);
}

static bool
RangeIsTooSmall(uint64_t Offset, uint64_t Length)
{
    const uint64_t min_file_size = 8;
    return (Length < min_file_size);
}

static void
TestEventRangeBegin(struct pio_range_begin * PioRangeBegin)
{
    if (!TestPlan.TransferInProgress)
        return;

    if (IsRestartAttempt(PioRangeBegin->Offset, *PioRangeBegin->Length))
        return;

    if (RangeIsTooSmall(PioRangeBegin->Offset, *PioRangeBegin->Length))
        return;

    TestPlan.InjectThisTransfer = true;

    if (TestPlan.Flags)
    {
        // We want the error to happen somewhere between 1/4 - 3/4 of the
        // transfer. Errors at 0 don't demonstrate much and errors at the
        // end of the file won't allow for bad restart marker testing.
        uint64_t offset = *PioRangeBegin->Length / 4;
        uint64_t length = *PioRangeBegin->Length / 2;

        // This will cause hpss_PIOExecute() to kick out at new offset
        // 'PioRangeBegin->Length' which is less than what the caller
        // intended. This gives us the chance to inject an error before
        // the transfer completes.
        *PioRangeBegin->Length = Random64InRange(offset, length);

        WARN("Current transfer will be truncated to %lu bytes for "
             "testing purposes.", *PioRangeBegin->Length);
    }
}

static bool
TransferFailedOnItsOwn(int ReturnValue)
{
    return (ReturnValue != HPSS_E_NOERROR);
}

static void
TestEventRangeComplete(struct pio_range_complete * PioRangeComplete)
{
    if (!TestPlan.InjectThisTransfer)
        return;

    TestPlan.InjectThisTransfer = false;

    if (TransferFailedOnItsOwn(*PioRangeComplete->ReturnValue))
        return;

    if (TestPlan.Flags == INJECT_TRANSFER_FAILURE)
    {
        WARN("Injecting failure into current transfer");
        *PioRangeComplete->ReturnValue = INJECTED_ERROR_VALUE;
    } else if (TestPlan.Flags == INJECT_BAD_RESTART_MARKER)
    {
        // We know we transferred upto 3/4 of the file at this point.
        // So we can adjust BytesMoved up to, at most, another 1/4
        // of the file. Since we don't know the original length,
        // we'll base it on 1/3 of *PioRangeComplete->Length.
        uint64_t offset = *PioRangeComplete->BytesMovedOut;
        uint64_t length = PioRangeComplete->Length / 3;

        *PioRangeComplete->BytesMovedOut = Random64InRange(offset, length);
        *PioRangeComplete->ReturnValue = INJECTED_ERROR_VALUE;

        WARN("Injecting bad restart marker at offset %lu",
              *PioRangeComplete->BytesMovedOut);
    } else if (TestPlan.Flags == INJECT_BZ4719)
    {
        // Leave BytesMoved unchanged.
        *PioRangeComplete->BytesMovedOut = PioRangeComplete->BytesMovedIn;
        *PioRangeComplete->ReturnValue = INJECTED_ERROR_VALUE;
        WARN("Injecting BZ4719");
    }
}

static void
TestEventTransferBegin()
{
    TestPlan.TransferInProgress = true;
}

static void
TestEventTransferFinished()
{
    TestPlan.TransferInProgress = false;
}


static void
TestEventGetpwnam(struct passwd ** pwd)
{
    if (TestPlan.TestsEnabled == false)
        return;

    if (TestPlan.Flags != INJECT_UNKNOWN_ACCOUNT)
        return;

    *pwd = NULL;
}

static void
TestEventLoadDefaultThreadState(int * Retval)
{
    if (TestPlan.TestsEnabled == false)
        return;

    if (TestPlan.Flags != INJECT_LOGIN_FAILED)
        return;

    *Retval = -EPERM;
}

void
TestEventHandler(struct test_event * TestEvent)
{
    pthread_once(&TestsInitialized, TestInit);
    if (!TestPlan.TestsEnabled)
        return;

    switch (TestEvent->TestEventType)
    {
    case TEST_EVENT_TYPE_TRANSFER_BEGIN:
        TestEventTransferBegin();
        break;
    case TEST_EVENT_TYPE_TRANSFER_FINISHED:
        TestEventTransferFinished();
        break;
    case TEST_EVENT_TYPE_PIO_RANGE_BEGIN:
        TestEventRangeBegin(&TestEvent->_u.PioRangeBegin);
        break;
    case TEST_EVENT_TYPE_PIO_RANGE_COMPLETE:
        TestEventRangeComplete(&TestEvent->_u.PioRangeComplete);
        break;
    case TEST_EVENT_TYPE_GETPWNAM:
        TestEventGetpwnam(TestEvent->_u.Getpwnam);
        break;
    case TEST_EVENT_TYPE_LOAD_DEFAULT_THREAD_STATE:
        TestEventLoadDefaultThreadState(TestEvent->_u.ReturnValue);
        break;
    }
}

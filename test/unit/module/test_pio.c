#include <pio.h>
#include <stdlib.h>

#include <testing.h>
#include <mocking.h>
#include "driver.h"

#define CREATE_SHORTCUT(Name, Value) typeof((Value)) Name = (Value)
#define RANGE(O, L) &(struct range){O, L}
#define RANGES(...) (struct range *[]){__VA_ARGS__, NULL}

void (*pio_coordinator_thread)(void * Arg) = NULL;

struct range {
    globus_off_t Offset;
    globus_off_t Length;
};

uint64_t
sum_ranges(struct range ** Ranges)
{
    uint64_t length = 0;

    for (int i = 0; Ranges && Ranges[i]; i++)
    {
        length += Ranges[i]->Length;
    }
    return length;
}

void
add_range(struct range *** Ranges, globus_off_t Offset, globus_off_t Length)
{
    int count = 0;
    while (*Ranges && (*Ranges)[count]) count++;

    *Ranges = realloc(*Ranges, (count+2)*sizeof(**Ranges));

    (*Ranges)[count] = calloc(1, sizeof(***Ranges));
    (*Ranges)[count]->Offset = Offset;
    (*Ranges)[count]->Length = Length;
    (*Ranges)[count+1] = NULL;
}

void
destroy_ranges(struct range ** Ranges)
{
    for (int i = 0; Ranges && Ranges[i]; i++)
    {
        free(Ranges[i]);
    }
    free(Ranges);
}

uint64_t
min(uint64_t X, uint64_t Y)
{
    if (X < Y)
        return X;
    return Y;
}

bool
match_ranges(struct range ** R1, struct range ** R2)
{
    int i1 = 0;
    int i2 = 0;

    while ((R1 && R1[i1]) || (R2 && R2[i2]))
    {
        uint64_t r1_off = 0;
        uint64_t r1_len = 0;

        while (R1 && R1[i1] && R1[i1]->Length == 0) i1++;
        if (R1 && R1[i1])
        {
            r1_off = R1[i1]->Offset;
            r1_len = R1[i1]->Length;
            i1++;
        }

        uint64_t r2_off = 0;
        uint64_t r2_len = 0;

        while (R2 && R2[i2] && R2[i2]->Length == 0) i2++;
        if (R2 && R2[i2])
        {
            r2_off = R2[i2]->Offset;
            r2_len = R2[i2]->Length;
            i2++;
        }

        if (r1_off != r2_off || r1_len != r2_len)
            return false;
    }
    return true;
}

bool
compare_ranges(struct range ** R1, struct range ** R2)
{
    int i1 = 0;
    uint64_t r1_off = 0;
    uint64_t r1_len = 0;

    int i2 = 0;
    uint64_t r2_off = 0;
    uint64_t r2_len = 0;

    while ((R1 && R1[i1]) || (R2 && R2[i2]))
    {
        if (r1_len == 0 && R1 && R1[i1])
        {
            r1_off = R1[i1]->Offset;
            r1_len = R1[i1++]->Length;
        }

        if (r2_len == 0 && R2 && R2[i2])
        {
            r2_off = R2[i2]->Offset;
            r2_len = R2[i2++]->Length;
        }

        if (r1_off != r2_off)
            return false;

        uint64_t len = min(r2_len, r1_len);
        r1_off += len;
        r1_len -= len;
        r2_off += len;
        r2_len -= len;
    }
    return (r1_len == 0 && r2_len == 0);
}

struct test_pio {
    struct {
        struct {
            uint64_t Offset;
            uint64_t Length;
        } PIOCallback;

        struct {
            pio_t Pio;
        } PIOCoordinatorThread;
    } Input;

    struct {
        struct {
            struct range ** Ranges;
        } PIOCallback;
    } Output;
};

bool
pio_callback_range(struct test_pio * TestPIO, struct range ** Ranges)
{
    return compare_ranges(TestPIO->Output.PIOCallback.Ranges, Ranges);
}

bool
pio_restart_range(struct test_pio * TestPIO, struct range ** Ranges)
{
    return match_ranges(TestPIO->Output.PIOCallback.Ranges, Ranges);
}

void
pio_range_callback(globus_off_t * Offset,
                   globus_off_t * Length,
                   int          * Eot,
                   void         * UserArg)
{
    struct test_pio * test_pio = UserArg;

    // Store this completed range in our user arg's output
    add_range(&test_pio->Output.PIOCallback.Ranges, *Offset, *Length);

    // Adjust for the remaining offset and range
    uint64_t length_so_far = sum_ranges(test_pio->Output.PIOCallback.Ranges);
    *Length = test_pio->Input.PIOCallback.Length - length_so_far;
    *Offset = test_pio->Input.PIOCallback.Offset + length_so_far;

    if (*Length == 0)
        *Eot = 1;
}

void
globus_i_GLOBUS_GRIDFTP_SERVER_HPSS_debug_printf(const char * fmt, ...)
{
assert(0);
}

test_status_t
test_setup(void * Arg)
{
    if (!pio_coordinator_thread)
        pio_coordinator_thread = lookup_symbol("pio_coordinator_thread");

    struct test_pio * test_pio = Arg;

    memset(test_pio, 0, sizeof(*test_pio));

    test_pio->Input.PIOCoordinatorThread.Pio.RngCmpltCB = pio_range_callback;
    test_pio->Input.PIOCoordinatorThread.Pio.UserArg = test_pio;

    return TEST_SUCCESS;
}

test_status_t
test_teardown(void * Arg)
{
    struct test_pio * test_pio = Arg;
    destroy_ranges(test_pio->Output.PIOCallback.Ranges);
    return TEST_SUCCESS;
}

// error because of we can't handle gaps [gaps are handled!]
   // Does it do it correctly? Doesn't look like it
// Log INFO restart
// Log WARN bad bytes_moved
// Log INFO gap
// 
// testing bad bytes_moved
// testing detection of bad restart markers / gap

void
test_single_range(void * Arg)
{
    struct test_pio * test_pio = Arg;

    test_pio->Input.PIOCoordinatorThread.Pio.InitialOffset = 0;
    test_pio->Input.PIOCoordinatorThread.Pio.InitialLength = 1024;

    test_pio->Input.PIOCallback.Offset = 0;
    test_pio->Input.PIOCallback.Length = 1024;

    CREATE_SHORTCUT(pio, &test_pio->Input.PIOCoordinatorThread.Pio);

    EXPECT_PARAMS("hpss_PIOExecute", WHEN_ONCE, 
                  "FileOffset", UINT64, 0,
                  "Size",       UINT64, (uint64_t)1024);

    EXPECT_RETURN("hpss_PIOExecute", WHEN_ONCE, INT, 0,
                  "BytesMoved", UINT64, (uint64_t)1024);

    pio_coordinator_thread(pio);

    ASSERT(pio->CoordinatorResult == GLOBUS_SUCCESS);
    ASSERT(pio_callback_range(test_pio, RANGES(RANGE(0, 1024))));
    ASSERT(pio_restart_range(test_pio,  RANGES(RANGE(0, 1024))));
}

void
test_multi_range(void * Arg)
{
    struct test_pio * test_pio = Arg;

    test_pio->Input.PIOCoordinatorThread.Pio.InitialOffset = 0;
    test_pio->Input.PIOCoordinatorThread.Pio.InitialLength = 1024;

    test_pio->Input.PIOCallback.Offset = 0;
    test_pio->Input.PIOCallback.Length = 1024;

    CREATE_SHORTCUT(pio, &test_pio->Input.PIOCoordinatorThread.Pio);

    EXPECT_PARAMS("hpss_PIOExecute", WHEN_ONCE, 
                  "FileOffset", UINT64, (uint64_t)0,
                  "Size",       UINT64, (uint64_t)1024);

    EXPECT_RETURN("hpss_PIOExecute", WHEN_ONCE, INT, 0,
                  "BytesMoved", UINT64, (uint64_t)512);

    EXPECT_PARAMS("hpss_PIOExecute", WHEN_ONCE, 
                  "FileOffset", UINT64, (uint64_t)512,
                  "Size",       UINT64, (uint64_t)512);

    EXPECT_RETURN("hpss_PIOExecute", WHEN_ONCE, INT, 0,
                  "BytesMoved", UINT64, (uint64_t)512);

    pio_coordinator_thread(pio);

    ASSERT(pio->CoordinatorResult == GLOBUS_SUCCESS);
    ASSERT(pio_callback_range(test_pio, RANGES(RANGE(0, 1024))));
    ASSERT(pio_restart_range(test_pio,  RANGES(RANGE(0, 512),
                                               RANGE(512, 512))));
}

void
test_good_bytes_moved(void * Arg)
{
    struct test_pio * test_pio = Arg;

    test_pio->Input.PIOCoordinatorThread.Pio.InitialOffset = 0;
    test_pio->Input.PIOCoordinatorThread.Pio.InitialLength = 1024;

    test_pio->Input.PIOCallback.Offset = 0;
    test_pio->Input.PIOCallback.Length = 1024;

    CREATE_SHORTCUT(pio, &test_pio->Input.PIOCoordinatorThread.Pio);

    EXPECT_PARAMS("hpss_PIOExecute", WHEN_ONCE, 
                  "FileOffset", UINT64, (uint64_t)0,
                  "Size",       UINT64, (uint64_t)1024);

    EXPECT_RETURN("hpss_PIOExecute", WHEN_ONCE, INT, -5,
                  "BytesMoved", UINT64, (uint64_t)512);

    pio_coordinator_thread(pio);

    ASSERT(pio->CoordinatorResult != GLOBUS_SUCCESS);
    ASSERT(pio_callback_range(test_pio, RANGES(RANGE(0, 512))));
    ASSERT(pio_restart_range(test_pio,  RANGES(RANGE(0, 512))));
}

void
test_bad_bytes_moved(void * Arg)
{
    struct test_pio * test_pio = Arg;

    test_pio->Input.PIOCoordinatorThread.Pio.InitialOffset = 0;
    test_pio->Input.PIOCoordinatorThread.Pio.InitialLength = 1024;

    test_pio->Input.PIOCallback.Offset = 0;
    test_pio->Input.PIOCallback.Length = 1024;

    CREATE_SHORTCUT(pio, &test_pio->Input.PIOCoordinatorThread.Pio);

    EXPECT_PARAMS("hpss_PIOExecute", WHEN_ONCE, 
                  "FileOffset", UINT64, (uint64_t)0,
                  "Size",       UINT64, (uint64_t)1024);

    EXPECT_RETURN("hpss_PIOExecute", WHEN_ONCE, INT, -5);

    pio_coordinator_thread(pio);

    ASSERT(pio->CoordinatorResult != GLOBUS_SUCCESS);
    ASSERT(pio_callback_range(test_pio, RANGES(RANGE(0, 0))));
    ASSERT(pio_restart_range(test_pio,  NULL));
// XXX check for log message too
}

struct test_suite TEST_SUITE = {
    .setup = test_setup,
    .teardown = test_teardown,
    .test_cases = (struct test_case[]) {
        {"test_single_range",     test_single_range},
        {"test_multi_range",      test_multi_range},
        {"test_good_bytes_moved", test_good_bytes_moved},
        {"test_bad_bytes_moved",  test_bad_bytes_moved},
        {.name = NULL}
    }
};

static struct test_pio test_pio;

void * TEST_SUITE_ARG = &test_pio;

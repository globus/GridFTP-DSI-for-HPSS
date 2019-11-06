#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <hpss_mech.h>
#include <hpss_api.h>

struct transfer;
#define TEST_ARG_TYPE struct transfer *
#include <testing.h>

#define CREATE_SHORTCUT(Name, Value) typeof((Value)) Name = (Value)

static const uint64_t NoValue64 = 0xDEADBEEFDEADBEEF;
static const uint32_t NoValue32 = 0xDEADBEEF;

struct range {
    uint64_t Offset;
    uint64_t Length;
};

// This is not really "summing ranges" so much as it is trying to find the
// largest offset so it can feed that to hints_in.MaxFileSize. Rename it.
// XXX
uint64_t
sum_ranges(struct range ** Ranges)
{
    uint64_t largest_offset = 0;

    for (int i = 0; Ranges && Ranges[i]; i++)
    {
        if ((Ranges[i]->Offset + Ranges[i]->Length) > largest_offset)
            largest_offset = (Ranges[i]->Offset + Ranges[i]->Length);
    }

    return largest_offset;
}

void
open_file_for_read(const char * FilePath, int * FileDesc, int * FileStripeWidth)
{
    hpss_cos_hints_t hints_in;
    hpss_cos_priorities_t priorities;
    hpss_cos_hints_t hints_out;

    memset(&hints_in,   0, sizeof(hints_in));
    memset(&priorities, 0, sizeof(priorities));
    memset(&hints_out,  0, sizeof(hints_out));

    *FileDesc = hpss_Open((char *)FilePath,
                          O_RDONLY,
                          S_IRUSR|S_IWUSR, 
                          &hints_in, 
                          &priorities, 
                          &hints_out);

    if (*FileDesc > 0)
    {
        printf("hpss_Open() failed\n");
        assert(0);
    }
    *FileStripeWidth = hints_out.StripeWidth;
}

void
open_file_for_write(const char * FilePath,
                    uint64_t     AlloSize,
                    int        * FileDesc, 
                    int        * FileStripeWidth)
{
    hpss_cos_hints_t hints_in;
    hpss_cos_priorities_t priorities;
    hpss_cos_hints_t hints_out;

    memset(&hints_in,   0, sizeof(hints_in));
    memset(&priorities, 0, sizeof(priorities));
    memset(&hints_out,  0, sizeof(hints_out));

    hints_in.MinFileSize = AlloSize;
    hints_in.MaxFileSize = AlloSize;
    priorities.MinFileSizePriority = REQUIRED_PRIORITY;
    priorities.MaxFileSizePriority = HIGHLY_DESIRED_PRIORITY;

    *FileDesc = hpss_Open((char *)FilePath,
                          O_WRONLY|O_CREAT|O_TRUNC, 
                          S_IRUSR | S_IWUSR,
                          &hints_in,
                          &priorities,
                          &hints_out);

    if (*FileDesc > 0)
    {
        printf("hpss_Open() failed\n");
        assert(0);
    }
    *FileStripeWidth = hints_out.StripeWidth;
}

hpss_pio_grp_t
create_stripe_group(hpss_pio_operation_t Operation,
                    uint32_t             BlockSize,
                    uint32_t             FileStripeWidth,
                    int                  Options)
{
    hpss_pio_params_t pio_params;
    pio_params.Operation = Operation;
    pio_params.ClntStripeWidth = 1;
    pio_params.BlockSize = BlockSize;
    pio_params.FileStripeWidth = FileStripeWidth;
    pio_params.IOTimeOutSecs = 0;
    pio_params.Transport = HPSS_PIO_TCPIP;
    pio_params.Options = Options;

    hpss_pio_grp_t stripe_group;
    int retval = hpss_PIOStart(&pio_params, &stripe_group);
    assert(retval == 0);
    return stripe_group;
}

hpss_pio_grp_t
copy_stripe_group(hpss_pio_grp_t StripeGroupIn)
{
    void * buffer;
    unsigned buffer_length;
    int retval = hpss_PIOExportGrp(StripeGroupIn, &buffer, &buffer_length);
    if (retval)
    {
        printf("hpss_PIOExportGrp() failed\n");
        assert(0);
    }

    hpss_pio_grp_t stripe_group_out;
    retval = hpss_PIOImportGrp(buffer, buffer_length, &stripe_group_out);
    if (retval)
    {
        printf("hpss_PIOImportGrp() failed\n");
        assert(0);
    }

    free(buffer);
    return stripe_group_out;
}

struct transfer_request {
    struct {
        uint32_t BlockSize;
        int      Options;
    } Coordinator;

    struct {
        uint32_t BufferLength;
        int      CalloutReturnValue;
    } Participant;
};

struct transfer_request
default_transfer_request()
{
    struct transfer_request request;
    memset(&request, 0, sizeof(request));

    request.Coordinator.BlockSize = (1024*1024);
    request.Coordinator.Options = 0;
    request.Participant.BufferLength = request.Coordinator.BlockSize;
    request.Participant.CalloutReturnValue = 0;

    return request;
}

struct pio_coordinator {
    struct {
        struct {
            int            FileDesc;
            hpss_pio_grp_t StripeGroup;

            struct range ** Ranges;
        } hpss_pio_execute;
    } Input;

    struct {
        struct hpss_pio_execute {
            int                ReturnValue;
            uint64_t           Offset;
            uint64_t           Length;
            uint64_t           BytesMoved;
            hpss_pio_gapinfo_t GapInfo;
        } ** hpss_pio_execute;

        struct {
            int ReturnValue;
        } hpss_pio_end;

        struct {
            int ReturnValue; // Only ever returns HPSS_E_NOERROR
        } hpss_close;
    } Output;
};

struct pio_coordinator
default_pio_coordinator()
{
    struct pio_coordinator coordinator;
    memset(&coordinator, 0, sizeof(coordinator));
    coordinator.Output.hpss_pio_end.ReturnValue = NoValue32;
    coordinator.Output.hpss_close.ReturnValue = NoValue32;
    return coordinator;
}

struct pio_coordinator
create_pio_coordinator(struct transfer_request * Request,
                       const char              * FilePath,
                       hpss_pio_operation_t      Operation,
                       struct range           ** Ranges)
{
    struct pio_coordinator coordinator = default_pio_coordinator();

    uint64_t allo_size = sum_ranges(Ranges);

    int file_stripe_width;
    switch (Operation)
    {
    case HPSS_PIO_READ:
        open_file_for_read(FilePath,
                           &coordinator.Input.hpss_pio_execute.FileDesc, 
                           &file_stripe_width);
        break;

    case HPSS_PIO_WRITE:
        open_file_for_write(FilePath,
                            allo_size, 
                            &coordinator.Input.hpss_pio_execute.FileDesc, 
                            &file_stripe_width);
        break;
    }

    coordinator.Input.hpss_pio_execute.StripeGroup = 
                           create_stripe_group(Operation,
                                               Request->Coordinator.BlockSize, 
                                               file_stripe_width,
                                               Request->Coordinator.Options);

    coordinator.Input.hpss_pio_execute.Ranges = Ranges;
    return coordinator;
}

void
delete_coordinator(struct pio_coordinator * Coordinator)
{
    CREATE_SHORTCUT(hpss_pio_execute, Coordinator->Output.hpss_pio_execute);

    for (int i = 0; hpss_pio_execute && hpss_pio_execute[i]; i++)
    {
        free(hpss_pio_execute[i]);
    }
    free(hpss_pio_execute);
}

static void
add_pio_execute_exit_values(int                         ReturnValue,
                            uint64_t                    Offset,
                            uint64_t                    Length,
                            uint64_t                    BytesMoved,
                            hpss_pio_gapinfo_t        * GapInfo,
                            struct hpss_pio_execute *** PIOExecuteReturnValues)
{
    struct hpss_pio_execute * new_return_values;
    new_return_values = calloc(1, sizeof(*new_return_values));

    new_return_values->ReturnValue = ReturnValue;
    new_return_values->Offset = Offset;
    new_return_values->Length = Length;
    new_return_values->BytesMoved = BytesMoved;
    new_return_values->GapInfo = *GapInfo;

    int count = 0;
    if (*PIOExecuteReturnValues)
        while ((*PIOExecuteReturnValues)[count]) count++;

    size_t size = sizeof(**PIOExecuteReturnValues)*(count+2);
    *PIOExecuteReturnValues = realloc(*PIOExecuteReturnValues, size);

    (*PIOExecuteReturnValues)[count] = new_return_values;
    (*PIOExecuteReturnValues)[count+1] = NULL;
}

void *
pio_coordinator(void * Arg)
{
    struct pio_coordinator * coordinator= Arg;

    CREATE_SHORTCUT(inputs,  &coordinator->Input);
    CREATE_SHORTCUT(outputs, &coordinator->Output);

    CREATE_SHORTCUT(pio_execute_in,  &inputs->hpss_pio_execute);
    CREATE_SHORTCUT(pio_execute_out, &outputs->hpss_pio_execute);
    CREATE_SHORTCUT(pio_end_out,    &outputs->hpss_pio_end);
    CREATE_SHORTCUT(hpss_close_out, &outputs->hpss_close);

    for (int index = 0; pio_execute_in->Ranges[index]; index++)
    {
        int64_t offset = pio_execute_in->Ranges[index]->Offset;
        int64_t length = pio_execute_in->Ranges[index]->Length;

        do
        {
            hpss_pio_gapinfo_t gap_info = {NoValue64, NoValue64};
            uint64_t bytes_moved = NoValue64;

            int retval = hpss_PIOExecute(pio_execute_in->FileDesc,
                                         offset,
                                         length,
                                         pio_execute_in->StripeGroup,
                                         &gap_info,
                                         &bytes_moved);

            add_pio_execute_exit_values(retval,
                                        offset,
                                        length,
                                        bytes_moved,
                                        &gap_info,
                                        pio_execute_out);

// gap.offset will equal bytes_moved, gap.length might be > length
            if (retval != HPSS_E_NOERROR)
                goto error_handler;


            length -= bytes_moved;
            offset += bytes_moved;

            if (gap_info.Length != NoValue64)
            {
// XXX This could roll length over since it is unsigned
                length -= gap_info.Length;
                offset += gap_info.Length;
            }
        } while (length > 0);
    }

error_handler:
    pio_end_out->ReturnValue = hpss_PIOEnd(pio_execute_in->StripeGroup);
    hpss_close_out->ReturnValue = hpss_Close(pio_execute_in->FileDesc);
    return NULL;
}

struct pio_participant {
    struct {
        struct {
            uint32_t       BufferLength;
            char         * Buffer;
            hpss_pio_grp_t StripeGroup;
        } hpss_pio_register;

        struct {
            int ReturnValue;
        } pio_callout;
    } Input;

    struct {
        struct {
            int ReturnValue;
        } hpss_pio_register;

        struct pio_callout {
            uint64_t Offset;
            unsigned Length;
        } ** pio_callout;

        struct {
            int ReturnValue; // Only ever returns HPSS_E_NOERROR
        } hpss_pio_end;
    } Output;
};

struct pio_participant
default_pio_participant()
{
    struct pio_participant participant;
    memset(&participant, 0, sizeof(participant));
    participant.Output.hpss_pio_register.ReturnValue = NoValue32;
    participant.Output.hpss_pio_end.ReturnValue = NoValue32;
    return participant;
}

struct pio_participant
create_pio_participant(struct transfer_request * Request,
                       hpss_pio_grp_t            StripeGroup)
{
    struct pio_participant participant = default_pio_participant();

    CREATE_SHORTCUT(pio_register, &participant.Input.hpss_pio_register);
    pio_register->BufferLength = Request->Participant.BufferLength;
    pio_register->Buffer = calloc(1, pio_register->BufferLength);
    pio_register->StripeGroup = copy_stripe_group(StripeGroup);

    CREATE_SHORTCUT(pio_callout, &participant.Input.pio_callout);
    pio_callout->ReturnValue = Request->Participant.CalloutReturnValue;

    return participant;
}

void
delete_participant(struct pio_participant * Participant)
{
    CREATE_SHORTCUT(pio_callout, Participant->Output.pio_callout);

    for (int i = 0; pio_callout && pio_callout[i]; i++)
    {
        free(pio_callout[i]);
    }
    free(pio_callout);
    free(Participant->Input.hpss_pio_register.Buffer);
}

static void
add_pio_callout_values(uint64_t               Offset,
                       unsigned               Length,
                       struct pio_callout *** PIOCalloutRanges)
{
    struct pio_callout * new_range;
    new_range = calloc(1, sizeof(*new_range));

    new_range->Offset = Offset;
    new_range->Length = Length;

    int count = 0;
    if (*PIOCalloutRanges)
        while ((*PIOCalloutRanges)[count]) count++;

    size_t size = sizeof(**PIOCalloutRanges)*(count+2);
    *PIOCalloutRanges = realloc(*PIOCalloutRanges, size);

    (*PIOCalloutRanges)[count] = new_range;
    (*PIOCalloutRanges)[count+1] = NULL;
}

int
pio_callout(void * Arg, uint64_t Offset, unsigned * Length, void ** Buffer)
{
    struct pio_participant * participant = Arg;

    CREATE_SHORTCUT(pio_callout_in,  &participant->Input.pio_callout);
    CREATE_SHORTCUT(pio_callout_out, &participant->Output.pio_callout);

    add_pio_callout_values(Offset, *Length, pio_callout_out);

    /*
     * This is a stupid behavior of the callout; *Buffer is NULL on the first
     * callout during a STOR,even though we passed a buffer into 
     * hpss_PIORegister().
     */
    if (!*Buffer)
        *Buffer = participant->Input.hpss_pio_register.Buffer;

    return pio_callout_in->ReturnValue;
}

void *
pio_participant(void * Arg)
{
    struct pio_participant * participant = Arg;

    CREATE_SHORTCUT(pio_register_in,  &participant->Input.hpss_pio_register);
    CREATE_SHORTCUT(pio_register_out, &participant->Output.hpss_pio_register);
    CREATE_SHORTCUT(pio_end_out,      &participant->Output.hpss_pio_end);

    int retval = hpss_PIORegister(0, // Stripe Element
                                  NULL, // Data Net Sock Addr
                                  pio_register_in->Buffer,
                                  pio_register_in->BufferLength,
                                  pio_register_in->StripeGroup,
                                  pio_callout,
                                  participant);
    pio_register_out->ReturnValue = retval;

    pio_end_out->ReturnValue = hpss_PIOEnd(pio_register_in->StripeGroup);
    return NULL;
}

struct transfer {
    struct transfer_request Request;
    struct pio_coordinator  Coordinator;
    struct pio_participant  Participant;
};

void
perform_transfer(struct transfer    * Transfer,
                 const char         * FilePath,
                 hpss_pio_operation_t Operation,
                 struct range      ** Ranges)
{
    CREATE_SHORTCUT(request,     &Transfer->Request);
    CREATE_SHORTCUT(coordinator, &Transfer->Coordinator);
    CREATE_SHORTCUT(participant, &Transfer->Participant);

    *coordinator = create_pio_coordinator(request, FilePath, Operation, Ranges);

    CREATE_SHORTCUT(stripe_group,
                    coordinator->Input.hpss_pio_execute.StripeGroup);
    *participant = create_pio_participant(request, stripe_group);

    pthread_t coordinator_thread_id;
    int retval = pthread_create(&coordinator_thread_id,
                                NULL,
                                pio_coordinator,
                                coordinator);
    assert(retval == 0);

    pthread_t participant_thread_id;
    retval = pthread_create(&participant_thread_id,
                            NULL,
                            pio_participant,
                            participant);
    assert(retval == 0);

    pthread_join(coordinator_thread_id, NULL);
    pthread_join(participant_thread_id, NULL);
}

struct transfer
initialize_transfer()
{
    struct transfer transfer;
    memset(&transfer, 0, sizeof(transfer));
    transfer.Request = default_transfer_request();
    return transfer;
}

void
cleanup_transfer(struct transfer * Transfer)
{
    delete_coordinator(&Transfer->Coordinator);
    delete_participant(&Transfer->Participant);
}

void
quick_transfer(const char         * FilePath,
               hpss_pio_operation_t Operation,
               struct range      ** Ranges)
{
    struct transfer transfer = initialize_transfer();
    perform_transfer(&transfer, FilePath, Operation, Ranges);
    cleanup_transfer(&transfer);
}

test_status_t
test_setup(struct transfer * Transfer)
{
   *Transfer = initialize_transfer();
    memset(Transfer, 0, sizeof(*Transfer));
    Transfer->Request = default_transfer_request();

    return TEST_SUCCESS;
}

test_status_t
test_cleanup(struct transfer * Transfer)
{
    cleanup_transfer(Transfer);
    return TEST_SUCCESS;
}

#define RANGE(O, L) &(struct range){O, L}
#define RANGES(...) (struct range *[]){__VA_ARGS__, NULL}

bool
gap_info_not_set(struct transfer * Transfer)
{
    CREATE_SHORTCUT(pio_execute, Transfer->Coordinator.Output.hpss_pio_execute);

    for (int i = 0; pio_execute && pio_execute[i]; i++)
    {
        if (pio_execute[i]->GapInfo.Offset != NoValue64)
            return false;
        if (pio_execute[i]->GapInfo.Length != NoValue64)
            return false;
    }
    return true;
}

bool
gap_info_set_to(struct transfer * Transfer, uint64_t Offset, uint64_t Length)
{
    CREATE_SHORTCUT(pio_execute, Transfer->Coordinator.Output.hpss_pio_execute);

    for (int i = 0; pio_execute && pio_execute[i]; i++)
    {
        if (pio_execute[i]->GapInfo.Offset == Offset)
        {
            if (pio_execute[i]->GapInfo.Length == Length)
                return true;
        }
    }
    return false;
}

bool
pio_end_always_returns_success(struct transfer * Transfer)
{
    if (Transfer->Coordinator.Output.hpss_pio_end.ReturnValue != HPSS_E_NOERROR)
        return false;
    if (Transfer->Participant.Output.hpss_pio_end.ReturnValue != HPSS_E_NOERROR)
        return false;
    return true;
}

int
coordinator_return_value(struct transfer * Transfer)
{
    int retval = NoValue32;

    CREATE_SHORTCUT(pio_execute, Transfer->Coordinator.Output.hpss_pio_execute);
    for (int i = 0; pio_execute && pio_execute[i]; i++)
    {
        if (pio_execute[i+1])
            ASSERT(pio_execute[i]->ReturnValue == HPSS_E_NOERROR);
        else
            retval = pio_execute[i]->ReturnValue;
    }
    return retval;
}

int
participant_return_value(struct transfer * Transfer)
{
    return Transfer->Participant.Output.hpss_pio_register.ReturnValue;
}

bool
last_bytes_moved_set(struct transfer * Transfer)
{
    CREATE_SHORTCUT(pio_execute, Transfer->Coordinator.Output.hpss_pio_execute);
    for (int i = 0; pio_execute && pio_execute[i]; i++)
    {
        if (pio_execute[i+1])
            continue;

        return (pio_execute[i]->BytesMoved != NoValue64);
    }

    assert(0); // we should never get here
    return false;
}

uint64_t
min(uint64_t X, uint64_t Y)
{
    if (X < Y)
        return X;
    return Y;
}

bool
pio_callout_range(struct transfer *  Transfer,
                  struct range    ** Ranges)
{
    CREATE_SHORTCUT(pio_callout, Transfer->Participant.Output.pio_callout);

    int p = 0;
    uint64_t p_off = 0;
    uint64_t p_len = 0;

    int r = 0;
    uint64_t r_off = 0;
    uint64_t r_len = 0;

    while ((Ranges && Ranges[r]) || (pio_callout && pio_callout[p]))
    {
        if (p_len == 0 && pio_callout && pio_callout[p])
        {
            p_off = pio_callout[p]->Offset;
            p_len = pio_callout[p++]->Length;
        }

        if (r_len == 0 && Ranges && Ranges[r])
        {
            r_off = Ranges[r]->Offset;
            r_len = Ranges[r++]->Length;
        }

        if (p_off != r_off)
            return false;

        uint64_t len = min(r_len, p_len);
        p_off += len;
        p_len -= len;
        r_off += len;
        r_len -= len;
    }
    return (p_len == 0 && r_len == 0);
}

void
test_write_small_file(struct transfer * Transfer)
{
    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_WRITE,
                     RANGES(RANGE(0, 1024)));

    ASSERT(coordinator_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(participant_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_not_set(Transfer));
    ASSERT(pio_callout_range(Transfer, RANGES(RANGE(0, 1024))));
}

void
test_Zero_Write_Fails(struct transfer * Transfer)
{
    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_WRITE,
                     RANGES(RANGE(0, 0)));

    ASSERT(coordinator_return_value(Transfer) == HPSS_EINVAL);
    ASSERT(participant_return_value(Transfer) == HPSS_EIO);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_not_set(Transfer));
    ASSERT(last_bytes_moved_set(Transfer));
    ASSERT(pio_callout_range(Transfer, RANGES(RANGE(0, 0))));
}

void
test_Zero_Read_Fails(struct transfer * Transfer)
{
    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_READ,
                     RANGES(RANGE(0, 0)));

    ASSERT(coordinator_return_value(Transfer) == HPSS_EINVAL);
    ASSERT(participant_return_value(Transfer) == HPSS_EIO);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_not_set(Transfer));
    ASSERT(last_bytes_moved_set(Transfer));
    ASSERT(pio_callout_range(Transfer, RANGES(RANGE(0, 0))));
}

void
test_pio_callout_fail(struct transfer * Transfer)
{
    Transfer->Request.Participant.CalloutReturnValue = 0x12345678;

    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_WRITE,
                     RANGES(RANGE(0, 1024)));

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION == 4) 
    ASSERT(coordinator_return_value(Transfer) == HPSS_EIO);
#else
    ASSERT(coordinator_return_value(Transfer) == HPSS_ECONN);
#endif
    ASSERT(participant_return_value(Transfer) == HPSS_EIO);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_not_set(Transfer));
    ASSERT(last_bytes_moved_set(Transfer));
    ASSERT(pio_callout_range(Transfer, RANGES(RANGE(0, 1024))));
}

void
test_bad_buffer_size(struct transfer * Transfer)
{
    Transfer->Request.Coordinator.BlockSize = 1024*1024;
    Transfer->Request.Participant.BufferLength = 512*1024;

    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_WRITE,
                     RANGES(RANGE(0, 1024)));

    /* Coordinator will hang waiting for the participant. */
}

void
test_multi_range_file(struct transfer * Transfer)
{
    struct range ** input_ranges = RANGES(RANGE(0, 1024), RANGE(1024, 1024));
    perform_transfer(Transfer, "test", HPSS_PIO_WRITE, input_ranges);

    ASSERT(coordinator_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(participant_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_not_set(Transfer));
    ASSERT(pio_callout_range(Transfer, input_ranges));
}

void
test_write_file_out_of_order(struct transfer * Transfer)
{
    struct range ** input_ranges = RANGES(RANGE(1024, 1024), RANGE(0, 1024));
    perform_transfer(Transfer, "test", HPSS_PIO_WRITE, input_ranges);

    ASSERT(coordinator_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(participant_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_not_set(Transfer));
    ASSERT(pio_callout_range(Transfer, input_ranges));
}

void
test_read_file_with_gap(struct transfer * Transfer)
{
    struct range ** input_ranges = RANGES(RANGE(0, 1024), RANGE(2048, 1024));
    quick_transfer("test", HPSS_PIO_WRITE, input_ranges);

    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_READ,
                     RANGES(RANGE(0, 3072)));

    ASSERT(coordinator_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(participant_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_set_to(Transfer, 1024, 1024));
    ASSERT(pio_callout_range(Transfer, input_ranges));
}

void
test_gap_relative_to_offset(struct transfer * Transfer)
{
    struct range ** input_ranges = RANGES(RANGE(0, 1024), RANGE(2048, 1024));
    quick_transfer("test", HPSS_PIO_WRITE, input_ranges);

    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_READ,
                     RANGES(RANGE(0, 1024), RANGE(1024, 2048)));

    ASSERT(coordinator_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(participant_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_set_to(Transfer, 0, 1024));
    ASSERT(pio_callout_range(Transfer, input_ranges));
}

void
test_gap_spans_blocks(struct transfer * Transfer)
{
    Transfer->Request.Coordinator.BlockSize = 1024 * 1024;

    struct range ** input_ranges = RANGES(RANGE(0, 0.5*1024*1024), 
                                          RANGE(1.5*1024*1024, 0.5*1024*1024));
    quick_transfer("test", HPSS_PIO_WRITE, input_ranges);

    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_READ,
                     RANGES(RANGE(0, 2*1024*1024)));

    ASSERT(coordinator_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(participant_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_set_to(Transfer, 0.5*1024*1024, 1024*1024));
    ASSERT(pio_callout_range(Transfer, input_ranges));
}

void
test_pio_handle_gap(struct transfer * Transfer)
{
    quick_transfer("test",
                   HPSS_PIO_WRITE,
                   RANGES(RANGE(0, 0.5*1024*1024), 
                          RANGE(1.5*1024*1024, 0.5*1024*1024)));

    Transfer->Request.Coordinator.Options = HPSS_PIO_HANDLE_GAP;
    perform_transfer(Transfer,
                     "test",
                     HPSS_PIO_READ,
                     RANGES(RANGE(0, 2*1024*1024)));

    ASSERT(coordinator_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(participant_return_value(Transfer) == HPSS_E_NOERROR);
    ASSERT(pio_end_always_returns_success(Transfer));
    ASSERT(gap_info_set_to(Transfer, 0.5*1024*1024, 1024*1024));
// This one is known to be broken in 7.4 and 7.5
    ASSERT(pio_callout_range(Transfer, RANGES(RANGE(0, 2*1024*1024))));

}
// XXX is it worth repeating any tests but with write?

int
main()
{
    // On kerberos systems, you should be able to point this at a kerberos
    // keytab file. You can construct a kerberos keytab like this:
    // > ktutil
    // ktutil:  addent -password -p <username>@<domain> -k 1 -e aes256-cts
    // Password for <username>@<domain>: [enter your password]
    // ktutil:  wkt <keytab_file>
    // ktutil:  quit
    int retval = hpss_SetLoginCred("jalt",
                                   hpss_authn_mech_krb5, // hpss_authn_mech_unix
                                   hpss_rpc_cred_client,
                                   hpss_rpc_auth_type_keytab,
                                   "/home/local/jalt/hpss.keytab2");
    if (retval)
    {
        printf("hpss_SetLoginCred() failed\n");
        return 1;
    }

    struct test_case * tc = (struct test_case[]) {
        {"test_write_small_file", test_write_small_file},
        {"test_Zero_Write_Fails", test_Zero_Write_Fails},
        {"test_Zero_Read_Fails", test_Zero_Read_Fails},
        {"test_pio_callout_fail", test_pio_callout_fail},

        // Skip. The coordinator will hang waiting for the participant.
        /* {"test_bad_buffer_size", test_bad_buffer_size}, */
        {"test_multi_range_file", test_multi_range_file},
        {"test_write_file_out_of_order", test_write_file_out_of_order},
        {"test_read_file_with_gap", test_read_file_with_gap},
        {"test_gap_relative_to_offset", test_gap_relative_to_offset},
        {"test_gap_spans_blocks", test_gap_spans_blocks},
        {"test_pio_handle_gap", test_pio_handle_gap},
        {.name = NULL}
    };

    struct test_suite ts = {
        .setup = test_setup,
        .teardown = test_cleanup,
        .test_cases = tc
    };

    struct transfer transfer;
    return !(run_suite(&ts, &transfer) == TEST_SUCCESS);
}

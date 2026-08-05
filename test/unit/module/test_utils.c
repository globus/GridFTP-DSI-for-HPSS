#include <stdlib.h>
#include <testing.h>
#include <driver.h>

#include <utils.h>

static bool (*_is_valid_uuid)(const char * uuid_str);

static globus_result_t
(*_generate_callback_id)(
    const char                  *  TaskID,
    bitfile_id_t                *  BitfileID,
    hpss_reqid_t                *  CallbackID);

#if HPSS_MAJOR_VERSION >= 8
static globus_result_t
(*_hpss_reqid_to_string)(
    const hpss_reqid_t          *  RequestID,
    char                        ** UUIDString);

static globus_result_t
(*_string_to_hpss_reqid)(
    const char                  *  UUIDString,
    hpss_reqid_t                *  RequestID);
#endif // HPSS_MAJOR_VERSION >= 8

void
test_is_valid_uuid(void * Arg)
{
    ASSERT(_is_valid_uuid("8b9e32b7-bdd0-4714-877a-8e8742168821"));
    // Capitols
    ASSERT(_is_valid_uuid("8B9E32B7-BDD0-4714-877A-8E8742168821"));
    // No hyphens
    ASSERT(!_is_valid_uuid("8b9e32b7abdd0a4714a877aa8e8742168821"));
    ASSERT(!_is_valid_uuid(NULL));
    ASSERT(!_is_valid_uuid(""));
    // Non-hex character
    ASSERT(!_is_valid_uuid("8b9e32bz-bdd0-4714-877a-8e8742168821"));
    // Extra hex character
    ASSERT(!_is_valid_uuid("8b9e32b7-bdd0-4714-877a-8e87421688211"));
}

void
test_generate_callback_id(void * Arg)
{
    globus_result_t result;
    bitfile_id_t bitfile_id;
    hpss_reqid_t callback_id;
    const char * task_id = "7c1506fd-f7f1-45fd-9fb6-df0401e2c77f";

    //
    // If TaskID is NULL, does not return GLOBUS_SUCCESS
    //
    result = _generate_callback_id(NULL, &bitfile_id, &callback_id);
    ASSERT(result != GLOBUS_SUCCESS);

    //
    // If BitfileID is NULL, CallbackID only consists of the TaskID
    //
    result = _generate_callback_id(task_id, NULL, &callback_id);
    ASSERT(result == GLOBUS_SUCCESS);

#if HPSS_MAJOR_VERSION == 7
    // In HPSS 7.x, hpss_reqid_t was an unsigned
    unsigned expected_value =
        (((0x7c ^ 0xf7 ^ 0x9f ^ 0x01) << 24) & 0xFF000000) |
        (((0x15 ^ 0xf1 ^ 0xb6 ^ 0xe2) << 16) & 0x00FF0000) |
        (((0x06 ^ 0x45 ^ 0xdf ^ 0xc7) <<  8) & 0x0000FF00) |
        (((0xfd ^ 0xfd ^ 0x04 ^ 0x7f) <<  0) & 0x000000FF);
    ASSERT(callback_id == expected_value);
#else // HPSS_MAJOR_VERSION >= 8
    // In HPSS 8.x+, hpss_reqid_t was an hpss_uuid_t
    ASSERT(callback_id.time_low == 0x7c1506fd);
    ASSERT(callback_id.time_mid == 0xf7f1);
    ASSERT(callback_id.time_hi_and_version == 0x45fd);
    ASSERT(callback_id.clock_seq_hi_and_reserved == 0x9f);
    ASSERT(callback_id.clock_seq_low == 0xb6);
    ASSERT((unsigned char)callback_id.node[0] == (unsigned char)0xdf);
    ASSERT((unsigned char)callback_id.node[1] == (unsigned char)0x04);
    ASSERT((unsigned char)callback_id.node[2] == (unsigned char)0x01);
    ASSERT((unsigned char)callback_id.node[3] == (unsigned char)0xe2);
    ASSERT((unsigned char)callback_id.node[4] == (unsigned char)0xc7);
    ASSERT((unsigned char)callback_id.node[5] == (unsigned char)0x7f);
#endif // HPSS_MAJOR_VERSION >= 8

    //
    // If BitfileID is not NULL, CallbackID consists of TaskID ^ BitfileID
    //
    // Bitfile ID is "92185e1e-48c8-424a-924e-dda3b3047353";
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    // BitfileID is a hpssoid_t for <= HPSS 7.4. We use the ObjectID field (hpss_uuid_t)
    // for this calculation.
    bitfile_id.ObjectID
    bitfile_id.ObjectID.time_low == 0x92185e1e);
    bitfile_id.ObjectID.time_mid == 0x48c8);
    bitfile_id.ObjectID.time_hi_and_version == 0x424a);
    bitfile_id.ObjectID.clock_seq_hi_and_reserved == 0x92);
    bitfile_id.ObjectID.clock_seq_low == 0x4e);
    bitfile_id.ObjectID.node[0] == (char)0xdd);
    bitfile_id.ObjectID.node[1] == (char)0xa3);
    bitfile_id.ObjectID.node[2] == (char)0xb3);
    bitfile_id.ObjectID.node[3] == (char)0x04);
    bitfile_id.ObjectID.node[4] == (char)0x73);
    bitfile_id.ObjectID.node[5] == (char)0x53);
#else // HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    // BitfileID is a bfs_bitfile_obj_handle_t for >= HPSS 7.5. We use  the BfId.Bytes
    // field for this calculation.
    bitfile_id.BfId.Bytes[0] = 0x92;
    bitfile_id.BfId.Bytes[1] = 0x18;
    bitfile_id.BfId.Bytes[2] = 0x5e;
    bitfile_id.BfId.Bytes[3] = 0x1e;
    bitfile_id.BfId.Bytes[4] = 0x48;
    bitfile_id.BfId.Bytes[5] = 0xc8;
    bitfile_id.BfId.Bytes[6] = 0x42;
    bitfile_id.BfId.Bytes[7] = 0x4a;
    bitfile_id.BfId.Bytes[8] = 0x92;
    bitfile_id.BfId.Bytes[9] = 0x4e;
    bitfile_id.BfId.Bytes[10] = 0xdd;
    bitfile_id.BfId.Bytes[11] = 0xa3;
    bitfile_id.BfId.Bytes[12] = 0xb3;
    bitfile_id.BfId.Bytes[13] = 0x04;
    bitfile_id.BfId.Bytes[14] = 0x73;
    bitfile_id.BfId.Bytes[15] = 0x53;
#endif // HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4

    result = _generate_callback_id(task_id, &bitfile_id, &callback_id);
    ASSERT(result == GLOBUS_SUCCESS);

#if HPSS_MAJOR_VERSION == 7
    // In HPSS 7.x, hpss_reqid_t was an unsigned
    expected_value =
        (((0x7c ^ 0x92 ^ 0xf7 ^ 0x48 ^ 0x9f ^ 0x92 ^ 0x01 ^ 0xb3 ) << 24) & 0xFF000000) |
        (((0x15 ^ 0x18 ^ 0xf1 ^ 0xc8 ^ 0xb6 ^ 0x4e ^ 0xe2 ^ 0x04 ) << 16) & 0x00FF0000) |
        (((0x06 ^ 0x5e ^ 0x45 ^ 0x42 ^ 0xdf ^ 0xdd ^ 0xc7 ^ 0x73 ) <<  8) & 0x0000FF00) |
        (((0xfd ^ 0x1e ^ 0xfd ^ 0x4a ^ 0x04 ^ 0xa3 ^ 0x7f ^ 0x53 ) <<  0) & 0x000000FF);
    ASSERT(callback_id == expected_value);
#else // HPSS_MAJOR_VERSION >= 8
    // In HPSS 8.x+, hpss_reqid_t was an hpss_uuid_t
    // Bitfile ID is "92185e1e-48c8-424a-924e-dda3b3047353";
    ASSERT(callback_id.time_low == (0x7c1506fd ^ 0x92185e1e));
    ASSERT(callback_id.time_mid == (0xf7f1 ^ 0x48c8));
    ASSERT(callback_id.time_hi_and_version == (0x45fd ^ 0x424a));
    ASSERT(callback_id.clock_seq_hi_and_reserved == (0x9f ^ 0x92));
    ASSERT(callback_id.clock_seq_low == (0xb6 ^ 0x4e));
    ASSERT((unsigned char)callback_id.node[0] == (unsigned char)(0xdf ^ 0xdd));
    ASSERT((unsigned char)callback_id.node[1] == (unsigned char)(0x04 ^ 0xa3));
    ASSERT((unsigned char)callback_id.node[2] == (unsigned char)(0x01 ^ 0xb3));
    ASSERT((unsigned char)callback_id.node[3] == (unsigned char)(0xe2 ^ 0x04));
    ASSERT((unsigned char)callback_id.node[4] == (unsigned char)(0xc7 ^ 0x73));
    ASSERT((unsigned char)callback_id.node[5] == (unsigned char)(0x7f ^ 0x53));
#endif // HPSS_MAJOR_VERSION >= 8
}

#if HPSS_MAJOR_VERSION >= 8
// Prior to HPSS v8, hpss_reqid_t was an unsigned. As of HPSS v8, it is a hpss_uuid_t.
// We only support these functions in HPSS v8+.
void
test_hpss_reqid_to_string(void * Arg)
{
    char * uuid_str = NULL;
    const char * expected_uuid_str = "02e69056-440a-4cc6-8fe5-11a80aa260fb";
    hpss_reqid_t request_id;

    request_id.time_low = 0x02e69056;
    request_id.time_mid = 0x440a;
    request_id.time_hi_and_version = 0x4cc6;
    request_id.clock_seq_hi_and_reserved = 0x8f;
    request_id.clock_seq_low = 0xe5;
    request_id.node[0] = (unsigned char)0x11;
    request_id.node[1] = (unsigned char)0xa8;
    request_id.node[2] = (unsigned char)0x0a;
    request_id.node[3] = (unsigned char)0xa2;
    request_id.node[4] = (unsigned char)0x60;
    request_id.node[5] = (unsigned char)0xfb;

    // Successful conversion (always?)
    globus_result_t result = _hpss_reqid_to_string(&request_id, &uuid_str);
    ASSERT(result == GLOBUS_SUCCESS);
    ASSERT(uuid_str != NULL);
    if (uuid_str != NULL)
    {
        ASSERT(strcmp(uuid_str, expected_uuid_str) == 0);
        free(uuid_str);
    }
}

void
test_string_to_hpss_reqid(void * Arg)
{
    const char * uuid_str = "16f3a9ed-0e8b-4cab-b3c9-95bf02e026a2";
    hpss_reqid_t request_id;

    // Successful conversion when UUIDString represents a valid UUID
    globus_result_t result = _string_to_hpss_reqid(uuid_str, &request_id);
    ASSERT(result == GLOBUS_SUCCESS);

    ASSERT(request_id.time_low == 0x16f3a9ed);
    ASSERT(request_id.time_mid == 0x0e8b);
    ASSERT(request_id.time_hi_and_version == 0x4cab);
    ASSERT(request_id.clock_seq_hi_and_reserved == 0xb3);
    ASSERT(request_id.clock_seq_low == 0xc9);
    ASSERT((unsigned char)request_id.node[0] == (unsigned char)0x95);
    ASSERT((unsigned char)request_id.node[1] == (unsigned char)0xbf);
    ASSERT((unsigned char)request_id.node[2] == (unsigned char)0x02);
    ASSERT((unsigned char)request_id.node[3] == (unsigned char)0xe0);
    ASSERT((unsigned char)request_id.node[4] == (unsigned char)0x26);
    ASSERT((unsigned char)request_id.node[5] == (unsigned char)0xa2);

    // Failed conversion when UUIDString is NULL
    result = _string_to_hpss_reqid(NULL, &request_id);
    ASSERT(result != GLOBUS_SUCCESS);

    // Failed conversion when UUIDString does not represent a valid UUID
    result = _string_to_hpss_reqid("Hello World", &request_id);
    ASSERT(result != GLOBUS_SUCCESS);
}
#endif // HPSS_MAJOR_VERSION >= 8

test_status_t
test_setup(void * Arg)
{
    if (!_is_valid_uuid)
        _is_valid_uuid = lookup_symbol("is_valid_uuid");
    if (!_generate_callback_id)
        _generate_callback_id = lookup_symbol("generate_callback_id");

#if HPSS_MAJOR_VERSION >= 8
    if (!_hpss_reqid_to_string)
        _hpss_reqid_to_string = lookup_symbol("hpss_reqid_to_string");

    if (!_string_to_hpss_reqid)
        _string_to_hpss_reqid = lookup_symbol("string_to_hpss_reqid");

    return TEST_SUCCESS;
#endif // HPSS_MAJOR_VERSION >= 8
}


struct test_suite TEST_SUITE = {
    .setup = test_setup,
    .teardown = NULL,
    .test_cases = (struct test_case[]) {
        {"test_is_valid_uuid",      test_is_valid_uuid},
        {"test_generate_callback_id", test_generate_callback_id},

#if HPSS_MAJOR_VERSION >= 8
        {"test_hpss_reqid_to_string", test_hpss_reqid_to_string},
        {"test_string_to_hpss_reqid", test_string_to_hpss_reqid},
#endif // HPSS_MAJOR_VERSION >= 8
        {NULL,  NULL},
    }
};

void * TEST_SUITE_ARG = NULL;

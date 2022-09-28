#include <stdlib.h>
#include <testing.h>
#include <driver.h>

#include <utils.h>

static bool (*_is_valid_uuid)(const char * uuid_str);
static void (*_uuid_str_to_bytes)(const char * UUID, unsigned char Bytes[UUID_BYTE_COUNT]);
static void (*_hpss_uuid_to_bytes)(const hpss_uuid_t * UUID, unsigned char bytes[UUID_BYTE_COUNT]);
static void (*_bytes_to_hpss_uuid)(const unsigned char Bytes[UUID_BYTE_COUNT], hpss_uuid_t * UUID);
static void (*_bytes_to_unsigned)(const unsigned char Bytes[UUID_BYTE_COUNT], unsigned * Unsigned);


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
test_uuid_str_to_bytes(void * Arg)
{
    unsigned char uuid_bytes[UUID_BYTE_COUNT];
    _uuid_str_to_bytes("d0d50984-1fbb-40f1-a014-a2996944aa19", uuid_bytes);

    ASSERT(uuid_bytes[0] == 0xd0);
    ASSERT(uuid_bytes[1] == 0xd5);
    ASSERT(uuid_bytes[2] == 0x09);
    ASSERT(uuid_bytes[3] == 0x84);

    ASSERT(uuid_bytes[4] == 0x1f);
    ASSERT(uuid_bytes[5] == 0xbb);

    ASSERT(uuid_bytes[6] == 0x40);
    ASSERT(uuid_bytes[7] == 0xf1);

    ASSERT(uuid_bytes[8] == 0xa0);
    ASSERT(uuid_bytes[9] == 0x14);

    ASSERT(uuid_bytes[10] == 0xa2);
    ASSERT(uuid_bytes[11] == 0x99);
    ASSERT(uuid_bytes[12] == 0x69);
    ASSERT(uuid_bytes[13] == 0x44);
    ASSERT(uuid_bytes[14] == 0xaa);
    ASSERT(uuid_bytes[15] == 0x19);
}


void
test_hpss_uuid_to_bytes(void * Arg)
{
    hpss_uuid_t hpss_uuid = {0xdeadbeef, 0x0123, 0x4567, 0x89, 0xab, {0xcd, 0xef, 0xde, 0xad, 0xbe, 0xef}};

    unsigned char bytes[UUID_BYTE_COUNT];
    _hpss_uuid_to_bytes(&hpss_uuid, bytes);

    ASSERT(bytes[0] == 0xde);
    ASSERT(bytes[1] == 0xad);
    ASSERT(bytes[2] == 0xbe);
    ASSERT(bytes[3] == 0xef);

    ASSERT(bytes[4] == 0x01);
    ASSERT(bytes[5] == 0x23);

    ASSERT(bytes[6] == 0x45);
    ASSERT(bytes[7] == 0x67);

    ASSERT(bytes[8] == 0x89);
    ASSERT(bytes[9] == 0xab);

    ASSERT(bytes[10] == 0xcd);
    ASSERT(bytes[11] == 0xef);
    ASSERT(bytes[12] == 0xde);
    ASSERT(bytes[13] == 0xad);
    ASSERT(bytes[14] == 0xbe);
    ASSERT(bytes[15] == 0xef);
}


void
test_bytes_to_hpss_uuid(void * Arg)
{
    unsigned char bytes[UUID_BYTE_COUNT] = {
        0xf1, 0xe2, 0xd3, 0xc4, 0xb5, 0xa6, 0x97, 0x88,
        0x79, 0x6a, 0x5b, 0x4c, 0x3d, 0x2e, 0x1f, 0xff,
    };

    hpss_uuid_t uuid;
    _bytes_to_hpss_uuid(bytes, &uuid);

    ASSERT(uuid.time_low == 0xf1e2d3c4);
    ASSERT(uuid.time_mid == 0xb5a6);
    ASSERT(uuid.time_hi_and_version == 0x9788);
    ASSERT(uuid.clock_seq_hi_and_reserved == 0x79);
    ASSERT(uuid.clock_seq_low == 0x6a);
    ASSERT(uuid.node[0] == 0x5b);
    ASSERT(uuid.node[1] == 0x4c);
    ASSERT(uuid.node[2] == 0x3d);
    ASSERT(uuid.node[3] == 0x2e);
    ASSERT(uuid.node[4] == 0x1f);
    ASSERT(uuid.node[5] == (char)0xff);
}


void
test_bytes_to_unsigned(void * Arg)
{
    unsigned char bytes[UUID_BYTE_COUNT] = {
        0x67, 0xc6, 0x69, 0x73, 0x51, 0xff, 0x4a, 0xec,
        0x29, 0xcd, 0xba, 0xab, 0xf2, 0xfb, 0xe3, 0x46
    };

    unsigned returned_value;
    _bytes_to_unsigned(bytes, &returned_value);

    unsigned expected_value =
        (((bytes[0] ^ bytes[4] ^ bytes[8]  ^ bytes[12]) << 24) & 0xFF000000) |
        (((bytes[1] ^ bytes[5] ^ bytes[9]  ^ bytes[13]) << 16) & 0x00FF0000) |
        (((bytes[2] ^ bytes[6] ^ bytes[10] ^ bytes[14]) <<  8) & 0x0000FF00) |
        (((bytes[3] ^ bytes[7] ^ bytes[11] ^ bytes[15]) <<  0) & 0x000000FF);

    ASSERT(returned_value == expected_value);
}


test_status_t
test_setup(void * Arg)
{
    if (!_is_valid_uuid)
        _is_valid_uuid = lookup_symbol("is_valid_uuid");
    if (!_uuid_str_to_bytes)
        _uuid_str_to_bytes = lookup_symbol("uuid_str_to_bytes");
    if (!_hpss_uuid_to_bytes)
        _hpss_uuid_to_bytes = lookup_symbol("hpss_uuid_to_bytes");
    if (!_bytes_to_hpss_uuid)
        _bytes_to_hpss_uuid = lookup_symbol("bytes_to_hpss_uuid");
    if (!_bytes_to_unsigned)
        _bytes_to_unsigned = lookup_symbol("bytes_to_unsigned");
    return TEST_SUCCESS;
}


struct test_suite TEST_SUITE = {
    .setup = test_setup,
    .teardown = NULL,
    .test_cases = (struct test_case[]) {
        {"test_is_valid_uuid",      test_is_valid_uuid},
        {"test_uuid_str_to_bytes",  test_uuid_str_to_bytes},
        {"test_hpss_uuid_to_bytes", test_hpss_uuid_to_bytes},
        {"test_bytes_to_hpss_uuid", test_bytes_to_hpss_uuid},
        {"test_bytes_to_unsigned",  test_bytes_to_unsigned},
        {NULL,  NULL},
    }
};

void * TEST_SUITE_ARG = NULL;

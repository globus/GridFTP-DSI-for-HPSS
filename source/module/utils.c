/*
 * System includes
 */
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

/*
 * Local includes
 */
#include "logging.h"
#include "utils.h"

#define UUID_BYTE_COUNT 16
#define UUID_STR_COUNT 37 // 36 characters + 1 null terminator

static bool
_is_hex_str(const char * str, size_t index, size_t len)
{
    for (int i = index; i < len; i++)
    {
        if (!isxdigit(str[i]))
            return false;
    }
    return true;
}


/*
 * Returns True if UUIDString is a non-null value with the format:
 *    "[hex]{8}-[hex]{4}-[hex]{4}-[hex]{4}-[hex]{12}\0"
 * The string can use upper or lower case characters.
 */
bool
is_valid_uuid(const char * uuid_str)
{
    if (uuid_str == NULL || strlen(uuid_str) != 36)
        return false;

    if (!_is_hex_str(uuid_str, 0, 8))
        return false;

    if (!_is_hex_str(uuid_str, 9, 4))
        return false;

    if (!_is_hex_str(uuid_str, 14, 4))
        return false;

    if (!_is_hex_str(uuid_str, 19, 4))
        return false;

    if (!_is_hex_str(uuid_str, 24, 12))
        return false;

    if (uuid_str[8] != '-')
        return false;

    if (uuid_str[13] != '-')
        return false;

    if (uuid_str[18] != '-')
        return false;

    if (uuid_str[23] != '-')
        return false;

    return true;
}


static unsigned char
_hex_char_to_hex(char HexChar)
{
    if (!isxdigit(HexChar))
        return -1;

    if (isdigit(HexChar))
        return (char) (int)HexChar - (int)'0';

    if (islower(HexChar))
        return (char) (int)HexChar - (int)'a' + 10;

    if (isupper(HexChar))
        return (char) (int)HexChar - (int)'A' + 10;

    return -1;
}


// Returns an array of UUID_BYTE_COUNT bytes. Not NULL-terminated.
static void
_uuid_str_to_bytes(const char * UUID, unsigned char Bytes[UUID_BYTE_COUNT])
{
    assert(is_valid_uuid(UUID));

    Bytes[0] = _hex_char_to_hex(UUID[0]) << 4 | _hex_char_to_hex(UUID[1]);
    Bytes[1] = _hex_char_to_hex(UUID[2]) << 4 | _hex_char_to_hex(UUID[3]);
    Bytes[2] = _hex_char_to_hex(UUID[4]) << 4 | _hex_char_to_hex(UUID[5]);
    Bytes[3] = _hex_char_to_hex(UUID[6]) << 4 | _hex_char_to_hex(UUID[7]);

    Bytes[4] = _hex_char_to_hex(UUID[9]) << 4 | _hex_char_to_hex(UUID[10]);
    Bytes[5] = _hex_char_to_hex(UUID[11]) << 4 | _hex_char_to_hex(UUID[12]);

    Bytes[6] = _hex_char_to_hex(UUID[14]) << 4 | _hex_char_to_hex(UUID[15]);
    Bytes[7] = _hex_char_to_hex(UUID[16]) << 4 | _hex_char_to_hex(UUID[17]);

    Bytes[8] = _hex_char_to_hex(UUID[19]) << 4 | _hex_char_to_hex(UUID[20]);
    Bytes[9] = _hex_char_to_hex(UUID[21]) << 4 | _hex_char_to_hex(UUID[22]);

    Bytes[10] = _hex_char_to_hex(UUID[24]) << 4 | _hex_char_to_hex(UUID[25]);
    Bytes[11] = _hex_char_to_hex(UUID[26]) << 4 | _hex_char_to_hex(UUID[27]);
    Bytes[12] = _hex_char_to_hex(UUID[28]) << 4 | _hex_char_to_hex(UUID[29]);
    Bytes[13] = _hex_char_to_hex(UUID[30]) << 4 | _hex_char_to_hex(UUID[31]);
    Bytes[14] = _hex_char_to_hex(UUID[32]) << 4 | _hex_char_to_hex(UUID[33]);
    Bytes[15] = _hex_char_to_hex(UUID[34]) << 4 | _hex_char_to_hex(UUID[35]);
}

// Returns an array of UUID_BYTE_COUNT bytes. Not NULL-terminated.
static void
_hpss_uuid_to_bytes(const hpss_uuid_t * UUID, unsigned char Bytes[UUID_BYTE_COUNT])
{
    Bytes[0] = (UUID->time_low >> 24) & 0xFF;
    Bytes[1] = (UUID->time_low >> 16) & 0xFF;
    Bytes[2] = (UUID->time_low >>  8) & 0xFF;
    Bytes[3] = (UUID->time_low >>  0) & 0xFF;

    Bytes[4] = (UUID->time_mid >>  8) & 0xFF;
    Bytes[5] = (UUID->time_mid >>  0) & 0xFF;

    Bytes[6] = (UUID->time_hi_and_version >>  8) & 0xFF;
    Bytes[7] = (UUID->time_hi_and_version >>  0) & 0xFF;

    Bytes[8] = UUID->clock_seq_hi_and_reserved;
    Bytes[9] = UUID->clock_seq_low;

    Bytes[10] = UUID->node[0];
    Bytes[11] = UUID->node[1];
    Bytes[12] = UUID->node[2];
    Bytes[13] = UUID->node[3];
    Bytes[14] = UUID->node[4];
    Bytes[15] = UUID->node[5];
}

static void
_bytes_to_hpss_uuid(const unsigned char Bytes[UUID_BYTE_COUNT], hpss_uuid_t * UUID)
{
    UUID->time_low = 
        ((Bytes[0] << 24) & 0xFF000000) |
        ((Bytes[1] << 16) & 0x00FF0000) |
        ((Bytes[2] <<  8) & 0x0000FF00) |
        ((Bytes[3] <<  0) & 0x000000FF);

    UUID->time_mid = 
        ((Bytes[4] << 8) & 0xFF00) |
        ((Bytes[5] << 0) & 0x00FF);

    UUID->time_hi_and_version = 
        ((Bytes[6] << 8) & 0xFF00) |
        ((Bytes[7] << 0) & 0x00FF);

    UUID->clock_seq_hi_and_reserved = Bytes[8];
    UUID->clock_seq_low = Bytes[9];

    UUID->node[0] = Bytes[10];
    UUID->node[1] = Bytes[11];
    UUID->node[2] = Bytes[12];
    UUID->node[3] = Bytes[13];
    UUID->node[4] = Bytes[14];
    UUID->node[5] = Bytes[15];
}

#if HPSS_MAJOR_VERSION < 8
static void
_bytes_to_unsigned(const unsigned char Bytes[UUID_BYTE_COUNT], unsigned * Unsigned)
{
    *Unsigned = 0;

    for (int i = 0; i < UUID_BYTE_COUNT; i++)
    {
        int bits_to_shift = ((sizeof(*Unsigned) - (i % sizeof(*Unsigned)) - 1) * 8);
        *Unsigned ^= Bytes[i] << bits_to_shift;
    }
}
#endif // HPSS_MAJOR_VERSION < 8

static void
_uuid_bytes_to_str(const unsigned char Bytes[UUID_BYTE_COUNT], char UUID[UUID_STR_COUNT])
{
    assert(Bytes != NULL);
    assert(UUID != NULL);

    sprintf(UUID,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        Bytes[0], Bytes[1], Bytes[2], Bytes[3],
        Bytes[4], Bytes[5],
        Bytes[6], Bytes[7],
        Bytes[8], Bytes[9],
        Bytes[10], Bytes[11], Bytes[12], Bytes[13], Bytes[14], Bytes[15]
    );
}

/*
 * Generate CallbackID from TaskID and BitfileID.
 *   TaskID - Required
 *   BitfileID - Optional
 *
 * If BitfileID is NULL, CallbackID is set to TaskID. If BitfileID is not NULL,
 * CallbackID is TaskID^BitfileID (TaskID is first converted byte-by-byte to its
 * int values).
 */
globus_result_t
generate_callback_id(
    const char                  *  TaskID,
    bitfile_id_t                *  BitfileID,
    hpss_reqid_t                *  CallbackID)
{
    unsigned char request_id_bytes[UUID_BYTE_COUNT];

    if (TaskID == NULL)
    {
        ERROR("TaskID missing while generating the callback ID");
        return GlobusGFSErrorGeneric("TaskID missing while generating the callback ID");
    }

    // Convert Task ID to a bytes array
    _uuid_str_to_bytes(TaskID, request_id_bytes);

    // XOR the BitfileID, if provided.
    if (BitfileID != NULL)
    {
        // Convert BitfileID to a byte array
        unsigned char bitfile_id_bytes[UUID_BYTE_COUNT];

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
        memcpy(bitfile_id_bytes, BitfileID->BfId.Bytes, UUID_BYTE_COUNT);
#else
        _hpss_uuid_to_bytes(&BitfileID->ObjectID, bitfile_id_bytes);
#endif

        // Combine the two byte arrays
        for (int i = 0; i < UUID_BYTE_COUNT; i++)
        {
            request_id_bytes[i] ^= bitfile_id_bytes[i];
        }
    }

#if HPSS_MAJOR_VERSION >= 8
    // Convert to a UUID
    _bytes_to_hpss_uuid(request_id_bytes, CallbackID);
#else
    // Convert to unsigned
    _bytes_to_unsigned(request_id_bytes, CallbackID);
#endif

    return GLOBUS_SUCCESS;
}

#if HPSS_MAJOR_VERSION >= 8
/*
 * Converts a hpss_reqid_t * (aka a hpss_uuid_t) to a UUID in string format:
 *   ex. hpss_request_id * => "ddfeb23c-53ee-435b-8318-a2c4fb2519d2"
 *
 * Added with batch staging in 9.3.
 */
globus_result_t
hpss_reqid_to_string(
    const hpss_reqid_t          *  RequestID,
    char                        ** UUIDString)
{
    unsigned char bytes[UUID_BYTE_COUNT];
    _hpss_uuid_to_bytes(RequestID, bytes);

    *UUIDString = calloc(UUID_STR_COUNT, 1);
    if (*UUIDString == NULL)
        return GlobusGFSErrorMemory("UUIDString");

    _uuid_bytes_to_str(bytes, *UUIDString);
    return GLOBUS_SUCCESS;
}

/*
 * Converts a UUID string to hpss_reqid_t *.
 *   ex. "ddfeb23c-53ee-435b-8318-a2c4fb2519d2" => hpss_request_id
 *
 * Added with batch staging in 9.3.
 */
globus_result_t
string_to_hpss_reqid(
    const char                  *  UUIDString,
    hpss_reqid_t                *  RequestID)
{
    if (!is_valid_uuid(UUIDString))
    {
        WARN("Invalid UUID string passed for conversion to request ID");
        return GlobusGFSErrorGeneric("Invalid UUID string passed for conversion to request ID");
    }

    unsigned char request_id_bytes[UUID_BYTE_COUNT];
    _uuid_str_to_bytes(UUIDString, request_id_bytes);
    _bytes_to_hpss_uuid(request_id_bytes, RequestID);
    return GLOBUS_SUCCESS;
}

#endif // HPSS_MAJOR_VERSION >= 8

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
#include "utils.h"


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


unsigned char
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
void
uuid_str_to_bytes(const char * UUID, unsigned char Bytes[UUID_BYTE_COUNT])
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
void
hpss_uuid_to_bytes(const hpss_uuid_t * UUID, unsigned char Bytes[UUID_BYTE_COUNT])
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

void
bytes_to_hpss_uuid(const unsigned char Bytes[UUID_BYTE_COUNT], hpss_uuid_t * UUID)
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

void
bytes_to_unsigned(const unsigned char Bytes[UUID_BYTE_COUNT], unsigned * Unsigned)
{
    *Unsigned = 0;

    for (int i = 0; i < UUID_BYTE_COUNT; i++)
    {
        int bits_to_shift = ((sizeof(*Unsigned) - (i % sizeof(*Unsigned)) - 1) * 8);
        *Unsigned ^= Bytes[i] << bits_to_shift;
    }
}

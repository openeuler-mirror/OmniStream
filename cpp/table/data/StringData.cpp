#include "StringData.h"
#include "binary/BinaryStringData.h"

StringData *StringData::fromString(std::u32string *str)
{
    return BinaryStringData::fromString(str);
}

StringData *StringData::fromBytes(uint8_t* bytes, int len)
{
    return BinaryStringData::fromBytes(bytes, len);
}

StringData *StringData::fromBytes(uint8_t* bytes, int offset, int numBytes)
{
    return BinaryStringData::fromBytes(bytes, offset, numBytes);
}

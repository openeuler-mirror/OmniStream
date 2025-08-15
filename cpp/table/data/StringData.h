//
// Created by root on 8/9/24.
//

#ifndef FLINK_TNEL_STRINGDATA_H
#define FLINK_TNEL_STRINGDATA_H

#include <string>
#include <vector>
#include <cstdint> // For uint8_t



class StringData
{
public:
    virtual uint8_t* toBytes() = 0;
    virtual std::u32string* toString() = 0;
    virtual std::string* ToUtF8String() = 0;

    static StringData *fromString(std::u32string *str);

    static StringData *fromBytes(uint8_t* bytes, int len);

    static StringData *fromBytes(uint8_t* bytes, int offset, int numBytes);
};

#endif // FLINK_TNEL_STRINGDATA_H

//
// Created by root on 8/9/24.
//

#ifndef FLINK_TNEL_STRINGUTF8UTILS_H
#define FLINK_TNEL_STRINGUTF8UTILS_H

#include <string>
#include <vector>
#include <cstdint> // For uint8_t
#include <stdexcept>


class StringUtf8Utils
{
    static const int MAX_BYTES_PER_CHAR = 3;

public:
    // Decoder and Encoder
    static std::u32string* decodeUTF8(uint8_t *bytes, int offset, int len);
    static uint8_t *encodeUTF8(const std::u32string *str);

    static int computeUTF8Length(const std::u32string *str);
};

#endif // FLINK_TNEL_STRINGUTF8UTILS_H

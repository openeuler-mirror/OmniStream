/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: String Value for DataStream
 */

#include <cstdint>
#include <stdexcept>
#include "StringValue.h"

StringValue::StringValue() = default;

void StringValue::write(DataOutputSerializer &out)
{
    unsigned int len = len_;

    // write the length, variable-length encoded
    while (len >= HIGH_BIT) {
        out.write(len | HIGH_BIT);
        len >>= 7;
    }
    out.write(len);

    // write the char data, variable length encoded
    for (unsigned int i = 0; i < len_; i++) {
        auto c = static_cast<uint32_t>(value_[i]);

        while (c >= HIGH_BIT) {
            out.write(c | HIGH_BIT);
            c >>= 7;
        }
        out.write(c);
    }
}

void StringValue::read(DataInputView &in)
{
    value_.clear();

    unsigned int len = in.readUnsignedByte();
    if (len >= HIGH_BIT) {
        int shift = 7;
        unsigned int curr;
        len = len & 0x7f;
        while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
            len |= (curr & 0x7f) << shift;
            shift += 7;
        }
        len |= curr << shift;
    }
    len_ = len;

    value_.reserve(len);

    // len is the size of char of string not the byte size of the string
    for (unsigned int i = 0; i < len; i++) {
        unsigned int c = in.readUnsignedByte();
        if (c < HIGH_BIT) {
            value_.push_back(c);
        } else {
            int shift = 7;
            unsigned int curr;
            c = c & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                c |= (curr & 0x7f) << shift;
                shift += 7;
            }
            c |= curr << shift;
            value_.push_back(c);;
        }
    }
}

// return the ref, means the value_ ownership is still in this object, although caller may change the internal
// content of value_
const std::u32string &StringValue::getValue() const
{
    return value_;
}

// notice, the input argument value is copied (Copy Assignment Operator) to the internal value_
// value is copied
void StringValue::setValue(const std::u32string &value)
{
    value_ = value;
    len_ = value.size();
}

//
void StringValue::writeString(const std::u32string *value, DataOutputSerializer &out)
{
    if (value != nullptr) {
        auto &cs = const_cast<std::u32string &>(*value);

        uint32_t strlen = cs.length();

        // the length we write is offset_ by one, because a length of zero indicates a null value
        uint32_t lenToWrite = strlen + 1;
        if (lenToWrite < strlen) {
            THROW_LOGIC_EXCEPTION("CharSequence is too long.");
        }

        // string is prefixed by it's variable length encoded size, which can take 1-5 bytes.
        if (lenToWrite < HIGH_BIT) {
            out.write((uint8_t) lenToWrite);
        } else if (lenToWrite < HIGH_BIT14) {
            out.write((lenToWrite | HIGH_BIT));
            out.write((lenToWrite >> 7));
        } else if (lenToWrite < HIGH_BIT21) {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14));
        } else if (lenToWrite < HIGH_BIT28) {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14) | HIGH_BIT);
            out.write((lenToWrite >> 21));
        } else {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14) | HIGH_BIT);
            out.write((lenToWrite >> 21) | HIGH_BIT);
            out.write((lenToWrite >> 28));
        }
        // write the char data, variable length encoded
        for (uint32_t i = 0; i < strlen; i++) {
            uint32_t c = cs[i];

            // manual loop unroll, as it performs much better on jdk8
            if (c < HIGH_BIT) {
                out.write(c);
            } else if (c < HIGH_BIT14) {
                out.write(c | HIGH_BIT);
                out.write((c >> 7));
            } else {
                out.write(c | HIGH_BIT);
                out.write((c >> 7) | HIGH_BIT);
                out.write((c >> 14));
            }
        }
    } else {
        out.write(0);
    }
}

std::u32string *StringValue::readString(SysDataInput& in)
{
    // the length we read is offset_ by one, because a length of zero indicates a null value
    unsigned int len = in.readUnsignedByte();

    LOG("first len" + std::to_string(len))

    if (len == 0) {
        return nullptr;
    }

    if (len >= HIGH_BIT) {
        int shift = 7;
        unsigned int curr;
        len = len & 0x7f;
        while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
            len |= (curr & 0x7f) << shift;
            shift += 7;
        }
        len |= curr << shift;
    }

    // subtract one for the null length
    len -= 1;
    LOG("final len" + std::to_string(len))

    auto* data = new std::u32string();
    if (len > SHORT_STRING_MAX_LENGTH) {
        data->reserve(len);
    } else {
        data->reserve(SHORT_STRING_MAX_LENGTH);
    }

    for (unsigned int i = 0; i < len; i++) {
        unsigned int c = in.readUnsignedByte();
        if (c >= HIGH_BIT) {
            int shift = 7;
            unsigned int curr;
            c = c & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                c |= (curr & 0x7f) << shift;
                shift += 7;
            }
            c |= curr << shift;
        }
        data->push_back(c);
    }

    LOG("final string" + u32string_to_std_string(*data));

    return data;
}


void StringValue::writeString(String *buffer, DataOutputSerializer &out)
{
    std::string_view value = buffer->getValue();
    if (likely(!value.empty())) {
        uint32_t strlen = value.size();

        // the length we write is offset_ by one, because a length of zero indicates a null value
        uint32_t lenToWrite = strlen + 1;
        if (unlikely(lenToWrite < strlen)) {
            THROW_LOGIC_EXCEPTION("CharSequence is too long.");
        }

        // string is prefixed by it's variable length encoded size, which can take 1-5 bytes.
        if (likely(lenToWrite < HIGH_BIT)) {
            out.write((uint8_t) lenToWrite);
        } else if (lenToWrite < HIGH_BIT14) {
            out.write((lenToWrite | HIGH_BIT));
            out.write((lenToWrite >> 7));
        } else if (lenToWrite < HIGH_BIT21) {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14));
        } else if (lenToWrite < HIGH_BIT28) {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14) | HIGH_BIT);
            out.write((lenToWrite >> 21));
        } else {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14) | HIGH_BIT);
            out.write((lenToWrite >> 21) | HIGH_BIT);
            out.write((lenToWrite >> 28));
        }
        // use memcpy to write all data
        out.write((uint8_t *) value.data(), strlen, 0, strlen);
    } else {
        out.write(0);
    }
}

void StringValue::readString(String *buffer, SysDataInput& in)
{
    // the length we read is offset_ by one, because a length of zero indicates a null value
    unsigned int len = in.readUnsignedByte();
#ifdef DEBUG
    LOG("first len" + std::to_string(len))
#endif
    if (unlikely(len == 0)) {
        return;
    }

    if (unlikely(len >= HIGH_BIT)) {
        int shift = 7;
        unsigned int curr;
        len = len & 0x7f;
        while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
            len |= (curr & 0x7f) << shift;
            shift += 7;
        }
        len |= curr << shift;
    }

    // subtract one for the null length
    len -= 1;
    char* data = buffer->getData();
    size_t capacity = buffer->getSize();
    // read all date into str
    if (unlikely(capacity < len)) {
        buffer->resize(len);
        data = buffer->data();
    }

    in.readFully(reinterpret_cast<uint8_t *>(data), len, 0, len);
    buffer->resize(len);
}



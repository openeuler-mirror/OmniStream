/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: String value
 * */

#ifndef FLINK_TNEL_STRINGVALUE_H
#define FLINK_TNEL_STRINGVALUE_H

#include <string>
#include "../io/IOReadableWritable.h"
#include "basictypes/String.h"

class StringValue : public IOReadableWritable {
public:
    StringValue();
    void write(DataOutputSerializer &out) override;

    void read(DataInputView &in) override;

    const std::u32string &getValue() const;

    void setValue(const std::u32string &value);

    // the following two static functions are for  ser/der of **string**, not the StringValue.

    //  string value ownership is still in the caller side. using pointer instead of ref is to support
    //  null object (nullptr).
    //  Notice the static writeString/readString has different handling in len with
    // instance function read/write.   The difference is from the flink java implementation.
    // The root reason requires more investigation.
    static  void writeString(const std::u32string *value, DataOutputSerializer &out);

    // the ownership of return string is transferred to caller
    static  std::u32string* readString(SysDataInput& in);

    static void writeString(String *buffer, DataOutputSerializer &out);

    // the ownership of return string is transferred to caller
    static void readString(String *buffer, SysDataInput& in);

private:
    static const int HIGH_BIT = 0x1 << 7;

    static const int HIGH_BIT14 = 0x1 << 14;

    static const int HIGH_BIT21 = 0x1 << 21;

    static const int HIGH_BIT28 = 0x1 << 28;

    static const int HIGH_BIT2 = 0x1 << 13;

    static const int HIGH_BIT2_MASK = 0x3 << 6;

    static const int SHORT_STRING_MAX_LENGTH = 2048;

    std::u32string value_;
    unsigned int len_{};
};


#endif //FLINK_TNEL_STRINGVALUE_H

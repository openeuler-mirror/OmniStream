/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_STRINGBUILDER_H
#define FLINK_TNEL_STRINGBUILDER_H

#include "basictypes/String.h"

class StringBuilder : public Object {
public:
    StringBuilder();

    StringBuilder(std::string str);

    StringBuilder(int size);

    ~StringBuilder();

    // todo returnType is different from java
    StringBuilder* append(String *input);

    StringBuilder* append(Object *input);

    // this contains copy op of std::string
    StringBuilder* append(const std::string &input);

//    std::unique_ptr<String> toStringUniquePtr();

    std::string toString();

    int32_t length();

    StringBuilder* deleteCharAt(int32_t idx);
    // expand other append(), such as append(int value)
    static constexpr int INIT_SIZE = 16;

private:
    std::vector<char> value;
};

#endif // FLINK_TNEL_STRINGBUILDER_H

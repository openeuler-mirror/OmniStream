/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_STRINGCONSTANT_H
#define FLINK_TNEL_STRINGCONSTANT_H
#include "Object.h"
#include "String.h"

class StringConstant : public Object {
public:
    using StringMap = std::unordered_map<std::string, String *>;
    StringMap stringMap;

    static StringConstant& getInstance();
    String* get(const std::string& str);

private:
    explicit StringConstant();
    ~StringConstant();

    StringConstant(const StringConstant&) = delete;
    StringConstant& operator=(const StringConstant&) = delete;
};
#endif //FLINK_TNEL_STRINGCONSTANT_H

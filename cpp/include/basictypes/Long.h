/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/17/25.
//

#ifndef FLINK_TNEL_LONG_H
#define FLINK_TNEL_LONG_H

#include <iostream>
#include "String.h"

class Long : public Object {
public:
    Long();

    Long(int64_t val);

    ~Long();

    int64_t getValue();

    void setValue(int64_t val);

    int hashCode() override;

    bool equals(Object *obj) override;

    std::string toString() override;

    Object *clone() override;

    int64_t longValue();

    static Long *valueOf(String *str);

    static Long *valueOf_tune(String *str);

    static Long *valueOf(std::string str);

    static Long *valueOf_tune(std::string str);

    static Long *valueOf(int64_t val);

    static Long *valueOf_tune(int64_t val);

    void putRefCount();

    int64_t value;
    Long *next = nullptr;

protected:
    static std::uint64_t parseLong(std::string_view s);
};

#endif // FLINK_TNEL_LONG_H

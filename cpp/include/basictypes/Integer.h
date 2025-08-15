/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_Integer_H
#define FLINK_TNEL_Integer_H

#include "String.h"

class Integer : public Object {
public:
    Integer();

    Integer(int32_t val);

    ~Integer();

    int32_t getValue();

    int32_t jsonValue();

    void setValue(int32_t val);

    int hashCode() override;

    bool equals(Object *obj) override;

    std::string toString() override;

    Object *clone() override;

    int32_t intValue();

    static Integer *valueOf(String *str);

    static Integer *valueOf(int32_t val);

protected:
    static std::uint32_t parseInt(std::string_view s) noexcept;

private:
    int32_t value;
};

#endif // FLINK_TNEL_Integer_H

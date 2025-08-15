/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef CPP_SERIALIZATIONSCHEMA_H
#define CPP_SERIALIZATIONSCHEMA_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "basictypes/Object.h"

class SerializationSchema {
public:
    // 虚析构函数，确保正确释放派生类对象
    virtual ~SerializationSchema() = default;

    virtual std::vector<uint8_t> serialize(Object* element) = 0;
};

#endif // CPP_SERIALIZATIONSCHEMA_H

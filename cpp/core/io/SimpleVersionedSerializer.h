/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_SIMPLEVERSIONEDSERIALIZER_H
#define OMNISTREAM_SIMPLEVERSIONEDSERIALIZER_H

#include <vector>
#include <stdexcept>
#include <memory>

template <typename E>
class SimpleVersionedSerializer {
public:
    // 虚析构函数，确保正确释放派生类对象
    virtual ~SimpleVersionedSerializer() = default;

    // 获取序列化版本
    virtual int getVersion() const = 0;

    // 序列化对象
    virtual std::vector<uint8_t> serialize(const E& obj) = 0;

    // 反序列化对象
    virtual E* deserialize(int version, std::vector<uint8_t>& serialized) = 0;
};

#endif // OMNISTREAM_SIMPLEVERSIONEDSERIALIZER_H

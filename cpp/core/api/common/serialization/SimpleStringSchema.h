/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef CPP_SIMPLESTRINGSCHEMA_H
#define CPP_SIMPLESTRINGSCHEMA_H


#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include <locale>
#include <codecvt>
#include "DeserializationSchema.h"
#include "SerializationSchema.h"
#include "basictypes/StringView.h"

class SimpleStringSchema : public DeserializationSchema, public SerializationSchema {
public:
    // 默认构造函数，使用 UTF-8 编码
    SimpleStringSchema();
    ~SimpleStringSchema() override;
    // 带字符集参数的构造函数
    explicit SimpleStringSchema(const std::string& charsetName);
    // 获取当前使用的字符集名称
    std::string_view getCharset() const;

    // 反序列化方法
    Object *deserialize(const uint8_t* message, size_t length) override;
    // 判断是否为流结束
    bool isEndOfStream(const void* nextElement) override;

    std::vector<uint8_t> serialize(Object* element) override;

private:
    std::string_view charsetName;
    String *buffer;
};


#endif // CPP_SIMPLESTRINGSCHEMA_H

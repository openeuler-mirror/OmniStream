/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SimpleStringSchema.h"
#include <string>

// 默认构造函数，使用 UTF-8 编码, only support UTF-8
SimpleStringSchema::SimpleStringSchema() : charsetName("UTF-8")
{
    buffer = new String();
}

SimpleStringSchema::~SimpleStringSchema()
{
    delete buffer;
}

// 带字符集参数的构造函数
SimpleStringSchema::SimpleStringSchema(const std::string& charsetName) : charsetName(charsetName)
{
    if (charsetName.empty() || charsetName != "UTF-8") {
        throw std::invalid_argument("Charset name can only be UTF-8");
    }
    buffer = new String();
}

// 获取当前使用的字符集名称
std::string_view SimpleStringSchema::getCharset() const
{
    return charsetName;
}

// 反序列化方法
Object *SimpleStringSchema::deserialize(const uint8_t* message, size_t length)
{
    buffer->getRefCount();
    buffer->setData((char *) message);
    buffer->setSize(length);
    return buffer;
}

// 判断是否为流结束
bool SimpleStringSchema::isEndOfStream(const void* nextElement)
{
    return false;
}

std::vector<uint8_t> SimpleStringSchema::serialize(Object* element)
{
    auto record = reinterpret_cast<String *>(element)->getValue();
    return {record.begin(), record.end()};
}
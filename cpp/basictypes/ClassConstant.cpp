/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/ClassConstant.h"
ClassConstant& ClassConstant::getInstance()
{
    thread_local static ClassConstant* instance = new ClassConstant(); // 初始化时创建 16 个对象
    return *instance;
}

ClassConstant::ClassConstant() = default;

ClassConstant::~ClassConstant()
{
    for (auto& pair : classMap) {
        delete pair.second;
    }
    classMap.clear();
};

Class* ClassConstant::get(const std::string& str)
{
    auto it = classMap.find(str);
    if (it != classMap.end()) {
        return it->second;
    } else {
        Class* newClass = new Class(str);
        classMap[str] = newClass;
        return newClass;
    }
}
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SOURCESPLIT_H
#define FLINK_TNEL_SOURCESPLIT_H

#include <string>

// 定义抽象基类 SourceSplit
class SourceSplit {
public:
    // 纯虚函数，类似于 Java 接口中的抽象方法
    virtual std::string splitId() const  = 0;

    // 虚析构函数，确保派生类对象能被正确释放
    virtual ~SourceSplit() = default;
};

#endif // FLINK_TNEL_SOURCESPLIT_H

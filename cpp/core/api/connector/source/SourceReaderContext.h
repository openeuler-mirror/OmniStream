/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SOURCEREADERCONTEXT_H
#define FLINK_TNEL_SOURCEREADERCONTEXT_H

class SourceReaderContext {
public:
    // 构造函数
    SourceReaderContext(const int subtaskIndex)
        :subtaskIndex(subtaskIndex){}

    // 析构函数
    ~SourceReaderContext() {}

    int getSubTaskIndex() {
        return subtaskIndex;
    }
private:
    int subtaskIndex;
};

#endif // FLINK_TNEL_SOURCEREADERCONTEXT_H

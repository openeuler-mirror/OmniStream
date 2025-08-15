/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SPLITREADER_H
#define FLINK_TNEL_SPLITREADER_H

#include "RecordsWithSplitIds.h"
#include "connector-kafka/source/split/KafkaPartitionSplit.h"

template <typename E, typename SplitT>
class SplitReader {
public:
    // 纯虚函数，用于从数据源获取记录
    virtual RecordsWithSplitIds<E>* fetch() = 0;
    // 纯虚函数，用于处理分片变更
    virtual void handleSplitsChanges(const std::vector<SplitT*>& splitsChange) = 0;
    // 纯虚函数，用于唤醒读取操作
    virtual void wakeUp() = 0;
    // 纯虚函数，用于关闭读取器
    virtual void close() = 0;
    // 虚析构函数，确保正确释放派生类对象
    virtual ~SplitReader() = default;
};

#endif // FLINK_TNEL_SPLITREADER_H

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SOURCEREADER_H
#define FLINK_TNEL_SOURCEREADER_H

#include <memory>
#include <vector>
#include <future>
#include <string>
#include "connector-kafka/source/split/KafkaPartitionSplit.h"
#include "core/io/InputStatus.h"
#include "ReaderOutput.h"
#include "runtime/operators/coordination/OperatorEvent.h"
#include "core/utils/threads/CompletableFuture.h"

template <typename SplitT>
class SourceReader {
public:
    virtual ~SourceReader() = default;

    // 启动读取器
    virtual void start() = 0;

    // 轮询下一个可用记录到 ReaderOutput 中
    virtual InputStatus pollNext(ReaderOutput* output) = 0;

    // 对源的状态进行检查点操作
    virtual std::vector<SplitT> snapshotState(long checkpointId) = 0;

    // 返回一个 future，用于指示读取器有数据可用
    virtual std::shared_ptr<omnistream::CompletableFuture> getAvailable() = 0;

    // 添加要读取的拆分列表
    virtual void addSplits(std::vector<SplitT*>& splits) = 0;

    // 通知读取器不会再收到更多拆分
    virtual void notifyNoMoreSplits() = 0;

    virtual void handleSourceEvents(const SourceEvent& sourceEvent) {
        // 默认实现为空
    }

    // 通知检查点完成
    virtual void notifyCheckpointComplete(long checkpointId) {
        // 默认实现为空
    }

    // 关闭读取器，继承自 AutoCloseable 功能的体现
    virtual void close() = 0;
};
#endif  //FLINK_TNEL_SOURCEREADER_H

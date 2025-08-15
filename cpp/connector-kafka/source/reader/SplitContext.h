/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SPLITCONTEXT_H
#define FLINK_TNEL_SPLITCONTEXT_H

#include "core/api/connector/source/ReaderOutput.h"
#include "connector-kafka/source/split/KafkaPartitionSplitState.h"

class SplitContext {
public:
    std::string splitId;
    std::shared_ptr<KafkaPartitionSplitState> state;
    SourceOutput* sourceOutput = nullptr;

    SplitContext(const std::string& splitId, const std::shared_ptr<KafkaPartitionSplitState>& state)
        : splitId(splitId), state(state) {}

    // 获取或创建 SplitOutput 的方法
    SourceOutput &getOrCreateSplitOutput(ReaderOutput* mainOutput)
    {
        if (sourceOutput == nullptr) {
            // 拆分输出应该在 SourceOperator 中处理 AddSplitsEvent 时创建。
            // 这里我们只是使用此方法来获取先前创建的输出。
            sourceOutput = &(mainOutput->CreateOutputForSplit(splitId));
        }
        return *sourceOutput;
    }
};


#endif // FLINK_TNEL_SPLITCONTEXT_H

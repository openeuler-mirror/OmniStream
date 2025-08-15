/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RECORDEMITTER_H
#define FLINK_TNEL_RECORDEMITTER_H

#include <memory>
#include "core/api/connector/source/SourceOutput.h"


template <typename E, typename SplitStateT>
class RecordEmitter {
public:
    virtual void emitRecord(E* consumerRecord, SourceOutput* output,
        std::shared_ptr<SplitStateT>& splitState) = 0;
    virtual void emitBatchRecord(const std::vector<E*>& messageVec, SourceOutput* output,
        std::shared_ptr<SplitStateT>& splitState) = 0;
    // 虚析构函数，确保正确释放派生类对象
    virtual ~RecordEmitter() = default;
};

#endif // FLINK_TNEL_RECORDEMITTER_H

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SINGLETHREADMULTIPLEXSOURCEREADERBASE_H
#define FLINK_TNEL_SINGLETHREADMULTIPLEXSOURCEREADERBASE_H

#include <iostream>
#include <memory>
#include <queue>
#include "SourceReaderBase.h"


template <typename E, typename SplitT, typename SplitStateT>
class SingleThreadMultiplexSourceReaderBase : public SourceReaderBase<E, SplitT, SplitStateT> {
public:
    SingleThreadMultiplexSourceReaderBase(std::shared_ptr<FutureCompletingBlockingQueue<E>>& elementsQueue,
        std::shared_ptr<SingleThreadFetcherManager<E, SplitT>>& splitFetcherManager,
        std::shared_ptr<RecordEmitter<E, SplitStateT>>& recordEmitter,
        const std::shared_ptr<SourceReaderContext>& context, bool isBatch)
        : SourceReaderBase<E, SplitT, SplitStateT>(elementsQueue,
            splitFetcherManager, recordEmitter, context, isBatch) {}
};

#endif // FLINK_TNEL_SINGLETHREADMULTIPLEXSOURCEREADERBASE_H

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include "NettyBufferPool.h"
#include "common.h"
#include "core/streamrecord/StreamRecord.h"
#include "include/functions/Watermark.h"
#include "partition/BufferAvailabilityListener.h"
#include "partition/ResultPartitionManager.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace omnistream {
    class OmniCreditBasedSequenceNumberingViewReader
        : public BufferAvailabilityListener {
    public:
        OmniCreditBasedSequenceNumberingViewReader(ResultPartitionIDPOD partitionId,
                                                   int subPartitionIndex,
                                                   long outputBufferStatus);
        ~OmniCreditBasedSequenceNumberingViewReader() override;
        void notifyDataAvailable() override;
        void requestSubpartitionView(
            std::shared_ptr<ResultPartitionManager> resultPartitionManager,
            ResultPartitionIDPOD partitionId, int subPartitionId);
        int getAvailabilityAndBacklog();
        void getNextBufferInternal();
        int getNextBuffer();
        void RecycleNettyBuffer(long address);
        std::shared_ptr<NettyBufferInfo> RequestNettyBuffer(int size);
        bool SerializeVectorBatch(VectorBatch* vectorBatch,
                                  std::shared_ptr<NettyBufferInfo>& bufferInfo);
        void DoSerializeVectorBatch(VectorBatch* vectorBatch, int vectorSize,
                                    std::shared_ptr<NettyBufferInfo>& bufferInfo);
        void AddNettyBufferInfoToQueue(std::shared_ptr<NettyBufferInfo>& bufferInfo);
        bool DoSerializeWaterMark(long timestamp,
                                  std::shared_ptr<NettyBufferInfo> bufferInfo);
        void SerializeBufferAndBacklog(
            std::shared_ptr<VectorBatchBuffer> vectorBatchBuffer);
        void SerializeEvent(
            std::shared_ptr<VectorBatchBuffer> vectorBatchBuffer);
        void SerializeVectorBatchBuffer(
            std::shared_ptr<VectorBatchBuffer> vectorBatchBuffer);
        void DestroyNettyBufferPool() ;

    private:
        std::shared_ptr<ResultSubpartitionView> subpartitionView;
        std::queue<std::shared_ptr<SerializedBatchInfo>> serializedBatchQueue;
        OutputBufferStatus* outputBufferStatus;
        std::recursive_mutex queueMutex;
        int nextDataAvailable = 0;
        NettyBufferPool* nettyBufferPool = nullptr;
        int bufferPoolSize = 100;
        // 32k
        int bufferSize = 32 * 1024;
        std::recursive_mutex fetchingDataMutex;
        int requestNextBufferWaitingTime = 10;
        int noBufferPrintCount = 200;
    };
} // namespace omnistream

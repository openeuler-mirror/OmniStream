/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// PipelinedSubpartition.h
#ifndef PIPELINEDSUBPARTITION_H
#define PIPELINEDSUBPARTITION_H

#include <deque>
#include <memory>
#include <vector>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <mutex>
#include <buffer/ObjectBufferConsumerWithPartialRecordLength.h>

#include "PipelinedSubpartitionView.h"
#include "PrioritizedDeque.h"
#include "ResultSubpartition.h"
#include "buffer/BufferConsumerWithPartialRecordLength.h"

namespace omnistream {


class PipelinedSubpartition : public ResultSubpartition, public  std::enable_shared_from_this<PipelinedSubpartition> {
public:
    PipelinedSubpartition(int index, int receiverExclusiveBuffersPerChannel, std::shared_ptr<ResultPartition> parent);
    ~PipelinedSubpartition() override;

    // int add(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength) override;
    int add(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength) override;

    void finish() override;
    void release() override;
    std::shared_ptr<BufferAndBacklog> pollBuffer();
    void resumeConsumption();
    void acknowledgeAllDataProcessed();
    bool isReleased() override;

    std::shared_ptr<ResultSubpartitionView> createReadView(std::shared_ptr<BufferAvailabilityListener> availabilityListener) override;
    AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable);
    int getNumberOfQueuedBuffers() override;
    void bufferSize(int desirableNewBufferSize) override;
    std::string toString() override;
    int unsynchronizedGetNumberOfQueuedBuffers() override;
    void flush() override;
   // std::shared_ptr<BufferBuilder> requestBufferBuilderBlocking() override;  chekcpoint related
    // std::shared_ptr<VectorBatchBuffer> buildSliceBuffer(std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength> buffer);
    std::shared_ptr<Buffer> buildSliceBuffer(std::shared_ptr<BufferConsumerWithPartialRecordLength> bufferConsumerWithPartialRecordLength);
    // std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength> getNextBuffer();
    std::shared_ptr<BufferConsumerWithPartialRecordLength> getNextBuffer();

    int getBuffersInBacklogUnsafe() const override;

private:
    int receiverExclusiveBuffersPerChannel;
    // PrioritizedDeque<ObjectBufferConsumerWithPartialRecordLength> buffers;
    PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers;
    std::recursive_mutex buffersMutex;

    int buffersInBacklog;
    std::shared_ptr<PipelinedSubpartitionView> readView;
    bool isFinished;
    bool flushRequested;
    volatile bool isReleased_;
    long totalNumberOfBuffers;
    long totalNumberOfBytes;
    int bufferSize_;
    bool isBlocked;
    int sequenceNumber;

    int add(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength, bool finish);
    bool addBuffer(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength);

    bool isDataAvailableUnsafe();
    ObjectBufferDataType getNextBufferTypeUnsafe();
    long getTotalNumberOfBuffers() const override;
    long getTotalNumberOfBytes() const override;

    bool shouldNotifyDataAvailable();
    void notifyDataAvailable();
    void notifyPriorityEvent(int prioritySequenceNumber);
    int getNumberOfFinishedBuffers();
};

} // namespace omnistream

#endif // PIPELINEDSUBPARTITION_H
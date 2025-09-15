/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_RESULTPARTITION_H
#define OMNISTREAM_RESULTPARTITION_H

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <exception>
#include <buffer/ObjectBufferPoolFactory.h>
#include <metrics/Counter.h>
#include <metrics/SimpleCounter.h>
#include <utils/function/Supplier.h>


#include "ResultPartitionWriter.h"

namespace omnistream {
    class ResultPartitionManager;

    class ResultPartition : public ResultPartitionWriter , public std::enable_shared_from_this<ResultPartition>{
public:
    ResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        int numSubpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory);

    ~ResultPartition() override = default;

        void setup() override;

    std::string getOwningTaskName() const;
    ResultPartitionIDPOD getPartitionId()  override;
    int getPartitionIndex() const;
    int getNumberOfSubpartitions() override;
    std::shared_ptr<ObjectBufferPool> getBufferPool();

    virtual int getNumberOfQueuedBuffers() = 0;
    virtual int getNumberOfQueuedBuffers(int targetSubpartition) = 0;

    int getPartitionType() const;

    void NotifyEndOfData(StopMode mode) override
    {
        NOT_IMPL_EXCEPTION
    }

    std::shared_ptr<CompletableFuture> getAllDataProcessedFuture() override;
    virtual void onSubpartitionAllDataProcessed(int subpartition);

    void finish() override;
    bool isFinished()  override;

    void release();
    void release(std::optional<std::exception_ptr> cause) override;

    void close() override;
            void closeBufferPool();
    void fail(std::optional<std::exception_ptr>  throwable) override;
    std::optional<std::exception_ptr>  getFailureCause() ;

    int getNumTargetKeyGroups()  override;

    bool isReleased()  override;
    std::shared_ptr<CompletableFuture> getAvailableFuture() override;

    std::string toString() const override;
    std::shared_ptr<ResultPartitionManager> getPartitionManager();
    virtual void OnConsumedSubpartition(int subpartitionIndex);

protected:
    virtual void releaseInternal() = 0;
    void checkInProduceState() const;

protected:
    static const std::string LOG_NAME;

    const std::string owningTaskName;
    const int partitionIndex;
    const ResultPartitionIDPOD partitionId;
    const int partitionType;
    const std::shared_ptr<ResultPartitionManager> partitionManager;
    const int numSubpartitions;
    const int numTargetKeyGroups;

    std::atomic<bool> isReleased_{false};
    std::shared_ptr<ObjectBufferPool> bufferPool;
    bool isFinished_{false};
    std::optional<std::exception_ptr> cause;
    std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory_;

    std::shared_ptr<Counter> numBytesOut = std::make_shared<SimpleCounter>();
    std::shared_ptr<Counter> numBuffersOut = std::make_shared<SimpleCounter>();
};

} // namespace omnistream

#endif // OMNISTREAM_RESULTPARTITION_H
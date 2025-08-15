/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// ResultPartitionManager.h
#ifndef OMNISTREAM_RESULTPARTITIONMANAGER_H
#define OMNISTREAM_RESULTPARTITIONMANAGER_H

#include <memory>
#include <unordered_map>
#include <vector>
#include "ResultPartition.h"
#include "ResultSubpartitionView.h"
#include "BufferAvailabilityListener.h"

#include "ResultPartitionProvider.h"

namespace omnistream {

    class ResultPartitionManager : public ResultPartitionProvider {
    public:
        ResultPartitionManager();
        ~ResultPartitionManager() override;

        void registerResultPartition(std::shared_ptr<ResultPartition> partition);

        std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
            const ResultPartitionIDPOD& partitionId,
            int subpartitionIndex,
            std::shared_ptr<BufferAvailabilityListener> availabilityListener) override;

        void releasePartition(const ResultPartitionIDPOD& partitionId, std::optional<std::exception_ptr>  cause);

        void shutdown();

        void onConsumedPartition(std::shared_ptr<ResultPartition> partition);

        std::vector<ResultPartitionIDPOD> getUnreleasedPartitions();

        std::string toString();

    private:
        std::unordered_map<ResultPartitionIDPOD, std::shared_ptr<ResultPartition>> registeredPartitions;
        bool isShutdown;
        std::recursive_mutex mutex_;
    };

} // namespace omnistream

#endif // OMNISTREAM_RESULTPARTITIONMANAGER_H

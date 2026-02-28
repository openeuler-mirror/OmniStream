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
// check
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

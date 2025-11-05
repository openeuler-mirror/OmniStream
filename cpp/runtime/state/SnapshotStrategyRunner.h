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

#ifndef OMNISTREAM_SNAPSHOTSTRATEGYRUNNER
#define OMNISTREAM_SNAPSHOTSTRATEGYRUNNER

#include "SnapshotExecutionType.h"
#include "SnapshotResult.h"
#include "CheckpointStreamFactory.h"
#include "AsyncSnapshotCallable.h"
#include "SnapshotStrategy.h"
#include "SnapshotResources.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "KeyedStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include <string>
#include <future>
#include <chrono>
#include <memory>

template <typename T, typename SR>
class SnapshotStrategyRunner {
public:
    SnapshotStrategyRunner() {};
    SnapshotStrategyRunner(std::string description, SnapshotStrategy<T, SR>* snapshotStrategy, SnapshotExecutionType executionType)
        : description_(description), snapshotStrategy_(snapshotStrategy), executionType_(executionType)
        {}
    ~SnapshotStrategyRunner() {};

    std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> snapshot(
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* streamFactory,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<omnistream::OmniTaskBridge> bridge)
    {
        SnapshotResources *snapshotResources = snapshotStrategy_->syncPrepareResources(checkpointId);
        auto asyncSnapshot = snapshotStrategy_->asyncSnapshot(snapshotResources, checkpointId, timestamp, streamFactory, checkpointOptions);
        auto task = std::make_shared<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>>(
            [asyncSnapshot, bridge]() {
                return asyncSnapshot->get(bridge);
        });

        if (executionType_ == SnapshotExecutionType::SYNCHRONOUS) {
            auto res = asyncSnapshot->get(bridge);
            if (res) {
                LOG("native rocksdb checkpoint has been finished.");
            }
        }
        return task;
    }

private:
    std::string description_;
    SnapshotStrategy<T, SR>* snapshotStrategy_;
    SnapshotExecutionType executionType_;
};

#endif // OMNISTREAM_SNAPSHOTSTRATEGYRUNNER
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

#ifndef OMNISTREAM_SNAPSHOTSTRATEGY_H
#define OMNISTREAM_SNAPSHOTSTRATEGY_H

#include "SnapshotResult.h"
#include "StateObject.h"
#include "SnapshotResources.h"
#include "CheckpointStreamFactory.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "state/bridge/OmniTaskBridge.h"

template <typename S>
class SnapshotResultSupplier {
public:
    virtual ~SnapshotResultSupplier() = default;

    virtual std::shared_ptr<SnapshotResult<S>> get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) = 0;
};

template <typename S, typename SR>
class SnapshotStrategy {
public:
    virtual ~SnapshotStrategy() = default;

    virtual std::shared_ptr<SnapshotResultSupplier<S>> asyncSnapshot(
        const std::shared_ptr<SR>& snapshotResources,
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* streamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") = 0;

    virtual std::shared_ptr<SR> syncPrepareResources(long checkpointId) = 0;
};

#endif // OMNISTREAM_SNAPSHOTSTRATEGY_H
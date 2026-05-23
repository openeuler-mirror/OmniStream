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

#ifndef OMNISTREAM_DEFAULTOPERATORSTATEBACKENDSNAPSHOTSTRATEGY_H
#define OMNISTREAM_DEFAULTOPERATORSTATEBACKENDSNAPSHOTSTRATEGY_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>


#include "core/include/common.h"
#include "runtime/checkpoint/CheckpointOptions.h"

#include "HeapBroadcastState.h"
#include "PartitionableListState.h"

#include "OperatorStateHandle.h"
#include "SnapshotStrategy.h"

#include "SnapshotResult.h"
#include "SnapshotResources.h"
#include "CheckpointStreamFactory.h"
#include "OperatorStreamStateHandle.h"
#include "DefaultOperatorSnapshotOperation.h"
#include "DefaultOperatorStateBackendSnapshotResources.h"
#include "../../core/include/common.h"

class DefaultOperatorStateBackendSnapshotStrategy
    : public SnapshotStrategy<OperatorStateHandle, SnapshotResources> {
public:
    DefaultOperatorStateBackendSnapshotStrategy(
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates)
        : registeredOperatorStates_(registeredOperatorStates),
          registeredBroadcastStates_(registeredBroadcastStates) {
    }

    ~DefaultOperatorStateBackendSnapshotStrategy() = default;

    std::shared_ptr<SnapshotResources> syncPrepareResources(long checkpointId) override {
        auto operatorStateMetaInfoSnapshots = std::vector<std::shared_ptr<StateMetaInfoSnapshot>>();
        auto broadcastStateMetaInfoSnapshots = std::vector<std::shared_ptr<StateMetaInfoSnapshot>>();

        if (registeredOperatorStates_->empty() && registeredBroadcastStates_->empty()) {
            return std::make_shared<DefaultOperatorStateBackendSnapshotResources>(
                registeredOperatorStates_,
                registeredBroadcastStates_,
                operatorStateMetaInfoSnapshots,
                broadcastStateMetaInfoSnapshots);
        }

        if (!registeredOperatorStates_->empty()) {
            for (auto& entry : *registeredOperatorStates_) {
                if (entry.second != nullptr) {
                    INFO_RELEASE("aaa second type:"<< typeid(*(entry.second)).name());
                    auto state = std::dynamic_pointer_cast<PartitionableListState<std::vector<uint8_t>>>(entry.second);
                    if (state) {
                        operatorStateMetaInfoSnapshots.push_back(state->getStateMetaInfo()->snapshot());
                    }else {
                        INFO_RELEASE("aaa not PartitionableListState")
                    }

                }
            }
        }

        if (!registeredBroadcastStates_->empty()) {
            for (auto& entry : *registeredBroadcastStates_) {
                if (entry.second !=  nullptr) {
                    // TODO  需使用broadcast具体类型
                    auto state = std::dynamic_pointer_cast<HeapBroadcastState<std::vector<uint8_t>, std::vector<uint8_t>>>(entry.second);
                    broadcastStateMetaInfoSnapshots.push_back(state->getStateMetaInfo()->snapshot());
                }
            }
        }

        return std::make_shared<DefaultOperatorStateBackendSnapshotResources>(
            registeredOperatorStates_,
            registeredBroadcastStates_,
            operatorStateMetaInfoSnapshots,
            broadcastStateMetaInfoSnapshots);
    }

    std::shared_ptr<SnapshotResultSupplier<OperatorStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp, /* not used */
        CheckpointStreamFactory* streamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer_ = "") override {

        auto operatorSnapshotResources = std::dynamic_pointer_cast<DefaultOperatorStateBackendSnapshotResources>(snapshotResources);

        return std::make_shared<DefaultOperatorSnapshotOperation>(checkpointId,
                                                                           checkpointOptions,
                                                                           streamFactory,
                                                                           operatorSnapshotResources);
    }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates_;
};

#endif //OMNISTREAM_DEFAULTOPERATORSTATEBACKENDSNAPSHOTSTRATEGY_H
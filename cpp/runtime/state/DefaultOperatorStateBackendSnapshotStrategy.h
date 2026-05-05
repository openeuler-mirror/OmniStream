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
#include "CheckpointStateOutputStreamProxy.h"
#include "OperatorStreamStateHandle.h"

class DefaultOperatorStateBackendSnapshotResources : public SnapshotResources {
public:
    DefaultOperatorStateBackendSnapshotResources(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStatesDeepCopies_,
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStatesDeepCopies_,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> operatorStateMetaInfoSnapshots_,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> broadcastStateMetaInfoSnapshots_)
        : registeredOperatorStatesDeepCopies(registeredOperatorStatesDeepCopies_),
          registeredBroadcastStatesDeepCopies(registeredBroadcastStatesDeepCopies_),
          operatorStateMetaInfoSnapshots(std::move(operatorStateMetaInfoSnapshots_)),
          broadcastStateMetaInfoSnapshots(std::move(broadcastStateMetaInfoSnapshots_)) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotResources");
    }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> getRegisteredOperatorStatesDeepCopies() { return registeredOperatorStatesDeepCopies; }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> getRegisteredBroadcastStatesDeepCopies() { return registeredBroadcastStatesDeepCopies; }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> getOperatorStateMetaInfoSnapshots() { return operatorStateMetaInfoSnapshots; }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> getBroadcastStateMetaInfoSnapshots() { return broadcastStateMetaInfoSnapshots; }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStatesDeepCopies;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStatesDeepCopies;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> operatorStateMetaInfoSnapshots;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> broadcastStateMetaInfoSnapshots;
};




class DefaultOperatorStateBackendSnapshotStrategy
    : public SnapshotStrategy<OperatorStateHandle, DefaultOperatorStateBackendSnapshotResources> {
public:
    DefaultOperatorStateBackendSnapshotStrategy(
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStates_)
        : registeredOperatorStates(std::move(registeredOperatorStates_)),
          registeredBroadcastStates(std::move(registeredBroadcastStates_)) {
            INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy");
    }

    std::shared_ptr<DefaultOperatorStateBackendSnapshotResources> syncPrepareResources(long checkpointId_) override;

    std::shared_ptr<SnapshotResultSupplier<OperatorStateHandle>> asyncSnapshot(
        const std::shared_ptr<DefaultOperatorStateBackendSnapshotResources>& snapshotResources_,
        long checkpointId_,
        long timestamp_, /* not used */
        CheckpointStreamFactory* streamFactory_,
        CheckpointOptions* checkpointOptions_,
        std::string keySerializer = "") override;

    std::shared_ptr<SnapshotResult<OperatorStateHandle>> CallMaterializeOperatorMetaData(
        std::shared_ptr<omnistream::OmniTaskBridge> bridge_,
        long checkpointId_,
        CheckpointOptions* checkpointOptions_,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& operatorStateMetaInfoSnapshots_,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& broadcastStateMetaInfoSnapshots_) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::CallMaterializeOperatorMetaData");
        return bridge_->CallMaterializeOperatorMetaData(
            checkpointId_,
            checkpointOptions_,
            operatorStateMetaInfoSnapshots_,
            broadcastStateMetaInfoSnapshots_);
    }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStates;
};




class DefaultOperatorSnapshotOperation : public SnapshotResultSupplier<OperatorStateHandle> {
public:
    DefaultOperatorSnapshotOperation(
        DefaultOperatorStateBackendSnapshotStrategy* parent_,
        long checkpointId_,
        CheckpointOptions* checkpointOptions_,
        CheckpointStreamFactory* streamFactory_,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> operatorStateMetaInfoSnapshots_,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> broadcastStateMetaInfoSnapshots_)
        : parent(parent_),
          checkpointId(checkpointId_),
          checkpointOptions(checkpointOptions_),
          streamFactory(streamFactory_),
          operatorStateMetaInfoSnapshots(operatorStateMetaInfoSnapshots_),
          broadcastStateMetaInfoSnapshots(broadcastStateMetaInfoSnapshots_) {
        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation 1");
    }

    DefaultOperatorSnapshotOperation(
        DefaultOperatorStateBackendSnapshotStrategy* parent_,
        long checkpointId_,
        CheckpointOptions* checkpointOptions_,
        CheckpointStreamFactory* streamFactory_,
        std::shared_ptr<DefaultOperatorStateBackendSnapshotResources> snapshotResources_)
        : parent(parent_),
          checkpointId(checkpointId_),
          checkpointOptions(checkpointOptions_),
          streamFactory(streamFactory_),
          snapshotResources(snapshotResources_) {
        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation 2");
    }

    DefaultOperatorSnapshotOperation(
        DefaultOperatorStateBackendSnapshotStrategy* parent_,
        long checkpointId_,
        CheckpointOptions* checkpointOptions_,
        CheckpointStreamFactory* streamFactory_,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStatesDeepCopies_,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStatesDeepCopies_)
        : parent(parent_),
          checkpointId(checkpointId_),
          checkpointOptions(checkpointOptions_),
          streamFactory(streamFactory_),
          registeredOperatorStatesDeepCopies(registeredOperatorStatesDeepCopies_),
          registeredBroadcastStatesDeepCopies(registeredBroadcastStatesDeepCopies_) {
        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation 2");
    }


    virtual ~DefaultOperatorSnapshotOperation() = default;

    std::shared_ptr<SnapshotResult<OperatorStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) override;

protected:
    DefaultOperatorStateBackendSnapshotStrategy* parent;
    long checkpointId;
    CheckpointOptions* checkpointOptions;
    CheckpointStreamFactory* streamFactory;
    std::shared_ptr<DefaultOperatorStateBackendSnapshotResources> snapshotResources;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> operatorStateMetaInfoSnapshots;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> broadcastStateMetaInfoSnapshots;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStatesDeepCopies;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStatesDeepCopies;
};

#endif //OMNISTREAM_DEFAULTOPERATORSTATEBACKENDSNAPSHOTSTRATEGY_H
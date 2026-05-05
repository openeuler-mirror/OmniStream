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

#include "DefaultOperatorStateBackendSnapshotStrategy.h"
#include "common.h"

std::shared_ptr<DefaultOperatorStateBackendSnapshotResources> DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources(long checkpointId_) {
    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources 1 === checkpointId: " + std::to_string(checkpointId_));
    auto operatorStateMetaInfoSnapshots = std::vector<std::shared_ptr<StateMetaInfoSnapshot>>();
    auto broadcastStateMetaInfoSnapshots = std::vector<std::shared_ptr<StateMetaInfoSnapshot>>();

    if (registeredOperatorStates->empty() && registeredBroadcastStates->empty()) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources is empty");
        return std::make_shared<DefaultOperatorStateBackendSnapshotResources>(
            registeredOperatorStates,
            registeredBroadcastStates,
            operatorStateMetaInfoSnapshots,
            broadcastStateMetaInfoSnapshots);
    }

    if (!registeredOperatorStates->empty()) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources registeredOperatorStates size: " + std::to_string(registeredOperatorStates->size()));
        for (auto& entry : *registeredOperatorStates) {
            if (entry.second != nullptr) {
                INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources for registeredOperatorStates === name: " + entry.first);
                auto listState = std::dynamic_pointer_cast<PartitionableListState<std::vector<uint8_t>>>(entry.second);
                operatorStateMetaInfoSnapshots.push_back(listState->getStateMetaInfo()->snapshot());
            } else {
                INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources for registeredOperatorStates === name: " + entry.first + " is nullptr");
            }
        }
    }

    if (!registeredBroadcastStates->empty()) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources registeredBroadcastStates size: " + std::to_string(registeredBroadcastStates->size()));
        for (auto& entry : *registeredBroadcastStates) {
            if (entry.second !=  nullptr) {
                INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources for registeredBroadcastStates === name: " + entry.first);
                broadcastStateMetaInfoSnapshots.push_back(entry.second->getStateMetaInfo()->snapshot());
            } else {
                INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources for registeredBroadcastStates === name: " + entry.first + " is nullptr");
            }
        }
    }
    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::syncPrepareResources end === operatorStateMetaInfoSnapshots size: " + std::to_string(operatorStateMetaInfoSnapshots.size()));

    return std::make_shared<DefaultOperatorStateBackendSnapshotResources>(
        registeredOperatorStates,
        registeredBroadcastStates,
        operatorStateMetaInfoSnapshots,
        broadcastStateMetaInfoSnapshots);
}

std::shared_ptr<SnapshotResultSupplier<OperatorStateHandle>> DefaultOperatorStateBackendSnapshotStrategy::asyncSnapshot(
    const std::shared_ptr<DefaultOperatorStateBackendSnapshotResources>& snapshotResources_,
    long checkpointId_,
    long timestamp_,
    CheckpointStreamFactory* streamFactory_,
    CheckpointOptions* checkpointOptions_,
    std::string keySerializer) {
    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::asyncSnapshot 1 === checkpointId: " + std::to_string(checkpointId_) + ", timestamp: " + std::to_string(timestamp_));
    auto registeredOperatorStatesDeepCopies = snapshotResources_->getRegisteredOperatorStatesDeepCopies();
    auto registeredBroadcastStatesDeepCopies = snapshotResources_->getRegisteredBroadcastStatesDeepCopies();
    auto operatorStateMetaInfoSnapshots = snapshotResources_->getOperatorStateMetaInfoSnapshots();
    auto broadcastStateMetaInfoSnapshots = snapshotResources_->getBroadcastStateMetaInfoSnapshots();

    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::asyncSnapshot === registeredOperatorStatesDeepCopies " + std::to_string(registeredOperatorStatesDeepCopies->size()));
    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::asyncSnapshot === registeredBroadcastStatesDeepCopies " + std::to_string(registeredBroadcastStatesDeepCopies->size()));
    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::asyncSnapshot === operatorStateMetaInfoSnapshots " + std::to_string(operatorStateMetaInfoSnapshots.size()));
    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::asyncSnapshot === broadcastStateMetaInfoSnapshots " + std::to_string(broadcastStateMetaInfoSnapshots.size()));

    return std::make_shared<DefaultOperatorSnapshotOperation>(this,
                                                              checkpointId_,
                                                              checkpointOptions_,
                                                              streamFactory_,
                                                              snapshotResources_);
}

std::shared_ptr<SnapshotResult<OperatorStateHandle>> DefaultOperatorSnapshotOperation::get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) {
    INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::DefaultOperatorSnapshotOperation::get 1 === checkpointId: " + std::to_string(checkpointId));
    try {
        auto registeredOperatorStatesDeepCopies = snapshotResources->getRegisteredOperatorStatesDeepCopies();
        auto registeredBroadcastStatesDeepCopies = snapshotResources->getRegisteredBroadcastStatesDeepCopies();
        auto operatorStateMetaInfoSnapshots = snapshotResources->getOperatorStateMetaInfoSnapshots();
        auto broadcastStateMetaInfoSnapshots = snapshotResources->getBroadcastStateMetaInfoSnapshots();
        
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::DefaultOperatorSnapshotOperation::get 1.1 === registeredOperatorStatesDeepCopies size: " + std::to_string(registeredOperatorStatesDeepCopies->size())
            + ", registeredBroadcastStatesDeepCopies size: " + std::to_string(registeredBroadcastStatesDeepCopies->size()));
        
        if (registeredOperatorStatesDeepCopies->empty() && registeredBroadcastStatesDeepCopies->empty()) {
            INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::get is empty");
            return SnapshotResult<OperatorStateHandle>::Empty();
        }

        CheckpointStateOutputStreamProxy outputStreamProxy(bridge, checkpointId, checkpointOptions);

        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::DefaultOperatorSnapshotOperation::get 2");
        outputStreamProxy.writeOperatorMetaData(operatorStateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots);

        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::DefaultOperatorSnapshotOperation::get 3");
        int initialMapCapacity = registeredOperatorStatesDeepCopies->size() + registeredBroadcastStatesDeepCopies->size();
        auto writtenStatesMetaData = std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo>(initialMapCapacity);

        INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::DefaultOperatorSnapshotOperation::get 4");
        for (auto& entry : *registeredOperatorStatesDeepCopies) {
            std::string name = entry.first;
            auto state = std::dynamic_pointer_cast<PartitionableListState<std::vector<uint8_t>>>(entry.second);
            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates === name : " + name);
            
            if (state != nullptr) {
                int currentPos = outputStreamProxy.getPos();
                INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 2 === currentPos : " + std::to_string(currentPos));
                DataOutputSerializer out;
                // out.setPosition(currentPos);
                auto offsets = state->write(out, currentPos);
                INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 5 === out.length() : " + std::to_string(out.length()));
                outputStreamProxy.writeBytes(out.getData(), out.length());
                auto distributionMode = state->getStateMetaInfo()->getAssignmentMode();
                writtenStatesMetaData.emplace(name, OperatorStateHandle::StateMetaInfo(offsets, distributionMode));
                INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if end");
            } else {
                INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 === WARNING: dynamic_pointer_cast failed for state: " + name);
            }
        }

        // for (auto& entry : *registeredBroadcastStatesDeepCopies) {
        //     std::string name = entry.first;
        //     std::shared_ptr<HeapBroadcastState<>> state = std::dynamic_pointer_cast<HeapBroadcastState<>>(entry.second);
        //     INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates === name : " + name);
        //     if (state != nullptr) {
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 1");
        //         int currentPos = outputStreamProxy.getPos();
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 2");
        //         DataOutputSerializer out;
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 3");
        //         out.setPosition(currentPos);
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 4");
        //         auto offsets = {state->write(out)};
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 5");
        //         outputStreamProxy.writeBytes(out.getData(), out.length());
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 6");
        //         auto distributionMode = entry.second->getStateMetaInfo()->getAssignmentMode();
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 7");
        //         writtenStatesMetaData.emplace(name, OperatorStateHandle::StateMetaInfo(offsets, distributionMode));
        //         INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if end");
        //     }
        // }

        auto handle = outputStreamProxy.close();
        if (handle) {
            INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::DefaultOperatorSnapshotOperation::get handle != nullptr");
            auto jobManagerOwnedSnapshot = handle->GetJobManagerOwnedSnapshot();
            // snapshotResources->cleanup();
            auto stateHandle = std::make_shared<OperatorStreamStateHandle>(writtenStatesMetaData, jobManagerOwnedSnapshot);
            return SnapshotResult<OperatorStateHandle>::WithLocalState(stateHandle, nullptr);
        }
        // snapshotResources->cleanup();
        return SnapshotResult<OperatorStateHandle>::Empty();
    } catch (const std::exception &e) {
        // snapshotResources->cleanup();
        GErrorLog("DefaultOperatorSnapshotOperation get exception : " + std::string(e.what()));
        throw std::runtime_error("DefaultOperatorSnapshotOperation get failed.");
    }
}
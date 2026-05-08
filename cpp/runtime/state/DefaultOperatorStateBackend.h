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

#ifndef OMNISTREAM_DEFAULTOPERATORSTATEBACKEND_H
#define OMNISTREAM_DEFAULTOPERATORSTATEBACKEND_H

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <stdexcept>
#include <future>
#include <typeinfo>

#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/ListState.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "runtime/state/SnapshotExecutionType.h"
#include "runtime/state/bridge/TaskStateManagerBridge.h"
#include "runtime/state/bridge/OmniTaskBridge.h"

#include "HeapBroadcastState.h"
#include "PartitionableListState.h"
#include "CheckpointStreamFactory.h"
#include "DefaultOperatorStateBackendSnapshotStrategy.h"
#include "OperatorStateBackend.h"
#include "OperatorStateHandle.h"
#include "RegisteredBroadcastStateBackendMetaInfo.h"
#include "RegisteredOperatorStateBackendMetaInfo.h"
#include "SnapshotStrategyRunner.h"
#include "SnapshotResult.h"

class DefaultOperatorStateBackend : public OperatorStateBackend {
public:
    DefaultOperatorStateBackend(bool asynchronousSnapshots,
                                std::shared_ptr<TaskStateManagerBridge> bridge,
                                std::shared_ptr<OmniTaskBridge> omniTaskBridge,
                                DefaultOperatorStateBackendSnapshotStrategy* snapshotStrategy,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedStatesByName,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedBroadcastStatesByName)
        : asynchronousSnapshots_(asynchronousSnapshots),
          bridge_(bridge),
          omniTaskBridge_(omniTaskBridge),
          snapshotStrategy_(snapshotStrategy),
          registeredOperatorStates_(std::move(registeredOperatorStates)),
          registeredBroadcastStates_(std::move(registeredBroadcastStates)),
          accessedStatesByName_(std::move(accessedStatesByName)),
          accessedBroadcastStatesByName_(std::move(accessedBroadcastStatesByName)) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend");
    }

    std::unordered_set<std::string> getRegisteredStateNames() override {
        std::unordered_set<std::string> nameSet;
        for (const auto& pair : *registeredOperatorStates_) {
            nameSet.insert(pair.first);
        }
        return nameSet;
    }

    std::unordered_set<std::string> getRegisteredBroadcastStateNames() override {
        std::unordered_set<std::string> nameSet;
        for (const auto& pair : *registeredBroadcastStates_) {
            nameSet.insert(pair.first);
        }
        return nameSet;
    }

    void close() {}

    void dispose() override {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend dispose");
        if (!registeredOperatorStates_->empty()) {
            registeredOperatorStates_->clear();
        }
        if (!registeredBroadcastStates_->empty()) {
            registeredBroadcastStates_->clear();
        }
        if (!accessedStatesByName_->empty()) {
            accessedStatesByName_->clear();
        }
        if (!accessedBroadcastStatesByName_->empty()) {
            accessedBroadcastStatesByName_->clear();
        }
    }

    template<typename K, typename V>
    std::shared_ptr<HeapBroadcastState<K, V>> getBroadcastState(MapStateDescriptor<K, V>* stateDescriptor) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 1");
        std::string name = stateDescriptor->getName();
        auto accessedIterator = accessedBroadcastStatesByName_->find(name);
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 2");
        if (accessedIterator != accessedBroadcastStatesByName_->end()) {
            std::shared_ptr<HeapBroadcastState<K, V>> state = std::dynamic_pointer_cast<HeapBroadcastState<K, V>>(accessedIterator->second);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 2 a if internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(state->getInternalMap().get())));
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 1 a size :" + std::to_string(state->getInternalMap()->size()));
            return std::dynamic_pointer_cast<HeapBroadcastState<K, V>>(accessedIterator->second);
        }

        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 3");
        std::shared_ptr<HeapBroadcastState<K, V>> resultState = nullptr;
        auto* broadcastStateKeySerializer = stateDescriptor->GetUserKeySerializer();
        auto* broadcastStateValueSerializer = stateDescriptor->GetValueSerializer();

        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 4");
        auto registeredIterator = registeredBroadcastStates_->find(name);

        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5");
        if (registeredIterator == registeredBroadcastStates_->end()) {
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 1 1");
            auto stateMetaInfo = std::make_shared<RegisteredBroadcastStateBackendMetaInfo>(
                    name,
                    OperatorStateHandle::Mode::BROADCAST,
                    broadcastStateKeySerializer,
                    broadcastStateValueSerializer);
            auto internalMap = std::shared_ptr<std::map<K, V>>();
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 1 2");
            resultState = std::make_shared<HeapBroadcastState<K, V>>(stateMetaInfo, internalMap);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 1 3");
            registeredBroadcastStates_->emplace(name, resultState);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 1 end");
        } else {
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 2 1");
            resultState = std::dynamic_pointer_cast<HeapBroadcastState<K, V>>(registeredIterator->second);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 2 2");
            auto stateMetaInfo = resultState->getStateMetaInfo();
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 2 3");
            stateMetaInfo->updateKeySerializer(broadcastStateKeySerializer);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 2 4");
            stateMetaInfo->updateValueSerializer(broadcastStateValueSerializer);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 2 5");
            resultState->setStateMetaInfo(stateMetaInfo);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 5 if 2 end");
        }

        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 6");
        accessedBroadcastStatesByName_->emplace(name, (*registeredBroadcastStates_)[name]);
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState end");

        return resultState;
    }

    template<typename S>
    std::shared_ptr<ListState<S>> getListState(ListStateDescriptor<S>* stateDescriptor)  {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState");
        return getListState(stateDescriptor, OperatorStateHandle::Mode::SPLIT_DISTRIBUTE);
    }

    template<typename S>
    std::shared_ptr<ListState<S>> getUnionListState(ListStateDescriptor<S>* stateDescriptor){
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getUnionListState");
        return getListState(stateDescriptor, OperatorStateHandle::Mode::UNION);
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* streamFactory,
        CheckpointOptions* checkpointOptions) override {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::snapshot 1");
        auto snapshotStrategyRunner = std::make_unique<SnapshotStrategyRunner<OperatorStateHandle, SnapshotResources>>(
                "DefaultOperatorStateBackend snapshot",
                snapshotStrategy_,
                asynchronousSnapshots_ ? ASYNCHRONOUS : SYNCHRONOUS);
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::snapshot 2");

        return snapshotStrategyRunner->snapshot(checkpointId,
                                                timestamp,
                                                streamFactory,
                                                checkpointOptions,
                                                omniTaskBridge_,
                                                "");
    }

private:
    bool asynchronousSnapshots_;
    std::shared_ptr<TaskStateManagerBridge> bridge_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    DefaultOperatorStateBackendSnapshotStrategy* snapshotStrategy_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedStatesByName_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedBroadcastStatesByName_;

    template<typename S>
    std::shared_ptr<ListState<S>> getListState(ListStateDescriptor<S>* stateDescriptor, OperatorStateHandle::Mode mode) {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState 1 ==========" + std::to_string(registeredOperatorStates_->size()));

        std::string name = stateDescriptor->getName();

        auto accessedIterator = accessedStatesByName_->find(name);
        if (accessedIterator != accessedStatesByName_->end()) {
            auto state = std::dynamic_pointer_cast<PartitionableListState<S>>(accessedIterator->second);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState 2 a if internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(state->getInternalList().get())));
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState 1 a size :" + std::to_string(state->getInternalList()->size()));
            return std::dynamic_pointer_cast<PartitionableListState<S>>(accessedIterator->second);
        }

        std::shared_ptr<PartitionableListState<S>> resultState = nullptr;
        auto* operatorStateSerializer = stateDescriptor->getStateSerializer();
        auto registeredIterator = registeredOperatorStates_->find(name);

        if (registeredIterator == registeredOperatorStates_->end()) {
            auto stateMetaInfo = std::make_shared<RegisteredOperatorStateBackendMetaInfo>(name, mode, operatorStateSerializer);
            auto internalList = std::make_shared<std::vector<S>>();
            resultState = std::make_shared<PartitionableListState<S>>(stateMetaInfo, internalList);
            registeredOperatorStates_->emplace(name, resultState);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState 2 a if internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(resultState->getInternalList().get())));
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState 2 a if size :" + std::to_string(resultState->getInternalList()->size()));
        } else {
            resultState = std::dynamic_pointer_cast<PartitionableListState<S>>(registeredIterator->second);
            auto stateMetaInfo = resultState->getStateMetaInfo();
            stateMetaInfo->updateStateSerializer(operatorStateSerializer);
            resultState->setStateMetaInfo(stateMetaInfo);
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState 3 0 if internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(resultState->getInternalList().get())));
            INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState 3 a if size :" + std::to_string(resultState->getInternalList()->size()));
        }

        accessedStatesByName_->emplace(name, (*registeredOperatorStates_)[name]);
        INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getListState end ========" + std::to_string(registeredOperatorStates_->size()));
        return resultState;
    }
};

#endif //OMNISTREAM_DEFAULTOPERATORSTATEBACKEND_H
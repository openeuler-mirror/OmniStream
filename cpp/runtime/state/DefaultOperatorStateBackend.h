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
    DefaultOperatorStateBackend(bool asynchronousSnapshots_,
                                std::shared_ptr<TaskStateManagerBridge> bridge_,
                                std::shared_ptr<OmniTaskBridge> omniTaskBridge_,
                                DefaultOperatorStateBackendSnapshotStrategy* snapshotStrategy_,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStates_,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedStatesByName_,
                                std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> accessedBroadcastStatesByName_)
        : asynchronousSnapshots(asynchronousSnapshots_),
          bridge(bridge_),
          omniTaskBridge(omniTaskBridge_),
          snapshotStrategy(snapshotStrategy_),
          registeredOperatorStates(registeredOperatorStates_),
          registeredBroadcastStates(registeredBroadcastStates_),
          accessedStatesByName(accessedStatesByName_),
          accessedBroadcastStatesByName(accessedBroadcastStatesByName_) {
    }

    std::unordered_set<std::string> getRegisteredStateNames() override {
        std::unordered_set<std::string> nameSet;
        for (const auto& pair : *registeredOperatorStates) {
            nameSet.insert(pair.first);
        }
        return nameSet;
    }

    std::unordered_set<std::string> getRegisteredBroadcastStateNames() override {
        std::unordered_set<std::string> nameSet;
        for (const auto& pair : *registeredBroadcastStates) {
            nameSet.insert(pair.first);
        }
        return nameSet;
    }

    void close() {}

    void dispose() override {
        INFO_RELEASE("DefaultOperatorStateBackend dispose");
        if (!registeredOperatorStates->empty()) {
            registeredOperatorStates->clear();
        }
        if (!registeredBroadcastStates->empty()) {
            registeredBroadcastStates->clear();
        }
        if (!accessedStatesByName->empty()) {
            accessedStatesByName->clear();
        }
        if (!accessedBroadcastStatesByName->empty()) {
            accessedBroadcastStatesByName->clear();
        }
    }

    template<typename K, typename V>
    std::shared_ptr<HeapBroadcastState<K, V>> getBroadcastState(MapStateDescriptor<K, V>* stateDescriptor) {
        // INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 1");
        // std::string name = stateDescriptor->getName();
        // auto accessedIterator = accessedBroadcastStatesByName->find(name);
        // if (accessedIterator != accessedBroadcastStatesByName->end()) {
        //     auto state = std::dynamic_pointer_cast<HeapBroadcastState<>>(accessedIterator->second);
        //     auto newState = std::make_shared<HeapBroadcastState<K, V>>(state->getStateMetaInfo());
        //     newState->setAndConvertInternalMap(state->getInternalMap());
        //     INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState 1 end");
        //     return newState;
        // }

        // std::shared_ptr<HeapBroadcastState<K, V>> resultState = nullptr;
        // auto* broadcastStateKeySerializer = stateDescriptor->GetUserKeySerializer();
        // auto* broadcastStateValueSerializer = stateDescriptor->GetValueSerializer();

        // auto registeredIterator = registeredBroadcastStates->find(name);

        // if (registeredIterator == registeredBroadcastStates->end()) {
        //     auto stateMetaInfo = std::make_shared<RegisteredBroadcastStateBackendMetaInfo>(
        //             name,
        //             OperatorStateHandle::Mode::BROADCAST,
        //             broadcastStateKeySerializer,
        //             broadcastStateValueSerializer);
        //     auto newState = std::make_shared<HeapBroadcastState<>>(stateMetaInfo);
        //     resultState = std::make_shared<HeapBroadcastState<K, V>>(stateMetaInfo);
        //     resultState->setAndConvertInternalMap(newState->getInternalMap());
        //     registeredBroadcastStates->emplace(name, newState);
        // } else {
        //     auto state = std::dynamic_pointer_cast<HeapBroadcastState<>>(registeredIterator->second);
        //     state->getStateMetaInfo()->updateKeySerializer(broadcastStateKeySerializer);
        //     state->getStateMetaInfo()->updateValueSerializer(broadcastStateValueSerializer);
        //     resultState = std::make_shared<HeapBroadcastState<K, V>>(state->getStateMetaInfo());
        //     resultState->setAndConvertInternalMap(state->getInternalMap());
        // }

        // accessedBroadcastStatesByName->emplace(name, (*registeredBroadcastStates)[name]);
        // INFO_RELEASE("h30082497 DefaultOperatorStateBackend::getBroadcastState end");
        // return resultState;
        // return resultState;
        // TODO("getBroadcastState not implemented");
        return nullptr;
    }

    template<typename S>
    std::shared_ptr<ListState<S>> getListState(ListStateDescriptor<S>* stateDescriptor)  {
        return getListState(stateDescriptor, OperatorStateHandle::Mode::SPLIT_DISTRIBUTE);
    }
    template<typename S>
    std::shared_ptr<ListState<S>> getUnionListState(ListStateDescriptor<S>* stateDescriptor){
        return getListState(stateDescriptor, OperatorStateHandle::Mode::UNION);
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* streamFactory,
        CheckpointOptions* checkpointOptions) override {
        auto snapshotStrategyRunner = std::make_unique<SnapshotStrategyRunner<OperatorStateHandle, DefaultOperatorStateBackendSnapshotResources>>(
                "DefaultOperatorStateBackend snapshot",
                snapshotStrategy,
                asynchronousSnapshots ? ASYNCHRONOUS : SYNCHRONOUS);

        return snapshotStrategyRunner->snapshot(checkpointId,
                                                timestamp,
                                                streamFactory,
                                                checkpointOptions,
                                                omniTaskBridge,
                                                "");
    }

private:
    bool asynchronousSnapshots;
    std::shared_ptr<TaskStateManagerBridge> bridge;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge;
    DefaultOperatorStateBackendSnapshotStrategy* snapshotStrategy;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> registeredBroadcastStates;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedStatesByName;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>> accessedBroadcastStatesByName;

    template<typename S>
    std::shared_ptr<ListState<S>> getListState(ListStateDescriptor<S>* stateDescriptor, OperatorStateHandle::Mode mode) {
        std::string name = stateDescriptor->getName();
        INFO_RELEASE("savepoint: DefaultOperatorStateBackend::getListState name :" <<name);
        auto accessedIterator = accessedStatesByName->find(name);
        if (accessedIterator != accessedStatesByName->end()) {
            auto state = std::dynamic_pointer_cast<PartitionableListState<S>>(accessedIterator->second);
            return state;
        }

        std::shared_ptr<PartitionableListState<S>> resultState ;
        auto* operatorStateSerializer = stateDescriptor->getStateSerializer();
        INFO_RELEASE("savepoint: DefaultOperatorStateBackend::getListState type :" <<typeid(*operatorStateSerializer).name());
        auto registeredIterator = registeredOperatorStates->find(name);

        if (registeredIterator == registeredOperatorStates->end()) {
            INFO_RELEASE("savepoint: DefaultOperatorStateBackend::getListState not found");
            auto stateMetaInfo = std::make_shared<RegisteredOperatorStateBackendMetaInfo>(name, mode, operatorStateSerializer);
            resultState = std::make_shared<PartitionableListState<S>>(stateMetaInfo);
            (*registeredOperatorStates)[name] = resultState;
        } else {
            INFO_RELEASE("savepoint: DefaultOperatorStateBackend::getListState found");
            resultState = std::dynamic_pointer_cast<PartitionableListState<S>>(registeredIterator->second);
            resultState->getStateMetaInfo()->updateStateSerializer(operatorStateSerializer);
        }
        (*accessedStatesByName)[name] = resultState;
        return resultState;
    }
};

#endif //OMNISTREAM_DEFAULTOPERATORSTATEBACKEND_H
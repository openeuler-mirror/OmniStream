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

#ifndef OMNISTREAM_DEFAULTOPERATORSTATEBACKENDBUILDER_H
#define OMNISTREAM_DEFAULTOPERATORSTATEBACKENDBUILDER_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

#include "BackendWritableBroadcastState.h"
#include "DefaultOperatorStateBackend.h"
#include "DefaultOperatorStateBackendSnapshotStrategy.h"
#include "OperatorStateHandle.h"
#include "PartitionableListState.h"

class DefaultOperatorStateBackendBuilder {
public:
    DefaultOperatorStateBackendBuilder(bool asynchronousSnapshots_,
                                       std::string operatorIdentifier_,
                                       std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles_,
                                       std::shared_ptr<TaskStateManagerBridge> bridge_,
                                       std::shared_ptr<OmniTaskBridge> omniTaskBridge_)
        : asynchronousSnapshots(asynchronousSnapshots_),
          operatorIdentifier(operatorIdentifier_),
          restoreStateHandles(stateHandles_),
          bridge(bridge_),
          omniTaskBridge(omniTaskBridge_){
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder");
    }

    OperatorStateBackend* build() {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder::build 1");
        auto registeredOperatorStates = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto registeredBroadcastStates = std::make_shared<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>>();
        auto accessedStatesByName = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto accessedBroadcastStatesByName = std::make_shared<std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<>>>>();

        auto snapshotStrategy = new DefaultOperatorStateBackendSnapshotStrategy(registeredOperatorStates, registeredBroadcastStates);

        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder::build ======================== OperatorStateRestoreOperation not impl");

        /*
        OperatorStateRestoreOperation restoreOperation(
            registeredOperatorStates,
            // registeredBroadcastStates,
            restoreStateHandles,
            omniTaskBridge);

        try {
            restoreOperation.restore();
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed when trying to restore operator state backend"+ std::string(e.what()));
        }*/

        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder::build end");
        return new DefaultOperatorStateBackend(
            asynchronousSnapshots,
            bridge,
            omniTaskBridge,
            snapshotStrategy,
            registeredOperatorStates,
            registeredBroadcastStates,
            accessedStatesByName,
            accessedBroadcastStatesByName);
    }

protected:
    bool asynchronousSnapshots;
    std::string operatorIdentifier;
    std::vector<std::shared_ptr<OperatorStateHandle>> restoreStateHandles;
    std::shared_ptr<TaskStateManagerBridge> bridge;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge;
};

#endif //OMNISTREAM_DEFAULTOPERATORSTATEBACKENDBUILDER_H
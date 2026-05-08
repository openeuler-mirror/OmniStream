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
#include "OperatorStateRestoreOperation.h"

class DefaultOperatorStateBackendBuilder {
public:
    DefaultOperatorStateBackendBuilder(bool asynchronousSnapshots,
                                       std::string operatorIdentifier,
                                       std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles,
                                       std::shared_ptr<TaskStateManagerBridge> bridge,
                                       std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : asynchronousSnapshots_(asynchronousSnapshots),
          operatorIdentifier_(operatorIdentifier),
          restoreStateHandles_(stateHandles),
          bridge_(bridge),
          omniTaskBridge_(omniTaskBridge){
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder");
    }

    OperatorStateBackend* build() {
        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder::build 1");
        auto registeredOperatorStates = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto registeredBroadcastStates = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto accessedStatesByName = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto accessedBroadcastStatesByName = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();

        auto snapshotStrategy = new DefaultOperatorStateBackendSnapshotStrategy(registeredOperatorStates, registeredBroadcastStates);

        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder::build ======================== OperatorStateRestoreOperation not impl");

        auto restoreOperation = std::make_shared<OperatorStateRestoreOperation>(
            registeredOperatorStates,
            registeredBroadcastStates,
            restoreStateHandles_,
            omniTaskBridge_);
        try {
            restoreOperation->restore();
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed when trying to restore operator state backend"+ std::string(e.what()));
        }

        INFO_RELEASE("h30082497 DefaultOperatorStateBackendBuilder::build end");
        return new DefaultOperatorStateBackend(
            asynchronousSnapshots_,
            bridge_,
            omniTaskBridge_,
            snapshotStrategy,
            registeredOperatorStates,
            registeredBroadcastStates,
            accessedStatesByName,
            accessedBroadcastStatesByName);
    }

protected:
    bool asynchronousSnapshots_;
    std::string operatorIdentifier_;
    std::vector<std::shared_ptr<OperatorStateHandle>> restoreStateHandles_;
    std::shared_ptr<TaskStateManagerBridge> bridge_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
};

#endif //OMNISTREAM_DEFAULTOPERATORSTATEBACKENDBUILDER_H
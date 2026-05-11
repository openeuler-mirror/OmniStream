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
#ifndef OMNISTREAM_OPERATORSTATERESTOREOPERATION_H
#define OMNISTREAM_OPERATORSTATERESTOREOPERATION_H

#include <unordered_map>
#include <set>
#include <vector>
#include <string>
#include <nlohmann/json.hpp>
#include "PartitionableListState.h"
#include "RegisteredBroadcastStateBackendMetaInfo.h"
#include "state/bridge/OmniTaskBridge.h"
#include "OperatorStreamStateHandle.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"
#include "RegisteredOperatorStateBackendMetaInfo.h"

class OperatorStateRestoreOperation {
public:
    OperatorStateRestoreOperation(
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates,
        const std::vector<std::shared_ptr<OperatorStateHandle>>& stateHandles,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : registeredOperatorStates_(registeredOperatorStates),
          registeredBroadcastStates_(registeredBroadcastStates),
          stateHandles_(stateHandles),
          omniTaskBridge_(omniTaskBridge) {}

    ~OperatorStateRestoreOperation() = default;
    
    void restore() 
    {
        INFO_RELEASE("savepoint: OperatorStateRestoreOperation::restore stateHandles_ size: " + std::to_string(stateHandles_.size()));
        if (stateHandles_.empty()) {
            INFO_RELEASE("savepoint: OperatorStateRestoreOperation::restore stateHandles_ is empty, skip restore");
            return;
        }
        int handleIdx = 0;
        for (auto& stateHandle : stateHandles_) {
            INFO_RELEASE("savepoint: OperatorStateRestoreOperation::restore handleIdx: " + std::to_string(handleIdx)
                + ", handle ptr: " + std::to_string(reinterpret_cast<uintptr_t>(stateHandle.get())));
            auto streamStateHandle = std::dynamic_pointer_cast<OperatorStreamStateHandle>(stateHandle);
            if (streamStateHandle) {
                INFO_RELEASE("savepoint: OperatorStateRestoreOperation::restore streamStateHandle cast OK, omniTaskBridge isNull: "
                    + std::string(omniTaskBridge_ == nullptr ? "true" : "false"));
                auto json = TaskStateSnapshotSerializer::parseOperatorStreamStateHandle(streamStateHandle);
                auto stateMetaInfoSnapshots = omniTaskBridge_->readOperatorMetaData(to_string(json));
                INFO_RELEASE("savepoint: OperatorStateRestoreOperation::restore stateMetaInfoSnapshots count: "
                    + std::to_string(stateMetaInfoSnapshots.size()));
                auto cppResult = omniTaskBridge_->restoreOperatorStreamState(to_string(json));
                convertResult(cppResult, stateMetaInfoSnapshots);
                INFO_RELEASE("savepoint: OperatorStateRestoreOperation::restore after convertResult registeredOperatorStates size: "
                    + std::to_string(registeredOperatorStates_->size()));
            } else {
                INFO_RELEASE("savepoint: OperatorStateRestoreOperation::restore streamStateHandle cast FAILED, handle type: "
                    + std::string(typeid(*stateHandle).name()));
            }
            handleIdx++;
        }
    }
    
    void convertResult(const std::string& cppResult,
        std::vector<StateMetaInfoSnapshot>& stateMetaInfoSnapshots)
    {
        INFO_RELEASE("xuhb_test OperatorStateRestoreOperation restore cppResult=" << cppResult)
        INFO_RELEASE("savepoint: OperatorStateRestoreOperation::convertResult cppResult len: " + std::to_string(cppResult.size())
            + ", snapshots count: " + std::to_string(stateMetaInfoSnapshots.size()));
        nlohmann::json parsed = nlohmann::json::parse(cppResult);
        
        std::string stateName;
        nlohmann::json value;

        for (auto& snapshot : stateMetaInfoSnapshots) {
            INFO_RELEASE("savepoint: OperatorStateRestoreOperation::convertResult snapshot name: " + snapshot.getName());
            auto metaInfo = std::make_shared<RegisteredOperatorStateBackendMetaInfo>(snapshot);
            if (parsed.contains(snapshot.getName())) {
                stateName = snapshot.getName();
                value = parsed[stateName];
                INFO_RELEASE("savepoint: OperatorStateRestoreOperation::convertResult found stateName in parsed: " + stateName
                    + ", in typeByteStateNames: " + std::string(typeByteStateNames.find(stateName) != typeByteStateNames.end() ? "true" : "false"));
            
                // 根据 stateName 判断属于什么类型的数据
                if (typeByteStateNames.find(stateName) != typeByteStateNames.end()) {
                    auto stateMetaInfo = value["stateMetaInfo"];
                    auto name = stateMetaInfo["name"].get<std::string>();
                    auto internalList = value["internalList"];
                    auto listState = std::make_shared<PartitionableListState<std::vector<uint8_t>>>(metaInfo);

                    int itemCount = 0;
                    for (const auto& item : internalList) {
                        std::vector<uint8_t> decodedData = Base64_decode(item.get<std::string>());
                        listState->add(decodedData);
                        itemCount++;
                    }
                    INFO_RELEASE("savepoint: OperatorStateRestoreOperation::convertResult stateName: " + name
                        + ", restored itemCount: " + std::to_string(itemCount));
                    registeredOperatorStates_->emplace(name, listState);
                    continue;
                }
                INFO_RELEASE("savepoint: OperatorStateRestoreOperation::convertResult stateName: " + stateName
                    + " NOT in typeByteStateNames, skip");
                // 更多类型进行判断
            } else {
                INFO_RELEASE("savepoint: OperatorStateRestoreOperation::convertResult snapshot name: " + snapshot.getName()
                    + " NOT found in parsed cppResult");
            }
        }
    }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates_;
    std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;

    inline static std::set<std::string> typeByteStateNames = {
        "SourceReaderState",
        "writer_raw_states",
        "streaming_committer_raw_states"
    };
};

#endif // OMNISTREAM_OPERATORSTATERESTOREOPERATION_H
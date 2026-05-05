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

#include <nlohmann/json.hpp>
#include <unordered_map>
#include <vector>
#include "PartitionableListState.h"
// #include "RegisteredBroadcastStateBackendMetaInfo.h"
#include "state/bridge/OmniTaskBridge.h"
// #include "OperatorStateHandle.h"
#include "OperatorStreamStateHandle.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"
#include "RegisteredOperatorStateBackendMetaInfo.h"

class OperatorStateRestoreOperation {
public:
    OperatorStateRestoreOperation(
        std::unordered_map<std::string, std::shared_ptr<PartitionableListState<>>>& registeredOperatorStates,
        // 暂时不涉及广播状态
        // std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<K, V>>* registeredBroadcastStates,
        const std::vector<std::shared_ptr<OperatorStateHandle>>& stateHandles,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : registeredOperatorStates_(registeredOperatorStates),
          stateHandles_(stateHandles),
          omniTaskBridge_(omniTaskBridge) {}

    ~OperatorStateRestoreOperation() = default;
    
    void restore() 
    {
        if (stateHandles_.empty()) {
            return;
        }
        for (auto& stateHandle : stateHandles_) {
            auto streamStateHandle = std::dynamic_pointer_cast<OperatorStreamStateHandle>(stateHandle);
            if (streamStateHandle) {
                auto json = TaskStateSnapshotSerializer::parseOperatorStreamStateHandle(streamStateHandle);

                auto stateMetaInfoSnapshots = omniTaskBridge_->readOperatorMetaData(to_string(json));
                auto metaInfo = std::make_shared<RegisteredOperatorStateBackendMetaInfo>(stateMetaInfoSnapshots[0]);

                auto cppResult = omniTaskBridge_->restoreOperatorStreamState(to_string(json));
                convertResult(cppResult, metaInfo);
            }
        }
    }
    
    void convertResult(const std::string& cppResult,
        std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> metaDataInfo)
    {
        nlohmann::json parsed = nlohmann::json::parse(cppResult);
        
        std::string stateName;
        nlohmann::json value;
        
        if (parsed.contains("writer_raw_states")) {
            stateName = "writer_raw_states";
        } else if (parsed.contains("SourceReaderState")) {
            stateName = "SourceReaderState";
        } else {
            throw std::runtime_error("Unknown state name");
        }
        value = parsed[stateName];

        auto stateMetaInfo = value["stateMetaInfo"];
        auto name = stateMetaInfo["name"].get<std::string>();
        auto internalList = value["internalList"];
        auto listState = std::make_shared<PartitionableListState<>>(metaDataInfo);

        for (const auto& item : internalList) {
            std::vector<uint8_t> decodedData = Base64_decode(item.get<std::string>());
            listState->add(decodedData);
        }
        registeredOperatorStates_.emplace(name, listState);
    }

   private:
    std::unordered_map<std::string, std::shared_ptr<PartitionableListState<>>>& registeredOperatorStates_;
    //std::unordered_map<std::string, std::shared_ptr<BackendWritableBroadcastState<K, V>>* registeredBroadcastStates_;
    std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
};

#endif // OMNISTREAM_OPERATORSTATERESTOREOPERATION_H
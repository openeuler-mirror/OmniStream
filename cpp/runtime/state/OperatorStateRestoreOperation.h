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
#include <memory>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include "PartitionableListState.h"
#include "RegisteredBroadcastStateBackendMetaInfo.h"
#include "state/bridge/OmniTaskBridge.h"
#include "OperatorStreamStateHandle.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"
#include "RegisteredOperatorStateBackendMetaInfo.h"
#include "core/memory/DataInputDeserializer.h"
#include "state/memory/ByteStreamStateHandle.h"

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
        if (stateHandles_.empty()) {
            return;
        }
        for (auto& stateHandle : stateHandles_) {
            if (stateHandle == nullptr) {
                continue;
            }
            auto streamStateHandle = std::dynamic_pointer_cast<OperatorStreamStateHandle>(stateHandle);
            if (streamStateHandle) {
                auto stateNameToPartitionOffsets = stateHandle->getStateNameToPartitionOffsets();
                auto delegate = stateHandle->getDelegateStateHandle();
                auto json = TaskStateSnapshotSerializer::parseOperatorStreamStateHandle(streamStateHandle);
                std::string handleJson = to_string(json);
                auto stateMetaInfoSnapshots = omniTaskBridge_->readOperatorMetaData(handleJson);
                
                for (auto& snapshot : stateMetaInfoSnapshots) {
                    const std::string& stateName = snapshot.getName();
                    if (stateNameToPartitionOffsets.find(stateName) == stateNameToPartitionOffsets.end()) {
                        continue;
                    }
                    if (!isSupportedRestoredState(stateName)) {
                        continue;
                    }
                    auto metaInfo = std::make_shared<RegisteredOperatorStateBackendMetaInfo>(snapshot);
                    auto byteDelegate = std::dynamic_pointer_cast<ByteStreamStateHandle>(delegate);
                    if (byteDelegate != nullptr) {
                        deserializeOperatorStateValues(metaInfo, byteDelegate, stateNameToPartitionOffsets);
                    }
                }
            }
        }
    }
    
    void deserializeOperatorStateValues(
        std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> metaInfo,
        std::shared_ptr<ByteStreamStateHandle> stateHandle,
        std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo> stateNameToPartitionOffsets) 
    {
        DataInputDeserializer in(stateHandle->GetData().data(), stateHandle->GetStateSize(), 0);
        TypeSerializer *serializer = metaInfo->getStateSerializer();
        if (serializer == nullptr) {
            return;
        }
        for (auto& entry : stateNameToPartitionOffsets) {
            auto name = entry.first;
            if (name != metaInfo->getName()) {
                continue;
            }

            auto offsets = entry.second.getOffsets();
            // 根据 stateName 判断属于什么类型的数据
            if (typeByteStateNames.find(name) != typeByteStateNames.end()) {
                auto listState = getOrCreateOperatorListState<std::vector<uint8_t>>(name, metaInfo);
                for (auto& offset : offsets) {
                    in.setPosition(offset);
                    std::unique_ptr<std::vector<uint8_t>> value(
                        static_cast<std::vector<uint8_t>*>(serializer->deserialize(in)));
                    listState->add(*value);
                }
            }

            if (typeLongStateNames.find(name) != typeLongStateNames.end()) {
                auto listState = getOrCreateOperatorListState<long>(name, metaInfo);
                for (auto& offset : offsets) {
                    in.setPosition(offset);
                    std::unique_ptr<long> value(static_cast<long*>(serializer->deserialize(in)));
                    listState->add(*value);
                }
            }
        }
    }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates_;
    std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;

    static bool isSupportedRestoredState(const std::string& stateName)
    {
        return typeByteStateNames.find(stateName) != typeByteStateNames.end()
            || typeLongStateNames.find(stateName) != typeLongStateNames.end();
    }

    template <typename T>
    std::shared_ptr<PartitionableListState<T>> getOrCreateOperatorListState(
        const std::string& stateName,
        const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& metaInfo)
    {
        auto existing = registeredOperatorStates_->find(stateName);
        if (existing != registeredOperatorStates_->end()) {
            auto listState = std::dynamic_pointer_cast<PartitionableListState<T>>(existing->second);
            if (listState == nullptr) {
                throw std::runtime_error("Restored operator state type mismatch for state: " + stateName);
            }
            listState->setStateMetaInfo(metaInfo);
            return listState;
        }

        auto listState = std::make_shared<PartitionableListState<T>>(metaInfo);
        registeredOperatorStates_->emplace(stateName, listState);
        return listState;
    }

    inline static std::set<std::string> typeByteStateNames = {
        "SourceReaderState",
        "writer_raw_states",
        "streaming_committer_raw_states"
    };

    inline static std::set<std::string> typeLongStateNames = {
        "watermark",
        "elements-count-state"
    };
};

#endif // OMNISTREAM_OPERATORSTATERESTOREOPERATION_H

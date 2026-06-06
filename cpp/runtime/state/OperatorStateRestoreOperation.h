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
#include <sstream>
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
            INFO_RELEASE("[OS-operator-state] restore skipped, handleCount=0");
            return;
        }
        INFO_RELEASE("[OS-operator-state] restore start, handleCount=" << stateHandles_.size());
        for (auto& stateHandle : stateHandles_) {
            if (stateHandle == nullptr) {
                INFO_RELEASE("Error:[OS-operator-state] skip null operator state handle");
                continue;
            }
            auto streamStateHandle = std::dynamic_pointer_cast<OperatorStreamStateHandle>(stateHandle);
            if (streamStateHandle) {
                auto stateNameToPartitionOffsets = stateHandle->getStateNameToPartitionOffsets();
                auto delegate = stateHandle->getDelegateStateHandle();
                INFO_RELEASE("[OS-operator-state] restore handle, stateCount="
                    << stateNameToPartitionOffsets.size()
                    << ", states=" << StateNamesToString(stateNameToPartitionOffsets)
                    << ", delegateType=" << DelegateType(delegate)
                    << ", delegateSize=" << (delegate ? delegate->GetStateSize() : -1));
                auto json = TaskStateSnapshotSerializer::parseOperatorStreamStateHandle(streamStateHandle);
                std::string handleJson = to_string(json);
                INFO_RELEASE("[OS-operator-state] read metadata begin, handleJsonBytes=" << handleJson.size());
                auto stateMetaInfoSnapshots = omniTaskBridge_->readOperatorMetaData(handleJson);
                INFO_RELEASE("[OS-operator-state] read metadata done, snapshotCount=" << stateMetaInfoSnapshots.size());
                if (stateMetaInfoSnapshots.empty() && !stateNameToPartitionOffsets.empty()) {
                    INFO_RELEASE("Error:[OS-operator-state] operator metadata is empty, states="
                        << StateNamesToString(stateNameToPartitionOffsets));
                }
                
                for (auto& snapshot : stateMetaInfoSnapshots) {
                    const std::string& stateName = snapshot.getName();
                    if (stateNameToPartitionOffsets.find(stateName) == stateNameToPartitionOffsets.end()) {
                        continue;
                    }
                    if (!isSupportedRestoredState(stateName)) {
                        INFO_RELEASE("[OS-operator-state] skip unsupported restored operator state, state="
                            << stateName << ", offsets="
                            << stateNameToPartitionOffsets[stateName].getOffsets().size());
                        continue;
                    }
                    auto metaInfo = std::make_shared<RegisteredOperatorStateBackendMetaInfo>(snapshot);
                    auto byteDelegate = std::dynamic_pointer_cast<ByteStreamStateHandle>(delegate);
                    if (byteDelegate != nullptr) {
                        deserializeOperatorStateValues(metaInfo, byteDelegate, stateNameToPartitionOffsets);
                    } else {
                        INFO_RELEASE("[OS-operator-state] skip value restore for non-memory delegate, state="
                            << metaInfo->getName() << ", delegateType=" << DelegateType(delegate));
                    }
                }
            } else {
                INFO_RELEASE("Error:[OS-operator-state] unsupported operator state handle type");
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
            INFO_RELEASE("Error:[OS-operator-state] restored operator state has no serializer, state="
                << metaInfo->getName());
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
                auto listState = std::make_shared<PartitionableListState<std::vector<uint8_t>>>(metaInfo);
                for (auto& offset : offsets) {
                    in.setPosition(offset);
                    listState->add(
                        *static_cast<std::vector<uint8_t>*>(serializer->deserialize(in))
                    );
                }
                registeredOperatorStates_->emplace(name, listState);
            }

            if (typeLongStateNames.find(name) != typeLongStateNames.end()) {
                auto listState = std::make_shared<PartitionableListState<long>>(metaInfo);
                for (auto& offset : offsets) {
                    in.setPosition(offset);
                    listState->add(*static_cast<long*>(serializer->deserialize(in)));
                }
                registeredOperatorStates_->emplace(name, listState);
            }
        }
    }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates_;
    std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;

    static std::string StateNamesToString(
        const std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo>& stateNameToPartitionOffsets)
    {
        std::ostringstream oss;
        bool first = true;
        for (const auto& entry : stateNameToPartitionOffsets) {
            if (!first) {
                oss << ",";
            }
            first = false;
            oss << entry.first << ":" << entry.second.getOffsets().size();
        }
        return oss.str();
    }

    static std::string DelegateType(const std::shared_ptr<StreamStateHandle>& delegate)
    {
        if (delegate == nullptr) {
            return "null";
        }
        if (std::dynamic_pointer_cast<ByteStreamStateHandle>(delegate)) {
            return "ByteStreamStateHandle";
        }
        if (std::dynamic_pointer_cast<RelativeFileStateHandle>(delegate)) {
            return "RelativeFileStateHandle";
        }
        if (std::dynamic_pointer_cast<FileStateHandle>(delegate)) {
            return "FileStateHandle";
        }
        if (std::dynamic_pointer_cast<PlaceholderStreamStateHandle>(delegate)) {
            return "PlaceholderStreamStateHandle";
        }
        return "unknown";
    }

    static bool isSupportedRestoredState(const std::string& stateName)
    {
        return typeByteStateNames.find(stateName) != typeByteStateNames.end()
            || typeLongStateNames.find(stateName) != typeLongStateNames.end();
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

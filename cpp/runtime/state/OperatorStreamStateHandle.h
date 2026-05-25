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

#ifndef OMNISTREAM_OPERATORSTREAMSTATEHANDLE_H
#define OMNISTREAM_OPERATORSTREAMSTATEHANDLE_H

#include <typeinfo>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <algorithm>
#include <cstdint>
#include <memory>

#include <nlohmann/json.hpp>
#include "core/include/common.h"
#include "core/fs/FSDataInputStream.h"
#include "runtime/state/StreamStateHandleFactory.h"

#include "OperatorStateHandle.h"
#include "StreamStateHandle.h"
#include "PhysicalStateHandleID.h"

class OperatorStreamStateHandle : public OperatorStateHandle {
public:
    OperatorStreamStateHandle(
        std::unordered_map<std::string, StateMetaInfo>& stateNameToPartitionOffsets_,
        std::shared_ptr<StreamStateHandle> delegateStateHandle_)
        : stateNameToPartitionOffsets(stateNameToPartitionOffsets_),
          delegateStateHandle(delegateStateHandle_) {}

    explicit OperatorStreamStateHandle(const nlohmann::json &description) {
        delegateStateHandle = StreamStateHandleFactory::from_json(description["delegateStateHandle"]);
        const nlohmann::json &offsetsJson = description["stateNameToPartitionOffsets"];
        for (auto &el: offsetsJson.items()) {
            if (el.key() == "@class") {
                continue;
            }
            std::string stateName = el.key();
            StateMetaInfo stateMetaInfo = parseStateMetaInfo(el.value());
            stateNameToPartitionOffsets[stateName] = stateMetaInfo;
        }
    }

    void DiscardState() override {
        delegateStateHandle->DiscardState();
    }

    long GetStateSize() const override {
        return delegateStateHandle->GetStateSize();
    }

    PhysicalStateHandleID getStreamStateHandleID() const {
        return delegateStateHandle->GetStreamStateHandleID();
    }

    std::shared_ptr<FSDataInputStream> OpenInputStream() const override {
        return delegateStateHandle->OpenInputStream();
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override {
        return delegateStateHandle->AsBytesIfInMemory();
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override {
        return delegateStateHandle->GetStreamStateHandleID();
    }

    std::unordered_map<std::string, StateMetaInfo> getStateNameToPartitionOffsets() const override {
        return stateNameToPartitionOffsets;
    }

    std::shared_ptr<StreamStateHandle> getDelegateStateHandle() const override {
        return delegateStateHandle;
    }

    bool operator==(const StreamStateHandle& other_) const override {
        const auto* other = dynamic_cast<const OperatorStreamStateHandle*>(&other_);
        return other
            && stateNameToPartitionOffsets == other->stateNameToPartitionOffsets
            && delegateStateHandle == other->delegateStateHandle;
    }

    //将stateNameToPartitionOffsets转换为json
    nlohmann::json toJson() const  {
        nlohmann::json result ;
        for (const auto& entry : stateNameToPartitionOffsets) {
            const auto& stateName = entry.first;
            const auto& stateMetaInfo = entry.second;
             nlohmann::json stateMetaInfoJson ;
             stateMetaInfoJson["offsets"] = stateMetaInfo.getOffsets();
             // 将Mode枚举转换为字符串
             OperatorStateHandle::Mode mode = stateMetaInfo.getDistributionMode();
             std::string modeStr;
             switch (mode) {
                 case OperatorStateHandle::Mode::SPLIT_DISTRIBUTE:
                     modeStr = "SPLIT_DISTRIBUTE";
                     break;
                 case OperatorStateHandle::Mode::UNION:
                     modeStr = "UNION";
                     break;
                 case OperatorStateHandle::Mode::BROADCAST:
                     modeStr = "BROADCAST";
                     break;
                 default:
                     modeStr = "UNKNOWN";
                     break;
             }
             stateMetaInfoJson["distributionMode"] = modeStr;
             result[stateName] = stateMetaInfoJson;
        }
        INFO_RELEASE("savepoint:  OperatorStreamStateHandle ToJson "<<result.dump());
        return result;
    }


    std::string ToString() const override { 
        nlohmann::json result;
        result["stateHandleName"] ="OperatorStreamStateHandle";
        result["streamStateHandle"] = nlohmann::json::parse(getDelegateStateHandle()->ToString());
        result["stateHandleID"] = nlohmann::json::parse(getStreamStateHandleID().ToString());
        result["stateNameToPartitionOffsets"] = toJson();
        INFO_RELEASE("savepoint:  OperatorStreamStateHandle ToString "<<result.dump());
        return result.dump(); 
    }

private:
    std::unordered_map<std::string, StateMetaInfo> stateNameToPartitionOffsets;
    std::shared_ptr<StreamStateHandle> delegateStateHandle;

    StateMetaInfo parseStateMetaInfo(const nlohmann::json &j) {
        auto mode = OperatorStateHandle::StrToMode(j["distributionMode"].get<std::string>());
        auto offset = j["offsets"].get<std::vector<long>>();
        return StateMetaInfo(offset, mode);
    }
};

#endif //OMNISTREAM_OPERATORSTREAMSTATEHANDLE_H

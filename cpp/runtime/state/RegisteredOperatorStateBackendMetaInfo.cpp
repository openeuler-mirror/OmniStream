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

#include "RegisteredOperatorStateBackendMetaInfo.h"

RegisteredOperatorStateBackendMetaInfo::RegisteredOperatorStateBackendMetaInfo(
    const std::string& name, OperatorStateHandle::Mode assignmentMode, TypeSerializer* stateSerializer)
    : RegisteredStateMetaInfoBase(name), assignmentMode_(assignmentMode), stateSerializer_(stateSerializer) {
    INFO_RELEASE("h30082497 RegisteredOperatorStateBackendMetaInfo 1");
}

RegisteredOperatorStateBackendMetaInfo::RegisteredOperatorStateBackendMetaInfo(const StateMetaInfoSnapshot& snapshot)
    : RegisteredStateMetaInfoBase(snapshot.getName()) {
    INFO_RELEASE("h30082497 RegisteredOperatorStateBackendMetaInfo 2 1 ");

    auto assignmentModeStr = snapshot.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::OPERATOR_STATE_DISTRIBUTION_MODE);
    assignmentMode_ = OperatorStateHandle::StrToMode(assignmentModeStr);

    std::string stateSerializerKey = StateMetaInfoSnapshot::commonSerializerKeyToString(StateMetaInfoSnapshot::CommonSerializerKeys::VALUE_SERIALIZER);

    stateSerializer_ = snapshot.getTypeSerializer(stateSerializerKey);
    INFO_RELEASE("h30082497 RegisteredOperatorStateBackendMetaInfo 2 end ");
}

std::shared_ptr<StateMetaInfoSnapshot> RegisteredOperatorStateBackendMetaInfo::computeSnapshot() {
    INFO_RELEASE("h30082497 RegisteredOperatorStateBackendMetaInfo::computeSnapshot 1");
    std::unordered_map<std::string, std::string> optionsMap;
    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;

    std::string optionKey = StateMetaInfoSnapshot::commonOptionsKeyToString(StateMetaInfoSnapshot::CommonOptionsKeys::OPERATOR_STATE_DISTRIBUTION_MODE);
    optionsMap.emplace(optionKey, std::to_string((int) getAssignmentMode()));

    serializerMap.emplace("stateSerializer", getStateSerializer());
    INFO_RELEASE("h30082497 RegisteredOperatorStateBackendMetaInfo::computeSnapshot name ======== " + name);
    INFO_RELEASE("h30082497 RegisteredOperatorStateBackendMetaInfo::computeSnapshot name ======== " + getStateSerializer()->toJson());
    INFO_RELEASE("h30082497 RegisteredOperatorStateBackendMetaInfo::computeSnapshot end");

    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::OPERATOR,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}
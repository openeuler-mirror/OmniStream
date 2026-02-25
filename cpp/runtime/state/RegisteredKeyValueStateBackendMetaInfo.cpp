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
#include "RegisteredKeyValueStateBackendMetaInfo.h"

std::shared_ptr<StateMetaInfoSnapshot> RegisteredKeyValueStateBackendMetaInfo::computeSnapshot()
{
    std::unordered_map<std::string, std::string> optionsMap;
    optionsMap[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = std::to_string((int) stateType);

    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    serializerMap.emplace("stateSerializer", getStateSerializer());
    serializerMap.emplace("namespaceSerializer", getNamespaceSerializer());

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}

RegisteredKeyValueStateBackendMetaInfo::RegisteredKeyValueStateBackendMetaInfo(const StateMetaInfoSnapshot &snapshot)
    : RegisteredStateMetaInfoBase(snapshot.getName())
{
    auto stateTypeString = snapshot.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
    stateType = StateDescriptor::StringToType(stateTypeString);
    auto namespaceSerializerKey = StateMetaInfoSnapshot::CommonSerializerKeys::NAMESPACE_SERIALIZER;
    auto valueSerializerKey = StateMetaInfoSnapshot::CommonSerializerKeys::VALUE_SERIALIZER;

    namespaceSerializer =
        snapshot.getTypeSerializer(StateMetaInfoSnapshot::commonSerializerKeyToString(namespaceSerializerKey));
    stateSerializer =
        snapshot.getTypeSerializer(StateMetaInfoSnapshot::commonSerializerKeyToString(valueSerializerKey));
}

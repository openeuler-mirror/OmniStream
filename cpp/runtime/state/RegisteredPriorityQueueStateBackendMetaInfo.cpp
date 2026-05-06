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

#include "RegisteredPriorityQueueStateBackendMetaInfo.h"

RegisteredPriorityQueueStateBackendMetaInfo::RegisteredPriorityQueueStateBackendMetaInfo(const StateMetaInfoSnapshot &snapshot)
    : RegisteredStateMetaInfoBase(snapshot.getName()) {
    auto valueSerializerKey = StateMetaInfoSnapshot::CommonSerializerKeys::VALUE_SERIALIZER;
    elementSerializer = snapshot.getTypeSerializer(StateMetaInfoSnapshot::commonSerializerKeyToString(valueSerializerKey));
}

std::shared_ptr<StateMetaInfoSnapshot> RegisteredPriorityQueueStateBackendMetaInfo::computeSnapshot() {
    std::unordered_map<std::string, std::string> optionsMap;
    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    serializerMap.emplace("stateSerializer", getElementSerializer());

    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}
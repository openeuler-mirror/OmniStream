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
#include "TaskStateSnapshotDeserializer.h"

std::shared_ptr<TaskStateSnapshot> TaskStateSnapshotDeserializer::Deserialize(const std::string &jsonString)
{
    const json j = json::parse(jsonString);
    auto snapshot = std::make_shared<TaskStateSnapshot>();
    if (j.contains("taskDeployedAsFinished")) {
        // Assuming a setter exists on your TaskStateSnapshot class
        snapshot->SetIsTaskDeployedAsFinished(j.at("taskDeployedAsFinished").get<bool>());
    }
    if (j.contains("taskFinished")) {
        // Assuming a setter exists on your TaskStateSnapshot class
        snapshot->SetIsTaskFinished(j.at("taskFinished").get<bool>());
    }
    const json &subtask_states_json = j.at("subtaskStatesByOperatorID");
    for (auto &el: subtask_states_json.items()) {
        if (el.key() == "@class") {
            continue;
        }
        OperatorID id = HexStringToOperatorId<OperatorID>(el.key());
        snapshot->PutSubtaskStateByOperatorID(id, ParseOperatorSubtaskState(el.value()));
    }

    return snapshot;
}

std::shared_ptr<KeyedStateHandle> TaskStateSnapshotDeserializer::ParseKeyedStateHandle(const json &j)
{
    if (!j.contains("@class")) {
        throw std::runtime_error("State handle JSON is missing the '@class' field.");
    }
    const std::string className = j.at("@class").get<std::string>();
    // Dispatch to the correct parser based on the class name
    if (className.find("IncrementalRemoteKeyedStateHandle") != std::string::npos) {
        return ParseRemoteStateHandle(j);
    }
    if (className.find("DirectoryKeyedStateHandle") != std::string::npos) {
        return ParseDirectoryKeyedStateHandle(j);
    }
    if (className.find("IncrementalLocalKeyedStateHandle") != std::string::npos) {
        return ParseLocalStateHandle(j);
    }
    if (className.find("KeyGroupsSavepointStateHandle") != std::string::npos) {
        return ParseKeyGroupsSavepointStateHandle(j);
    }

    throw std::runtime_error("Unsupported or unknown KeyedStateHandle type: " + className);
}

std::shared_ptr<OperatorSubtaskState> TaskStateSnapshotDeserializer::ParseOperatorSubtaskState(const json &j)
{
    auto managedKeyedStateCol = ParseStateObjectCollection<KeyedStateHandle>(
        j.at("managedKeyedState"), &ParseKeyedStateHandle);

    StateObjectCollection<KeyedStateHandle> managedKeyedStateHandles;
    if (managedKeyedStateCol) {
        // This calls a constructor of StateObjectCollection
        managedKeyedStateHandles = StateObjectCollection<KeyedStateHandle>(managedKeyedStateCol->ToArray());
    }

    StateObjectCollection<OperatorStateHandle> managedOperatorState; // Empty lvalue
    StateObjectCollection<OperatorStateHandle> rawOperatorState;     // Empty lvalue
    StateObjectCollection<KeyedStateHandle> rawKeyedState;           // Empty lvalue
    StateObjectCollection<InputChannelStateHandle> inputChannelState; // Empty lvalue
    StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState; // Empty lvalue
    NoRescalingDescriptor noRescalingDescriptor;

    auto subtaskState = std::make_shared<OperatorSubtaskState>(
        managedOperatorState,
        rawOperatorState,
        managedKeyedStateHandles, // The only one with data
        rawKeyedState,
        inputChannelState,
        resultSubpartitionState,
        noRescalingDescriptor,
        noRescalingDescriptor
    );

    return subtaskState;
}

template<typename T>
std::shared_ptr<StateObjectCollection<T>> TaskStateSnapshotDeserializer::ParseStateObjectCollection(
    const json &j, std::shared_ptr<T> (*parser)(const json &))
{
    const int maxSize = 2;
    auto collection = std::make_shared<StateObjectCollection<T>>();
    if (j.is_array() && j.size() == maxSize && j.at(1).is_array()) {
        for (const auto &item_json: j.at(1)) {
            collection->Add(parser(item_json));
        }
    }
    return collection;
}

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

#include "TaskStateSnapshot.h"
// Static member initialization
const std::shared_ptr<TaskStateSnapshot> TaskStateSnapshot::finishedOnRestore = std::make_shared<TaskStateSnapshot>
    (std::unordered_map<OperatorID, std::shared_ptr<OperatorSubtaskState>>(), true, true);

/** Returns the subtask state for the given operator id (or null if not contained). */
std::shared_ptr<OperatorSubtaskState> TaskStateSnapshot::GetSubtaskStateByOperatorID(
    const OperatorID& operatorID) const
{
    auto it = subtaskStatesByOperatorID.find(operatorID);
    if (it != subtaskStatesByOperatorID.end()) {
        return it->second;
    }
    return nullptr;
}

/**
 * Maps the given operator id to the given subtask state. Returns the subtask state of a
 * previous mapping, if such a mapping existed or null otherwise.
 */
std::shared_ptr<OperatorSubtaskState> TaskStateSnapshot::PutSubtaskStateByOperatorID(
    const OperatorID& operatorID, std::shared_ptr<OperatorSubtaskState> state)
{
    auto it = subtaskStatesByOperatorID.find(operatorID);
    std::shared_ptr<OperatorSubtaskState> previousState = nullptr;

    if (it != subtaskStatesByOperatorID.end()) {
        previousState = it->second;
    }

    subtaskStatesByOperatorID[operatorID] = state;
    return previousState;
}

/** Returns the set of all mappings from operator id to the corresponding subtask state. */
std::set<std::pair<OperatorID, std::shared_ptr<OperatorSubtaskState>>> TaskStateSnapshot::GetSubtaskStateMappings()
    const
{
    std::set<std::pair<OperatorID, std::shared_ptr<OperatorSubtaskState>>> result;
    for (const auto& entry : subtaskStatesByOperatorID) {
        result.insert(entry);
    }
    return result;
}

/**
 * Returns true if at least one {@link OperatorSubtaskState} in subtaskStatesByOperatorID has
 * state.
 */
bool TaskStateSnapshot::HasState() const
{
    for (const auto& pair : subtaskStatesByOperatorID) {
        std::shared_ptr<OperatorSubtaskState> operatorSubtaskState = pair.second;
        if (operatorSubtaskState != nullptr && operatorSubtaskState->HasState()) {
            return true;
        }
    }
    return isTaskDeployedAsFinished;
}

/**
 * Returns the input channel mapping for rescaling with in-flight data or {@link
 * InflightDataRescalingDescriptor#noRescale}.
 */
InflightDataRescalingDescriptor TaskStateSnapshot::GetInputRescalingDescriptor() const
{
    // TTODO
    return InflightDataRescalingDescriptor::noRescale;
}

/**
 * Returns the output channel mapping for rescaling with in-flight data or {@link
 * InflightDataRescalingDescriptor#noRescale}.
 */
InflightDataRescalingDescriptor TaskStateSnapshot::GetOutputRescalingDescriptor() const
{
    // TTODO
    return InflightDataRescalingDescriptor::noRescale;
}

void TaskStateSnapshot::DiscardState()
{
    std::vector<std::shared_ptr<OperatorSubtaskState>> values;
    for (const auto& pair : subtaskStatesByOperatorID) {
        values.push_back(pair.second);
    }
    for (auto value : values) {
        value->DiscardState();
    }
}

long TaskStateSnapshot::GetStateSize() const
{
    long size = 0L;
    for (const auto& pair : subtaskStatesByOperatorID) {
        std::shared_ptr<OperatorSubtaskState> subtaskState = pair.second;
        if (subtaskState != nullptr) {
            size += subtaskState->GetStateSize();
        }
    }
    return size;
}

void TaskStateSnapshot::RegisterSharedStates(SharedStateRegistry& stateRegistry, long checkpointID)
{
    for (const auto& pair : subtaskStatesByOperatorID) {
        std::shared_ptr<OperatorSubtaskState> operatorSubtaskState = pair.second;
        if (operatorSubtaskState != nullptr) {
            operatorSubtaskState->RegisterSharedStates(stateRegistry, checkpointID);
        }
    }
}

size_t TaskStateSnapshot::HashCode() const
{
    // not used
    return 0;
}

std::string TaskStateSnapshot::ToString() const
{
    nlohmann::json j;
    j["stateHandleName"] = "TaskStateSnapshot";
    j["isTaskDeployedAsFinished"] = isTaskDeployedAsFinished;
    j["isTaskFinished"] = isTaskFinished;
    j["stateSize"] = GetStateSize();
    nlohmann::json subtask_states_map;
    for (const auto& pair : subtaskStatesByOperatorID) {
        // Use the hex string of the OperatorID as the key.
        std::string operatorIdHex = pair.first.toString();

        if (pair.second != nullptr) {
            subtask_states_map[operatorIdHex] = nlohmann::json::parse(pair.second->ToString());
        } else {
            subtask_states_map[operatorIdHex] = nullptr;
        }
    }
    j["subtaskStatesByOperatorID"] = subtask_states_map;
    return j.dump();
}

/** Returns the only valid mapping as ensured by {@link StateAssignmentOperation}. */
InflightDataRescalingDescriptor TaskStateSnapshot::GetMapping(std::function<InflightDataRescalingDescriptor
    (const std::shared_ptr<OperatorSubtaskState>&)> mappingExtractor) const
{
    std::vector<InflightDataRescalingDescriptor> mappings;

    for (const auto& pair : subtaskStatesByOperatorID) {
        if (pair.second != nullptr) {
            InflightDataRescalingDescriptor mapping = mappingExtractor(pair.second);
            if (!(mapping == InflightDataRescalingDescriptor::noRescale)) {
                mappings.push_back(mapping);
            }
        }
    }
    if (mappings.size() == 1) {
        return mappings[0];
    } else if (mappings.empty()) {
        return InflightDataRescalingDescriptor::noRescale;
    } else {
        throw std::runtime_error("getMapping gets more than 1 result!");
    }
}

long TaskStateSnapshot::GetCheckpointedSize()
{
    long size = 0L;

    for (auto it = subtaskStatesByOperatorID.begin(); it != subtaskStatesByOperatorID.end(); it++) {
        auto subtaskState = it->second;
        if (subtaskState != nullptr) {
            size += subtaskState->GetCheckpointedSize();
        }
    }
    return size;
}

void TaskStateSnapshot::SetIsTaskFinished(bool hasTaskFinished)
{
    this->isTaskFinished = hasTaskFinished;
}

void TaskStateSnapshot::SetIsTaskDeployedAsFinished(bool hasTaskDeployedAsFinished)
{
    this->isTaskDeployedAsFinished = hasTaskDeployedAsFinished;
}
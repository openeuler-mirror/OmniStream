/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef OMNISTREAM_INCREMENTALREMOTEKEYEDSTATEHANDLE
#define OMNISTREAM_INCREMENTALREMOTEKEYEDSTATEHANDLE

#include "IncrementalKeyedStateHandle.h"
#include "StreamStateHandle.h"
#include "KeyedStateHandle.h"
#include "KeyGroupRange.h"
#include "StateHandleID.h"
#include <vector>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include "StreamStateHandleFactory.h"

class IncrementalRemoteKeyedStateHandle : public IncrementalKeyedStateHandle {
public:
    static const long unkownCheckpointSize = -1;

    IncrementalRemoteKeyedStateHandle(
        UUID backendIdentifier,
        KeyGroupRange keyGroupRange,
        int64_t checkpointId,
        std::vector<HandleAndLocalPath> sharedState,
        std::vector<HandleAndLocalPath> privateState,
        std::shared_ptr<StreamStateHandle> metaStateHandle)
        : IncrementalRemoteKeyedStateHandle(
              backendIdentifier,
              keyGroupRange,
              checkpointId,
              sharedState,
              privateState,
              metaStateHandle,
              unkownCheckpointSize,
              StateHandleID(UUID::randomUUID().ToString())) {};

    IncrementalRemoteKeyedStateHandle(
        UUID backendIdentifier,
        KeyGroupRange keyGroupRange,
        int64_t checkpointId,
        std::vector<HandleAndLocalPath> sharedState,
        std::vector<HandleAndLocalPath> privateState,
        std::shared_ptr<StreamStateHandle> metaStateHandle,
        long persistedSizeOfThisCheckpoint)
        : IncrementalRemoteKeyedStateHandle(
              backendIdentifier,
              keyGroupRange,
              checkpointId,
              sharedState,
              privateState,
              metaStateHandle,
              persistedSizeOfThisCheckpoint,
              StateHandleID(UUID::randomUUID().ToString())) {};

    IncrementalRemoteKeyedStateHandle(
        UUID backendIdentifier,
        KeyGroupRange keyGroupRange,
        int64_t checkpointId,
        std::vector<HandleAndLocalPath> sharedState,
        std::vector<HandleAndLocalPath> privateState,
        std::shared_ptr<StreamStateHandle> metaStateHandle,
        long persistedSizeOfThisCheckpoint,
        StateHandleID stateHandleId);

    ~IncrementalRemoteKeyedStateHandle() override {};

    static IncrementalRemoteKeyedStateHandle *Restore(
        UUID backendIdentifier,
        KeyGroupRange keyGroupRange,
        int64_t checkpointId,
        std::vector<HandleAndLocalPath> sharedState,
        std::vector<HandleAndLocalPath> privateState,
        std::shared_ptr<StreamStateHandle> metaStateHandle,
        long persistedSizeOfThisCheckpoint,
        StateHandleID stateHandleId);

    explicit IncrementalRemoteKeyedStateHandle(const nlohmann::json &description)
        : stateHandleId_(StateHandleID::randomStateHandleId())
    {
        backendIdentifier_ = UUID::FromString(description["backendIdentifier"].get<std::string>());
        keyGroupRange_ = KeyGroupRange(
            description.at("keyGroupRange").at("startKeyGroup").get<int>(),
            description.at("keyGroupRange").at("endKeyGroup").get<int>());
        checkpointId_ = description.at("checkpointId").get<int64_t>();

        sharedState_ = ParseHandleAndLocalPathList(description, "sharedState");
        privateState_ = ParseHandleAndLocalPathList(description, "privateState");
        metaStateHandle_ = ParseMetaStateHandle(description);

        persistedSizeOfThisCheckpoint_ = ParseSizeField(description);
        if (persistedSizeOfThisCheckpoint_ == unkownCheckpointSize) {
            persistedSizeOfThisCheckpoint_ = GetStateSize();
        }
        stateHandleId_ = ParseStateHandleId(description);
    };

    long GetStateSize() const override;
    KeyGroupRange GetKeyGroupRange() const override;
    int64_t GetCheckpointId() const override;
    const std::vector<HandleAndLocalPath>& GetSharedState() const;
    std::vector<HandleAndLocalPath> GetPrivateState() const;
    void RegisterSharedStates(SharedStateRegistry& stateRegistry, int64_t checkpointId) override;
    long GetCheckpointedSize() override;
    StateHandleID GetStateHandleId() const override;
    std::shared_ptr<KeyedStateHandle> GetIntersection(const KeyGroupRange &keyGroupRange) const override;
    const UUID &GetBackendIdentifier() const override;
    std::shared_ptr<StreamStateHandle> GetMetaDataStateHandle() const;
    const std::vector<HandleAndLocalPath>& GetSharedStateHandles() const override;

    bool operator==(const IncrementalRemoteKeyedStateHandle& other) const;
    void DiscardState() override;
    std::shared_ptr<CheckpointBoundKeyedStateHandle> rebound(int64_t newCheckpointId) const;
    std::string ToString() const override;

private:
    static const nlohmann::json* UnwrapStateList(const nlohmann::json* listJson)
    {
        if (listJson == nullptr || listJson->is_null()) {
            return nullptr;
        }
        // Flink/Jackson often serializes StateObjectCollection as
        // ["org.apache.flink.runtime.checkpoint.StateObjectCollection", [ ... ]].
        if (listJson->is_array() && listJson->size() == 2
            && listJson->at(0).is_string() && listJson->at(1).is_array()) {
            return &listJson->at(1);
        }
        // Some OmniAdaptor paths serialize the collection as a plain object.
        if (listJson->is_object() && listJson->contains("stateObjects")
            && listJson->at("stateObjects").is_array()) {
            return &listJson->at("stateObjects");
        }
        return listJson;
    }

    static std::vector<HandleAndLocalPath> ParseHandleAndLocalPathList(
        const nlohmann::json& description, const std::string& fieldName)
    {
        std::vector<HandleAndLocalPath> result;
        if (!description.contains(fieldName) || description.at(fieldName).is_null()) {
            return result;
        }
        const nlohmann::json* stateList = UnwrapStateList(&description.at(fieldName));
        if (stateList == nullptr) {
            return result;
        }
        if (!stateList->is_array()) {
            throw std::runtime_error("IncrementalRemoteKeyedStateHandle field '" + fieldName + "' must be an array.");
        }
        for (const auto& item : *stateList) {
            if (!item.is_object() || !item.contains("handle") || item.at("handle").is_null()) {
                continue;
            }
            auto handle = StreamStateHandleFactory::from_json(item.at("handle"));
            if (handle == nullptr) {
                throw std::runtime_error("Unsupported stream state handle in IncrementalRemoteKeyedStateHandle " + fieldName);
            }
            std::string localPath = item.contains("localPath") ? item.at("localPath").get<std::string>() : "";
            if (localPath.empty()) {
                throw std::runtime_error("IncrementalRemoteKeyedStateHandle " + fieldName + " entry is missing localPath.");
            }
            result.emplace_back(HandleAndLocalPath::of(handle, localPath));
        }
        return result;
    }

    static std::shared_ptr<StreamStateHandle> ParseMetaStateHandle(const nlohmann::json& description)
    {
        if (description.contains("metaStateHandle") && !description.at("metaStateHandle").is_null()) {
            return StreamStateHandleFactory::from_json(description.at("metaStateHandle"));
        }
        if (description.contains("metaDataState") && !description.at("metaDataState").is_null()) {
            return StreamStateHandleFactory::from_json(description.at("metaDataState"));
        }
        throw std::runtime_error("IncrementalRemoteKeyedStateHandle missing metaStateHandle/metaDataState.");
    }

    static long ParseSizeField(const nlohmann::json& description)
    {
        if (description.contains("persistedSizeOfThisCheckpoint")) {
            return description.at("persistedSizeOfThisCheckpoint").get<long>();
        }
        if (description.contains("checkpointedSize")) {
            return description.at("checkpointedSize").get<long>();
        }
        if (description.contains("stateSize")) {
            return description.at("stateSize").get<long>();
        }
        return unkownCheckpointSize;
    }

    static StateHandleID ParseStateHandleId(const nlohmann::json& description)
    {
        if (description.contains("stateHandleId") && description.at("stateHandleId").is_object()
            && description.at("stateHandleId").contains("keyString")) {
            return StateHandleID(description.at("stateHandleId").at("keyString").get<std::string>());
        }
        if (description.contains("stateHandleID") && description.at("stateHandleID").is_object()
            && description.at("stateHandleID").contains("keyString")) {
            return StateHandleID(description.at("stateHandleID").at("keyString").get<std::string>());
        }
        return StateHandleID::randomStateHandleId();
    }

    static const nlohmann::json& GetHandleAndLocalPathItems(
        const nlohmann::json& description,
        const char* fieldName)
    {
        static const nlohmann::json emptyArray = nlohmann::json::array();
        if (!description.contains(fieldName) || description.at(fieldName).is_null()) {
            return emptyArray;
        }

        const auto& collection = description.at(fieldName);
        if (!collection.is_array()) {
            throw std::invalid_argument(std::string(fieldName) + " must be an array");
        }

        if (collection.size() == 2 && collection.at(0).is_string() && collection.at(1).is_array()) {
            return collection.at(1);
        }
        return collection;
    }

    UUID backendIdentifier_;
    KeyGroupRange keyGroupRange_;
    int64_t checkpointId_;
    std::vector<HandleAndLocalPath> sharedState_;
    std::vector<HandleAndLocalPath> privateState_;
    std::shared_ptr<StreamStateHandle> metaStateHandle_;
    long persistedSizeOfThisCheckpoint_;
    StateHandleID stateHandleId_;
};

#endif // OMNISTREAM_INCREMENTALREMOTEKEYEDSTATEHANDLE
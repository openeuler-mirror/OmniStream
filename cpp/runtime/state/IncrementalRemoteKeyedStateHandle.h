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
        : stateHandleId_("")
    {
        backendIdentifier_ = UUID::FromString(description["backendIdentifier"].get<std::string>());
        keyGroupRange_ = KeyGroupRange(
            description["keyGroupRange"]["startKeyGroup"].get<int>(),
            description["keyGroupRange"]["endKeyGroup"].get<int>());
        checkpointId_ = description["checkpointId"].get<int64_t>();
        for (const auto &item : description["sharedState"][1]) {
            if (item.contains("handle")) {
                auto handle = StreamStateHandleFactory::from_json(item["handle"]);
                auto localPath = item["localPath"].get<std::string>();
                sharedState_.emplace_back(HandleAndLocalPath::of(handle, localPath));
            }
        }
        for (const auto &item : description["privateState"][1]) {
            if (item.contains("handle")) {
                auto handle = StreamStateHandleFactory::from_json(item["handle"]);
                auto localPath = item["localPath"].get<std::string>();
                privateState_.emplace_back(HandleAndLocalPath::of(handle, localPath));
            }
        }
        metaStateHandle_ = StreamStateHandleFactory::from_json(description["metaStateHandle"]);
        persistedSizeOfThisCheckpoint_ = description["persistedSizeOfThisCheckpoint"].get<long>();
        stateHandleId_ = StateHandleID(description["stateHandleId"]["keyString"].get<std::string>());
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
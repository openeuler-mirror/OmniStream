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
#ifndef OMNISTREAM_DIRECTORYKEYEDSTATEHANDLE_H
#define OMNISTREAM_DIRECTORYKEYEDSTATEHANDLE_H

#include <memory>
#include <nlohmann/json.hpp>

#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/StateHandleID.h"
#include "runtime/state/DirectoryStateHandle.h"
#include "runtime/state/KeyGroupRange.h"

class DirectoryKeyedStateHandle : virtual public KeyedStateHandle {
public:
    DirectoryKeyedStateHandle(
        DirectoryStateHandle* directoryHandle,
        const KeyGroupRange& keyGroupRange)
        : directoryStateHandle_(directoryHandle),
          keyGroupRange_(keyGroupRange),
          stateHandleId_(StateHandleID::randomStateHandleId()) {}

    DirectoryKeyedStateHandle(const nlohmann::json& json)
        : directoryStateHandle_(nullptr),
          keyGroupRange_(KeyGroupRange(0, 0)),
          stateHandleId_(StateHandleID::randomStateHandleId())
    {
        if (!json.contains("directoryStateHandle")) {
            throw std::invalid_argument("DirectoryKeyedStateHandle 'directoryStateHandle' field missing");
        }
        if (!json.contains("keyGroupRange")) {
            throw std::invalid_argument("DirectoryKeyedStateHandle 'keyGroupRange' field missing");
        }

        directoryStateHandle_ = new DirectoryStateHandle(json["directoryStateHandle"]);
        keyGroupRange_ = KeyGroupRange(json["keyGroupRange"]["startKeyGroup"], json["keyGroupRange"]["endKeyGroup"]);
    }

    ~DirectoryKeyedStateHandle() = default;

    DirectoryStateHandle* getDirectoryStateHandle() const
    {
        return directoryStateHandle_;
    }

    KeyGroupRange GetKeyGroupRange() const override
    {
        return keyGroupRange_;
    }

    std::shared_ptr<KeyedStateHandle> GetIntersection(const KeyGroupRange& other) const override
    {
        KeyGroupRange* intersection = keyGroupRange_.getIntersection(other);
        if (intersection->getNumberOfKeyGroups() > 0) {
            return std::shared_ptr<KeyedStateHandle>(
                new DirectoryKeyedStateHandle(
                    new DirectoryStateHandle(*directoryStateHandle_),
                    *intersection
                )
            );
        }
        
        delete intersection;
        return nullptr;
    }

    StateHandleID GetStateHandleId() const override
    {
        return stateHandleId_;
    }

    long GetStateSize() const override
    {
        return directoryStateHandle_->GetStateSize();
    }

    long GetCheckpointedSize() override
    {
        return GetStateSize();
    }

    void RegisterSharedStates(SharedStateRegistry&, long) override {}

    void DiscardState() override
    {
        directoryStateHandle_->DiscardState();
    }

    std::string ToString() const override
    {
        nlohmann::json json;
        json["stateHandleName"] = "DirectoryKeyedStateHandle";
        json["directoryStateHandle"] = nlohmann::json::parse(directoryStateHandle_->ToString());
        json["keyGroupRange"] = nlohmann::json::parse(keyGroupRange_.ToString());
        return json.dump();
    }

    bool operator==(const DirectoryKeyedStateHandle& other) const
    {
        return *directoryStateHandle_ == *other.directoryStateHandle_ &&
               keyGroupRange_ == other.keyGroupRange_;
    }

private:
    DirectoryStateHandle* directoryStateHandle_;
    KeyGroupRange keyGroupRange_;
    StateHandleID stateHandleId_;
};

#endif // OMNISTREAM_DIRECTORYKEYEDSTATEHANDLE_H

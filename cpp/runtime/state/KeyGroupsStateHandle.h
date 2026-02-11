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
#ifndef FLINK_TNEL_KEYGROUPSSTATEHANDLE_H
#define FLINK_TNEL_KEYGROUPSSTATEHANDLE_H
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/PhysicalStateHandleID.h"
#include "runtime/state/StreamStateHandleFactory.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <nlohmann/json.hpp>

class KeyGroupsStateHandle : public StreamStateHandle, public KeyedStateHandle {
public:
    KeyGroupsStateHandle(const KeyGroupRangeOffsets& groupRangeOffsets,
                                                                std::shared_ptr<StreamStateHandle> streamStateHandle)
        : KeyGroupsStateHandle(groupRangeOffsets, std::move(streamStateHandle),
                                            StateHandleID(boost::uuids::to_string(boost::uuids::random_generator()())))
    {}

    KeyGroupsStateHandle(const KeyGroupRangeOffsets& groupRangeOffsets,
        std::shared_ptr<StreamStateHandle> streamStateHandle, const StateHandleID& stateHandleId)
        : keyGroupRangeOffsets_(groupRangeOffsets), stateHandle_(std::move(streamStateHandle)),
        stateHandleId_(stateHandleId)
    {}

    explicit KeyGroupsStateHandle(const nlohmann::json &description):
        keyGroupRangeOffsets_(description["keyGroupRange"]["startKeyGroup"].get<int>(),
            description["keyGroupRange"]["endKeyGroup"].get<int>(),
            description["groupRangeOffsets"]["offsets"].get<std::vector<int64_t>>()),
        stateHandle_(StreamStateHandleFactory::from_json(description["stateHandle"])),
        stateHandleId_(StateHandleID(description["stateHandleId"]["keyString"].get<std::string>()))
    {}

    ~KeyGroupsStateHandle() noexcept(true) override = default;

    std::unique_ptr<FSDataInputStream> OpenInputStream() const override {return stateHandle_->OpenInputStream();}

    std::shared_ptr<KeyedStateHandle> GetIntersection(
        const KeyGroupRange& keyGroupRange) const override
    {
        auto offsets = keyGroupRangeOffsets_.getIntersection(keyGroupRange);
        if (offsets.getKeyGroupRange().getNumberOfKeyGroups() <= 0) {
            return nullptr;
        }
        return std::make_shared<KeyGroupsStateHandle>(
            offsets, stateHandle_, stateHandleId_);
    }

    KeyGroupRange GetKeyGroupRange() const override {
        return keyGroupRangeOffsets_.getKeyGroupRange();
    }

    StateHandleID GetStateHandleId() const override {
        return stateHandleId_;
    }

    void RegisterSharedStates(SharedStateRegistry& stateRegistry, int64_t checkpointID) override {
        // No shared states
    }

    long GetStateSize() const override {
        return stateHandle_->GetStateSize();
    }

    long GetCheckpointedSize() override {
        return GetStateSize();
    }

    void DiscardState() override {
        stateHandle_->DiscardState();
    }

    static KeyGroupsStateHandle restore(
        const KeyGroupRangeOffsets& groupRangeOffsets,
        std::shared_ptr<StreamStateHandle> streamStateHandle,
        const StateHandleID& stateHandleId)
    {
        return KeyGroupsStateHandle(groupRangeOffsets, std::move(streamStateHandle), stateHandleId);
    }

    const KeyGroupRangeOffsets& getGroupRangeOffsets() const
    {
        return keyGroupRangeOffsets_;
    }

    std::shared_ptr<StreamStateHandle> getDelegateStateHandle() const
    {
        return stateHandle_;
    }

    int64_t getOffsetForKeyGroup(int keyGroupId) const
    {
        return keyGroupRangeOffsets_.getKeyGroupOffset(keyGroupId);
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        return stateHandle_->GetStreamStateHandleID();
    }

    bool operator==(const StreamStateHandle& other) const override
    {
        auto pOther = dynamic_cast<const KeyGroupsStateHandle*>(&other);
        if (!pOther) return false;
        return keyGroupRangeOffsets_ == pOther->keyGroupRangeOffsets_
            && stateHandle_->GetStreamStateHandleID() == pOther->stateHandle_->GetStreamStateHandleID()
            && stateHandle_ == pOther->stateHandle_;
    }

    std::size_t hashCode()
    {
        std::size_t seed = 0x8A20E9D7;
        seed ^= (seed << 6) + (seed >> 2) + 0x198CE17B + keyGroupRangeOffsets_.hashCode();
        seed ^= (seed << 6) + (seed >> 2) + 0x2BC8E8D1 + stateHandle_->GetStreamStateHandleID().hashCode();
        seed ^= (seed << 6) + (seed >> 2) + 0x9B74C1E3 + stateHandleId_.hashCode();
        return seed;
    }

    std::string ToString() const override
    {
        nlohmann::json json;
        json["stateHandleName"] = "KeyGroupsStateHandle";
        json["stateHandleId"] = nlohmann::json::parse(stateHandleId_.ToString());
        json["keyGroupRangeOffsets"] = nlohmann::json::parse(keyGroupRangeOffsets_.ToString());
        return json.dump();
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override {return stateHandle_->AsBytesIfInMemory();}

private:
    std::shared_ptr<StreamStateHandle> stateHandle_;
    KeyGroupRangeOffsets keyGroupRangeOffsets_;
    StateHandleID stateHandleId_;
};

#endif // FLINK_TNEL_KEYGROUPSSTATEHANDLE_H
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
#ifndef OMNISTREAM_PLACEHOLDERSTREAMSTATEHANDLE_H
#define OMNISTREAM_PLACEHOLDERSTREAMSTATEHANDLE_H

#include "PhysicalStateHandleID.h"
#include "StreamStateHandle.h"
#include <optional>
#include <cstdint>
#include <stdexcept>

class PlaceholderStreamStateHandle : public StreamStateHandle {
public:
    PlaceholderStreamStateHandle(std::unique_ptr<PhysicalStateHandleID> physicalID, int64_t stateSize)
        : physicalID(std::move(physicalID)), stateSize(stateSize) {}

    std::unique_ptr<FSDataInputStream> OpenInputStream() const override
    {
        throw std::runtime_error(
            "This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override
    {
        throw std::runtime_error(
            "This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
    }

    std::unique_ptr<PhysicalStateHandleID> GetStreamStateHandleID()
    {
        return std::move(physicalID);
    }

    std::string GetStreamStateHandleIDKeyString()
    {
        return physicalID->getKeyString();
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        return *physicalID;
    }

    int64_t GetStateSize() const override
    {
        return stateSize;
    }

    void DiscardState() override
    {
        LOG("do nothing.");
    }

    bool operator==(const StreamStateHandle& other) const override
    {
        return false;
    }

    std::string ToString() const override
    {
        nlohmann::json json;
        json["stateHandleName"] = "PlaceholderStreamStateHandle";
        json["physicalID"] = nlohmann::json::parse(physicalID->ToString());
        json["stateSize"] = GetStateSize();
        return json.dump();
    }

private:
    static const int64_t serialVersionUID = 1L;
    std::unique_ptr<PhysicalStateHandleID> physicalID;
    int64_t stateSize;
};

#endif // OMNISTREAM_PLACEHOLDERSTREAMSTATEHANDLE_H

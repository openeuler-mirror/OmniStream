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
#ifndef FLINK_TNEL_PHYSICALSTATEHANDLEID_H
#define FLINK_TNEL_PHYSICALSTATEHANDLEID_H
#include "StateObject.h"

#include <memory>
#include <optional>
#include <vector>
#include <string>
#include <stdexcept>

class PhysicalStateHandleID {
public:
    explicit PhysicalStateHandleID(const std::string &keyString)
        : keyString_(keyString) {}

    const std::string &getKeyString() const
    {
        return keyString_;
    }

    bool operator==(const PhysicalStateHandleID &other) const
    {
        return keyString_ == other.keyString_;
    }

    bool operator!=(const PhysicalStateHandleID &other) const
    {
        return !(*this == other);
    }

    std::size_t hashCode() const
    {
        return std::hash<std::string>()(keyString_);
    }

    std::string ToString() const
    {
        nlohmann::json j;
        j["stateHandleName"] = "PhysicalStateHandleID";
        j["keyString"] = keyString_;
        return j.dump();
    }
private:
    std::string keyString_;
};
#endif // FLINK_TNEL_PHYSICALSTATEHANDLEID_H

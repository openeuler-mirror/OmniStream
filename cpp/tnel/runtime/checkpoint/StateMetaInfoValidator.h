/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <stdexcept>
#include <sstream>
#include <vector>

#include "common.h"

#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "core/api/common/state/StateDescriptor.h"

namespace omnistream {
// state metadata 校验器 按 name 建立索引，提供 keyed state 类型级校验和白名单收口。
// 不读取 payload、不创建 VectorBatch accessor、不打开输出流、不写 construction backend。
class StateMetaInfoValidator {
public:
    explicit StateMetaInfoValidator(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
    {
        for (size_t i = 0; i < metaInfos.size(); i++) {
            const auto& meta = metaInfos[i];
            if (meta == nullptr) {
                continue;
            }
            const std::string& name = meta->getName();
            auto inserted = byName_.emplace(name, meta);
            if (!inserted.second) {
                INFO_RELEASE("Error: StateMetaInfoValidator: duplicate source state name: " + name);
                throw std::runtime_error("StateMetaInfoValidator: duplicate source state name: " + name);
            }
        }
    }

    // 要求指定 name 的 keyed value state 存在且 backend type 为 KEY_VALUE。
    void requireKeyedValueState(const std::string& stateName)
    {
        requireKeyedState(stateName, StateDescriptor::Type::VALUE);
    }

    // 要求指定 name 的 keyed map state 存在且 backend type 为 KEY_VALUE。
    void requireKeyedMapState(const std::string& stateName)
    {
        requireKeyedState(stateName, StateDescriptor::Type::MAP);
    }

    // 要求指定 name 的 keyed list state 存在且 backend type 为 KEY_VALUE。
    void requireKeyedListState(const std::string& stateName)
    {
        requireKeyedState(stateName, StateDescriptor::Type::LIST);
    }

    // 要求指定 name 的 keyed value state 及其附属 VB side table（名称 = stateName + "vb"）同时存在。
    void requireKeyedValueStateWithVB(const std::string& stateName)
    {
        requireKeyedValueState(stateName);
        consumeVbSideTable(stateName);
    }

    // 要求指定 name 的 keyed map state 及其附属 VB side table 同时存在。
    void requireKeyedMapStateWithVB(const std::string& stateName)
    {
        requireKeyedMapState(stateName);
        consumeVbSideTable(stateName);
    }

    // 要求指定 name 的 keyed list state 及其附属 VB side table 同时存在。
    void requireKeyedListStateWithVB(const std::string& stateName)
    {
        requireKeyedListState(stateName);
        consumeVbSideTable(stateName);
    }

    // 批量校验：要求 names 中所有 keyed value state 均存在。
    void requireKeyedValueStates(const std::vector<std::string>& stateNames)
    {
        for (const auto& name : stateNames) {
            requireKeyedValueState(name);
        }
    }

    // 批量校验：要求 names 中所有 keyed map state 均存在。
    void requireKeyedMapStates(const std::vector<std::string>& stateNames)
    {
        for (const auto& name : stateNames) {
            requireKeyedMapState(name);
        }
    }

    // 批量校验：要求 names 中所有 keyed list state 均存在。
    void requireKeyedListStates(const std::vector<std::string>& stateNames)
    {
        for (const auto& name : stateNames) {
            requireKeyedListState(name);
        }
    }

    // 批量校验：要求 names 中所有 keyed value state 及其附属 VB side table 均存在。
    void requireKeyedValueStatesWithVB(const std::vector<std::string>& stateNames)
    {
        for (const auto& name : stateNames) {
            requireKeyedValueStateWithVB(name);
        }
    }

    // 批量校验：要求 names 中所有 keyed map state 及其附属 VB side table 均存在。
    void requireKeyedMapStatesWithVB(const std::vector<std::string>& stateNames)
    {
        for (const auto& name : stateNames) {
            requireKeyedMapStateWithVB(name);
        }
    }

    // 批量校验：要求 names 中所有 keyed list state 及其附属 VB side table 均存在。
    void requireKeyedListStatesWithVB(const std::vector<std::string>& stateNames)
    {
        for (const auto& name : stateNames) {
            requireKeyedListStateWithVB(name);
        }
    }

    // 收口检查：不允许存在未被上述 require* 消费的 state。缺失或多余 state 均 fail fast。
    void requireNoMoreStates() const
    {
        for (const auto& kv : byName_) {
            if (consumed_.find(kv.first) == consumed_.end()) {
                std::ostringstream oss;
                oss << "Error: StateMetaInfoValidator: unexpected source state '" << kv.first
                    << "'. All source states must be explicitly consumed.";
                INFO_RELEASE(oss.str());
                throw std::runtime_error(oss.str());
            }
        }
    }

    // 校验 PRIORITY_QUEUE 类型的 state：仅允许 _timer_state 前缀的 PQ state，并显式消费。
    // 不符合前缀的 PRIORITY_QUEUE state 会 fail-fast（静默跳过 = 丢失 timer 数据，不可接受）。
    // 前缀与 InternalTimeServiceManager::TIMER_STATE_PREFIX 保持一致。
    void requirePriorityQueueStates()
    {
        static const std::string timerStatePrefix = "_timer_state";
        for (const auto& kv : byName_) {
            if (kv.second->getBackendStateType() != StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
                continue;
            }
            const std::string& name = kv.first;
            if (name.compare(0, timerStatePrefix.size(), timerStatePrefix) != 0) {
                std::ostringstream oss;
                oss << "Error: StateMetaInfoValidator: unexpected PRIORITY_QUEUE state '" << name
                    << "'. Only states prefixed with '" << timerStatePrefix << "' are expected.";
                INFO_RELEASE(oss.str());
                throw std::runtime_error(oss.str());
            }
            consumed_.insert(name);
        }
    }

    // 返回已消费的 state name 集合（供调用方进一步校验）
    const std::set<std::string>& consumed() const
    {
        return consumed_;
    }

    // 按 name 查找已索引的 StateMetaInfoSnapshot（仅限已消费的 name）
    const std::shared_ptr<StateMetaInfoSnapshot>& get(const std::string& name) const
    {
        auto it = byName_.find(name);
        if (it == byName_.end()) {
            INFO_RELEASE("Error: StateMetaInfoValidator: state not found: " + name);
            throw std::runtime_error("StateMetaInfoValidator: state not found: " + name);
        }
        return it->second;
    }

private:
    std::map<std::string, std::shared_ptr<StateMetaInfoSnapshot>> byName_;
    std::set<std::string> consumed_;

    // 消费 VB side table（如果存在），由各 WithVB 方法复用。
    void consumeVbSideTable(const std::string& stateName)
    {
        std::string vbName = stateName + "vb";
        auto it = byName_.find(vbName);
        if (it != byName_.end()) {
            consumed_.insert(vbName);
        }
    }

    // 统一 keyed state 校验入口：requireExisting + requireKeyValueBackend + requireStateType。
    void requireKeyedState(const std::string& stateName, StateDescriptor::Type expectedType)
    {
        auto it = requireExisting(stateName);
        requireKeyValueBackend(stateName, it->second);
        requireStateType(stateName, it->second, expectedType);
    }

    std::map<std::string, std::shared_ptr<StateMetaInfoSnapshot>>::const_iterator requireExisting(
        const std::string& stateName)
    {
        auto it = byName_.find(stateName);
        if (it == byName_.end()) {
            INFO_RELEASE("Error: StateMetaInfoValidator: required source state not found: " + stateName);
            throw std::runtime_error("StateMetaInfoValidator: required source state not found: " + stateName);
        }
        consumed_.insert(stateName);
        return it;
    }

    static void requireKeyValueBackend(const std::string& stateName, const std::shared_ptr<StateMetaInfoSnapshot>& meta)
    {
        if (meta == nullptr) {
            INFO_RELEASE("Error: StateMetaInfoValidator: null meta info for state: " + stateName);
            throw std::runtime_error("StateMetaInfoValidator: null meta info for state: " + stateName);
        }
        if (meta->getBackendStateType() != StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
            INFO_RELEASE("Error: StateMetaInfoValidator: state '" + stateName + "' is not a KEY_VALUE state");
            throw std::runtime_error("StateMetaInfoValidator: state '" + stateName + "' is not a KEY_VALUE state");
        }
    }

    static void requireStateType(
        const std::string& stateName,
        const std::shared_ptr<StateMetaInfoSnapshot>& meta,
        StateDescriptor::Type expectedType)
    {
        auto typeStr = meta->getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
        auto actualType = StateDescriptor::StringToType(typeStr);
        if (actualType == StateDescriptor::Type::UNKNOWN) {
            std::ostringstream oss;
            oss << "Error: StateMetaInfoValidator: state '" << stateName
                << "' has missing or invalid KEYED_STATE_TYPE metadata (typeStr='" << typeStr << "')";
            INFO_RELEASE(oss.str());
            throw std::runtime_error(oss.str());
        }
        if (actualType != expectedType) {
            std::ostringstream oss;
            oss << "Error: StateMetaInfoValidator: state '" << stateName
                << "' type mismatch, expected=" << static_cast<int>(expectedType)
                << ", actual=" << static_cast<int>(actualType);
            INFO_RELEASE(oss.str());
            throw std::runtime_error(oss.str());
        }
    }
};
} // namespace omnistream

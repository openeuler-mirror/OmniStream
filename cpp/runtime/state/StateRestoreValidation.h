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

#include <string_view>
#include <unordered_set>
#include <vector>
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/IncrementalLocalKeyedStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"

namespace omnistream {

/**
 * Reads state metadata from a single keyed state handle based on its concrete type.
 *
 * @param stateHandle the keyed state handle to read metadata from
 * @param omniTaskBridge bridge for reading state metadata
 * @return vector of StateMetaInfoSnapshot, empty if type is unrecognized
 */
inline std::vector<StateMetaInfoSnapshot> readMetaInfoFromHandle(
    const std::shared_ptr<KeyedStateHandle>& stateHandle, const std::shared_ptr<OmniTaskBridge>& omniTaskBridge)
{
    if (auto handle = std::dynamic_pointer_cast<KeyGroupsStateHandle>(stateHandle)) {
        auto serializerStr = TaskStateSnapshotSerializer::parseKeyGroupsStateHandle(handle);
        return omniTaskBridge->readMetaData(to_string(serializerStr));
    }
    if (auto handle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(stateHandle)) {
        auto serializerStr = TaskStateSnapshotSerializer::parseIncrementalRemoteKeyedStateHandle(handle);
        return omniTaskBridge->readMetaData(to_string(serializerStr));
    }
    if (auto handle = std::dynamic_pointer_cast<IncrementalLocalKeyedStateHandle>(stateHandle)) {
        auto serializerStr = TaskStateSnapshotSerializer::parseIncrementalKeyedStateHandle(handle);
        return omniTaskBridge->readMetaData(to_string(serializerStr));
    }
    return {};
}

/**
 * Validates that non-vb and vb state names are in 1:1 correspondence.
 * Every name not ending with "vb" must have a matching name+"vb", and vice versa.
 *
 * @param restoreStateHandles state handles to validate
 * @param omniTaskBridge bridge for reading state metadata
 * @return true if non-vb and vb names are perfectly paired, false otherwise
 */
inline bool validateRestoreStateHandles(
    const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
    const std::shared_ptr<OmniTaskBridge>& omniTaskBridge)
{
    for (const auto& stateHandle : restoreStateHandles) {
        auto handleMetaInfos = readMetaInfoFromHandle(stateHandle, omniTaskBridge);

        std::unordered_set<std::string_view> nonVbNameSet;
        std::unordered_set<std::string_view> vbStripped;
        for (const auto& info : handleMetaInfos) {
            if (info.getBackendStateType() != StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
                continue;
            }
            const auto& name = info.getName();
            if (name.size() > 2 && name[name.size() - 2] == 'v' && name[name.size() - 1] == 'b') {
                vbStripped.emplace(name.data(), name.size() - 2);
            } else {
                nonVbNameSet.emplace(name.data(), name.size());
            }
        }
        if (vbStripped.size() != nonVbNameSet.size()) {
            return false;
        }
        for (const auto& name : nonVbNameSet) {
            if (vbStripped.find(name) == vbStripped.end()) {
                return false;
            }
        }
    }
    return true;
}

} // namespace omnistream

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

#include <algorithm>
#include <iomanip>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"

namespace omnistream {

// 恢复方向公共调度流程：遍历 Flink logical source entries，通过 Adaptor 提供的
// RestorePlan 按 StateType 分发到 KV / KV_WITH_VB / PQ 子流程。
// Derived 需实现: buildRestorePlan / getStateType / buildOmniMainMetaInfo /

class VectorBatchRestoreFlow final {
public:
    template <typename Derived>
    static void executeRestore(
        Derived& derived, SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
    {
        INFO_RELEASE("VectorBatchRestoreFlow::executeRestore - start (plan+dispatch model)");

        int restoreResultIndex = 0;
        while (restoreIterator.hasNext()) {
            auto restoreResult = restoreIterator.next();
            auto& metaInfos = restoreResult->getStateMetaInfoSnapshots();
            INFO_RELEASE(
                "VectorBatchRestoreFlow: restoreResult#" << restoreResultIndex++
                                                         << ", metaInfoCount=" << metaInfos.size());

            auto restorePlan = derived.buildRestorePlan(metaInfos);

            // Step 1: 预创建所有 writer（按 kvStateId → type 分类）
            std::unordered_map<int, std::unique_ptr<RestoreKVState>> kvWriters;
            std::unordered_map<int, std::unique_ptr<RestoreKVStateVB>> kvVbWriters;
            std::unordered_map<int, std::unique_ptr<RestorePQState>> pqWriters;
            std::unordered_map<int, RestoreStateType> stateTypeMap;

            for (int kvStateId : restorePlan->sourceKvStateIds()) {
                RestoreStateType stateType = derived.getStateType(kvStateId, *restorePlan);
                stateTypeMap[kvStateId] = stateType;

                switch (stateType) {
                    case RestoreStateType::KV: {
                        auto mainMetaInfo = derived.buildOmniMainMetaInfo(kvStateId, *restorePlan);
                        kvWriters[kvStateId] = backend.createKVState(kvStateId, mainMetaInfo);
                        break;
                    }
                    case RestoreStateType::KV_WITH_VB: {
                        auto mainMetaInfo = derived.buildOmniMainMetaInfo(kvStateId, *restorePlan);
                        kvVbWriters[kvStateId] = backend.createKVStateVB(
                            kvStateId,
                            mainMetaInfo,
                            restorePlan->columnTypes(kvStateId),
                            restorePlan->batchSize(kvStateId));
                        break;
                    }
                    case RestoreStateType::PQ: {
                        pqWriters[kvStateId] = backend.createPQState(kvStateId, metaInfos[kvStateId]);
                        break;
                    }
                    default: break;
                }
            }

            // Step 2: 单次遍历 keyGroup→entry，按 kvStateId 路由到对应 writer
            auto keyGroupIterator = restoreResult->getKeyGroupIterator();
            try {
                while (keyGroupIterator->hasNext()) {
                    auto keyGroup = keyGroupIterator->next();
                    int keyGroupId = keyGroup->getKeyGroupId();
                    auto entryIter = keyGroup->getKeyGroupEntries();

                    while (entryIter->hasNext()) {
                        auto entry = entryIter->next();
                        int kvStateId = entry.getKvStateId();

                        auto stIt = stateTypeMap.find(kvStateId);
                        if (stIt == stateTypeMap.end()) {
                            continue;
                        }

                        switch (stIt->second) {
                            case RestoreStateType::KV: {
                                auto& w = kvWriters[kvStateId];
                                w->setKeyGroupId(keyGroupId);
                                ByteView valueView(entry.getValue().data(), entry.getValue().size());
                                w->template writeEntry<ByteView>(entry.getKey(), valueView);
                                break;
                            }
                            case RestoreStateType::KV_WITH_VB: {
                                auto& w = kvVbWriters[kvStateId];
                                w->setKeyGroupId(keyGroupId);
                                derived.retrieveKVRowData(
                                    entry.getKey(),
                                    entry.getValue(),
                                    kvStateId,
                                    *restorePlan,
                                    [&w](const std::vector<int8_t>& key, const RowDataView& row) {
                                        w->writeRowData(key, row);
                                    });
                                break;
                            }
                            case RestoreStateType::PQ: {
                                auto& w = pqWriters[kvStateId];
                                w->writeEntry(entry.getKey(), entry.getValue());
                                break;
                            }
                            default: break;
                        }
                    }

                    // keyGroup 切换时强制 flush 所有 KV_WITH_VB writer 的 VB 尾批，
                    // 避免跨 keyGroup 的数据混合到同一 VB batch 中。
                    for (auto& [id, w] : kvVbWriters) {
                        w->flushVB();
                    }
                }

                // 统一 flush 所有 writer
                for (auto& [id, w] : kvWriters) w->flush();
                for (auto& [id, w] : kvVbWriters) w->flush();
                for (auto& [id, w] : pqWriters) w->flush();
            } catch (...) {
                for (auto& [id, w] : kvWriters) w->discard();
                for (auto& [id, w] : kvVbWriters) w->discard();
                for (auto& [id, w] : pqWriters) w->discard();
                throw;
            }
        }
        INFO_RELEASE("VectorBatchRestoreFlow::executeRestore - complete, restoreResults=" << restoreResultIndex);
    }

private:
    VectorBatchRestoreFlow() = delete;
};

} // namespace omnistream

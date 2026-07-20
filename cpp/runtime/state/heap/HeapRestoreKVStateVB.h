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

#include <vector>

#include "runtime/state/heap/HeapRestoreKVState.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"

namespace omnistream {

template <typename K>
class HeapRestoreBackendDelegate;

// ============================================================================
// HeapRestoreKVStateVB — 继承 HeapRestoreKVState 复用主表能力，扩展 VB side table
// ============================================================================
// 菱形继承：HeapRestoreKVState<K> → virtual RestoreKVState ← RestoreKVStateVB
// HeapRestoreKVStateVB 复用 deserializeKey/DeserializedKeyGuard/writeBytesEntry
// 等主表状态能力，同时通过 RestoreKVStateVB 接口提供 VectorBatch 攒批写入。

template <typename K>
class HeapRestoreKVStateVB : public HeapRestoreKVState<K>, public RestoreKVStateVB {
public:
    HeapRestoreKVStateVB(
        HeapRestoreBackendDelegate<K>& delegate,
        typename HeapRestoreBackendDelegate<K>::RestoreStateInfo& stateInfo,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes,
        int vectorBatchSize);

    // 消除菱形继承的二义性：委托给 RestoreKVStateVB（flush vb tail + flush main writer）
    void flush() override
    {
        RestoreKVStateVB::flush();
    }

    void discard() override
    {
        RestoreKVStateVB::discard();
    }

protected:
    // 显式引入模板基类成员，避免 dependent-name 查找失败
    using HeapRestoreKVState<K>::stateInfo_;
    using HeapRestoreKVState<K>::delegate_;
    using HeapRestoreKVState<K>::keyGroupId_;
    using HeapRestoreKVState<K>::deserializeKey;
    using typename HeapRestoreKVState<K>::DeserializedKeyGuard;

    omnistream::ComboId appendRowToVectorBatch(const RowDataView& row) override;
    void flushVectorBatchIfNotEmpty() override;
    void flushMainWriter() override;
    void discardVectorBatch() override;
    void discardMainWriter() override;

    // 覆写 HeapRestoreKVState 的 fail-fast writeLongEntry：
    // VB 恢复路径通过 writeRowData → writeEntry<omnistream::ComboId> → writeLongEntry 写入 comboId
    void writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t value) override;

    // writeBytesEntry 从 HeapRestoreKVState 继承（按 stateType 路由 MAP/LIST/VALUE）

    std::vector<omniruntime::type::DataTypeId> columnTypes_;
    int vectorBatchSize_;
    VbBatchState vbState_;
};

// ============================================================================
// 实现
// ============================================================================

template <typename K>
HeapRestoreKVStateVB<K>::HeapRestoreKVStateVB(
    HeapRestoreBackendDelegate<K>& delegate,
    typename HeapRestoreBackendDelegate<K>::RestoreStateInfo& stateInfo,
    const std::vector<omniruntime::type::DataTypeId>& columnTypes,
    int vectorBatchSize)
    : HeapRestoreKVState<K>(delegate, stateInfo),
      columnTypes_(columnTypes),
      vectorBatchSize_(vectorBatchSize)
{
}

template <typename K>
void HeapRestoreKVStateVB<K>::writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t value)
{
    if (stateInfo_.mainStateDesc == nullptr) {
        INFO_RELEASE("HeapRestoreKVStateVB: Error: mainStateDesc is null for '" << stateInfo_.stateName << "'");
        throw std::runtime_error(
            "HeapRestoreKVStateVB: Error: mainStateDesc is null for '" + stateInfo_.stateName + "'");
    }

    if (stateInfo_.mainTablePtr == 0) {
        stateInfo_.mainTablePtr = delegate_.getBackend()->getStateTablePtr(stateInfo_.mainStateDesc->getName());
    }
    if (stateInfo_.mainTablePtr == 0) {
        INFO_RELEASE("HeapRestoreKVStateVB: Error: main table not found for '" << stateInfo_.stateName << "'");
        throw std::runtime_error(
            "HeapRestoreKVStateVB: Error: main table not found for '" + stateInfo_.stateName + "'");
    }

    auto [rawKey, rawNs] = deserializeKey(keyBytes);
    DeserializedKeyGuard keyGuard(rawKey, rawNs);

    auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(stateInfo_.mainTablePtr);
    table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), value);

    stateInfo_.mainEntryCount++;
}

template <typename K>
omnistream::ComboId HeapRestoreKVStateVB<K>::appendRowToVectorBatch(const RowDataView& row)
{
    if (row.valueBytes == nullptr || row.columnTypes == nullptr) {
        INFO_RELEASE(
            "HeapRestoreKVStateVB: Error: RowDataView has null valueBytes or columnTypes for '" << stateInfo_.stateName
                                                                                                << "'");
        throw std::runtime_error("HeapRestoreKVStateVB: RowDataView has null valueBytes or columnTypes");
    }
    int64_t comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState_, *row.valueBytes, columnTypes_, vectorBatchSize_);
    if (comboId < 0) {
        INFO_RELEASE("HeapRestoreKVStateVB: Error: appendRowToVectorBatch failed for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVStateVB: appendRowToVectorBatch failed");
    }

    if (vbState_.currentRowId >= vectorBatchSize_) {
        flushVectorBatchIfNotEmpty();
    }

    return comboId;
}

template <typename K>
void HeapRestoreKVStateVB<K>::flushVectorBatchIfNotEmpty()
{
    if (vbState_.currentBatch == nullptr) {
        return;
    }

    std::string vbTableName = stateInfo_.stateName + "vb";
    uintptr_t vbTablePtr = delegate_.getBackend()->getStateTablePtr(vbTableName);
    if (vbTablePtr == 0) {
        INFO_RELEASE("HeapRestoreKVStateVB: Error: VB table not found for '" << vbTableName << "'");
        delete vbState_.currentBatch;
        vbState_.currentBatch = nullptr;
        throw std::runtime_error("HeapRestoreKVStateVB: VB table not found for '" + vbTableName + "'");
    }

    auto* vbTable = reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, VectorBatch*>*>(vbTablePtr);

    VoidNamespace ns;
    VectorBatch* batchToStore = vbState_.currentBatch;
    int32_t actualRowCnt = vbState_.currentRowId;
    if (actualRowCnt < VB_RESTORE_BATCH_SIZE) {
        batchToStore = VectorBatchRestoreUtil::sliceVectorBatch(vbState_.currentBatch, 0, actualRowCnt);
        if (batchToStore != nullptr) {
            delete vbState_.currentBatch;
        } else {
            batchToStore = vbState_.currentBatch;
        }
    }

    int vbKeyGroup = vbTable->getKeyGroupRange()->getStartKeyGroup();
    vbTable->put(vbState_.currentBatchId, vbKeyGroup, ns, batchToStore);

    vbState_.currentBatch = nullptr;
    vbState_.currentBatchId++;
    vbState_.currentRowId = 0;
}

template <typename K>
void HeapRestoreKVStateVB<K>::flushMainWriter()
{
    // Heap main-table writes are immediate via table->put() in writeLongEntry;
    // only VectorBatch side table uses batching (flushVectorBatchIfNotEmpty).
}

template <typename K>
void HeapRestoreKVStateVB<K>::discardVectorBatch()
{
    if (vbState_.currentBatch != nullptr) {
        delete vbState_.currentBatch;
        vbState_.currentBatch = nullptr;
    }
}

template <typename K>
void HeapRestoreKVStateVB<K>::discardMainWriter()
{
    // Heap main-table writes are immediate; nothing to discard.
}

} // namespace omnistream

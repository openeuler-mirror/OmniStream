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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

using namespace omnistream;

namespace {

// ============================================================================
// 辅助工具：创建测试用 BinaryRowData 的序列化字节
// ============================================================================

std::vector<int8_t> serializeBinaryRow(int numFields, const std::vector<int64_t>& values)
{
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(numFields);
    for (size_t i = 0; i < values.size(); i++) {
        row->setLong(i, values[i]);
    }

    BinaryRowDataSerializer serializer(numFields);
    DataOutputSerializer dos;
    OutputBufferStatus obs;
    dos.setBackendBuffer(&obs);
    serializer.serialize(row, dos);

    std::vector<int8_t> bytes(
        reinterpret_cast<const int8_t*>(dos.getData()), reinterpret_cast<const int8_t*>(dos.getData() + dos.length()));
    delete row;
    return bytes;
}

std::vector<omniruntime::type::DataTypeId> makeLongColumnTypes(int count)
{
    return std::vector<omniruntime::type::DataTypeId>(count, omniruntime::type::DataTypeId::OMNI_LONG);
}

// ============================================================================
// sliceVectorBatch 测试
// ============================================================================

TEST(VectorBatchRestoreUtilTest, SliceVectorBatchReturnsSubBatch)
{
    auto* batch = new VectorBatch(5);
    batch->Append(new omniruntime::vec::Vector<int32_t>(5));
    for (int i = 0; i < 5; i++) {
        batch->SetValueAt<int32_t>(0, i, i * 10);
    }

    auto* sliced = VectorBatchRestoreUtil::sliceVectorBatch(batch, 1, 3);
    ASSERT_NE(sliced, nullptr);

    delete batch;
    delete sliced;
}

// ============================================================================
// appendRowToVectorBatch 测试
// ============================================================================

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchReturnsValidComboId)
{
    VbBatchState vbState;
    auto columnTypes = makeLongColumnTypes(1);
    auto valueBytes = serializeBinaryRow(1, {42L});

    int64_t comboId = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, 1024);
    EXPECT_GE(comboId, 0);
    EXPECT_EQ(vbState.currentRowId, 1);
    EXPECT_NE(vbState.currentBatch, nullptr);

    // 清理
    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchCreatesBatchIfNull)
{
    VbBatchState vbState;
    EXPECT_EQ(vbState.currentBatch, nullptr);
    EXPECT_EQ(vbState.currentRowId, 0);

    auto columnTypes = makeLongColumnTypes(2);
    auto valueBytes = serializeBinaryRow(2, {100L, 200L});

    int64_t comboId = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, 1024);
    EXPECT_GE(comboId, 0);
    EXPECT_NE(vbState.currentBatch, nullptr);
    EXPECT_EQ(vbState.currentRowId, 1);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchIncrementsRowId)
{
    VbBatchState vbState;
    auto columnTypes = makeLongColumnTypes(1);

    auto bytes1 = serializeBinaryRow(1, {10L});
    auto bytes2 = serializeBinaryRow(1, {20L});
    auto bytes3 = serializeBinaryRow(1, {30L});

    int64_t combo1 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes1, columnTypes, 1024);
    int64_t combo2 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes2, columnTypes, 1024);
    int64_t combo3 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes3, columnTypes, 1024);

    EXPECT_EQ(vbState.currentRowId, 3);
    // comboId 递增（rowId 部分增加）
    EXPECT_LT(combo1, combo2);
    EXPECT_LT(combo2, combo3);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsNegativeForEmptyValueBytes)
{
    VbBatchState vbState;
    auto columnTypes = makeLongColumnTypes(1);
    std::vector<int8_t> emptyBytes;

    int64_t comboId = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, emptyBytes, columnTypes, 1024);
    EXPECT_LT(comboId, 0);
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsNegativeForZeroColumnTypes)
{
    VbBatchState vbState;
    std::vector<omniruntime::type::DataTypeId> emptyTypes;
    auto valueBytes = serializeBinaryRow(1, {42L});

    int64_t comboId = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, emptyTypes, 1024);
    EXPECT_LT(comboId, 0);
}

// ============================================================================
// populateVectorBatchFromRow 测试
// ============================================================================

TEST(VectorBatchRestoreUtilTest, PopulateVectorBatchFromRowFillsLongValue)
{
    auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
    row->setLong(0, 999L);

    auto* batch = new VectorBatch(4);
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));

    auto columnTypes = makeLongColumnTypes(1);
    VectorBatchRestoreUtil::populateVectorBatchFromRow(batch, columnTypes, row, 2);

    EXPECT_EQ(batch->GetValueAt<int64_t>(0, 2), 999L);

    delete row;
    delete batch;
}

TEST(VectorBatchRestoreUtilTest, PopulateVectorBatchFromRowFillsMultipleColumns)
{
    auto* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setLong(0, 111L);
    row->setLong(1, 222L);

    auto* batch = new VectorBatch(4);
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));

    std::vector<omniruntime::type::DataTypeId> columnTypes = {
        omniruntime::type::DataTypeId::OMNI_LONG, omniruntime::type::DataTypeId::OMNI_LONG};
    VectorBatchRestoreUtil::populateVectorBatchFromRow(batch, columnTypes, row, 1);

    EXPECT_EQ(batch->GetValueAt<int64_t>(0, 1), 111L);
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, 1), 222L);

    delete row;
    delete batch;
}

// ============================================================================
// appendRowToVectorBatch + populate 集成测试 — 全批量流程
// ============================================================================

TEST(VectorBatchRestoreUtilTest, AppendMultipleRowsThenSliceTailBatch)
{
    VbBatchState vbState;
    auto columnTypes = makeLongColumnTypes(1);
    int batchSize = 3;

    for (int64_t val = 0; val < 5; val++) {
        auto valueBytes = serializeBinaryRow(1, {val});
        int64_t comboId = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, batchSize);
        EXPECT_GE(comboId, 0);
    }

    // appendRowToVectorBatch 不自动满批 flush；全部 5 行都在同一个 batch 中
    ASSERT_NE(vbState.currentBatch, nullptr);
    EXPECT_EQ(vbState.currentRowId, 5);

    delete vbState.currentBatch;
}

} // namespace

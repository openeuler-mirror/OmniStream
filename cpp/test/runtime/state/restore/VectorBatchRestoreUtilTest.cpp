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
    constexpr int32_t keyGroupId = 3;
    auto columnTypes = makeLongColumnTypes(1);
    auto valueBytes = serializeBinaryRow(1, {42L});

    ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, 1024, keyGroupId);
    EXPECT_NE(comboId, INVALID_COMBO_ID);
    EXPECT_EQ(vbState.currentRowId, 1);
    EXPECT_EQ(vbState.currentKeyGroupId, keyGroupId);
    EXPECT_NE(vbState.currentBatch, nullptr);

    // 清理
    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchAcceptsByteViewSubrange)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 3;
    auto columnTypes = makeLongColumnTypes(1);
    const auto valueBytes = serializeBinaryRow(1, {42L});
    std::vector<int8_t> backingBytes{9, 8, 7};
    backingBytes.insert(backingBytes.end(), valueBytes.begin(), valueBytes.end());
    backingBytes.insert(backingBytes.end(), {6, 5, 4});
    const ByteView rowBytes(backingBytes.data() + 3, valueBytes.size());

    const ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, rowBytes, columnTypes, 1024, keyGroupId);

    ASSERT_NE(comboId, INVALID_COMBO_ID);
    ASSERT_NE(vbState.currentBatch, nullptr);
    EXPECT_EQ(vbState.currentBatch->GetValueAt<int64_t>(0, 0), 42L);
    EXPECT_EQ(vbState.currentRowId, 1);
    EXPECT_EQ(vbState.currentKeyGroupId, keyGroupId);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchCreatesBatchIfNull)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 3;
    EXPECT_EQ(vbState.currentBatch, nullptr);
    EXPECT_EQ(vbState.currentRowId, 0);

    auto columnTypes = makeLongColumnTypes(2);
    auto valueBytes = serializeBinaryRow(2, {100L, 200L});

    ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, 1024, keyGroupId);
    EXPECT_NE(comboId, INVALID_COMBO_ID);
    EXPECT_NE(vbState.currentBatch, nullptr);
    EXPECT_EQ(vbState.currentRowId, 1);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchIncrementsRowId)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 3;
    auto columnTypes = makeLongColumnTypes(1);

    auto bytes1 = serializeBinaryRow(1, {10L});
    auto bytes2 = serializeBinaryRow(1, {20L});
    auto bytes3 = serializeBinaryRow(1, {30L});

    ComboId combo1 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes1, columnTypes, 1024, keyGroupId);
    ComboId combo2 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes2, columnTypes, 1024, keyGroupId);
    ComboId combo3 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes3, columnTypes, 1024, keyGroupId);

    EXPECT_EQ(vbState.currentRowId, 3);
    // comboId 递增（rowId 部分增加）
    EXPECT_LT(combo1, combo2);
    EXPECT_LT(combo2, combo3);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsNegativeForEmptyValueBytes)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 3;
    auto columnTypes = makeLongColumnTypes(1);
    std::vector<int8_t> emptyBytes;

    ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, emptyBytes, columnTypes, 1024, keyGroupId);
    EXPECT_EQ(comboId, INVALID_COMBO_ID);
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsNegativeForEmptyByteView)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 3;
    auto columnTypes = makeLongColumnTypes(1);

    const ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, ByteView{}, columnTypes, 1024, keyGroupId);

    EXPECT_EQ(comboId, INVALID_COMBO_ID);
    EXPECT_EQ(vbState.currentBatch, nullptr);
}

TEST(VectorBatchRestoreUtilTest, RowDataViewExposesVectorAndByteViewStorage)
{
    const std::vector<int8_t> vectorBytes{1, 2, 3, 4};
    const std::vector<int8_t> backingBytes{9, 8, 7, 6, 5};
    const auto columnTypes = makeLongColumnTypes(1);

    const RowDataView vectorRow{&vectorBytes, &columnTypes};
    const RowDataView byteViewRow{ByteView(backingBytes.data() + 1, 3), &columnTypes};

    EXPECT_EQ(vectorRow.bytes().data(), reinterpret_cast<const uint8_t*>(vectorBytes.data()));
    EXPECT_EQ(vectorRow.bytes().size(), vectorBytes.size());
    EXPECT_EQ(byteViewRow.bytes().data(), reinterpret_cast<const uint8_t*>(backingBytes.data() + 1));
    EXPECT_EQ(byteViewRow.bytes().size(), 3U);
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsNegativeForZeroColumnTypes)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 3;
    std::vector<omniruntime::type::DataTypeId> emptyTypes;
    auto valueBytes = serializeBinaryRow(1, {42L});

    ComboId comboId = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, emptyTypes, 1024, keyGroupId);
    EXPECT_EQ(comboId, INVALID_COMBO_ID);
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
    constexpr int32_t keyGroupId = 3;
    auto columnTypes = makeLongColumnTypes(1);
    int batchSize = 5;

    for (int64_t val = 0; val < 5; val++) {
        auto valueBytes = serializeBinaryRow(1, {val});
        ComboId comboId =
            VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, batchSize, keyGroupId);
        EXPECT_NE(comboId, INVALID_COMBO_ID);
    }

    ASSERT_NE(vbState.currentBatch, nullptr);
    EXPECT_EQ(vbState.currentRowId, 5);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, PopulateVectorBatchFromRowFillsIntValue)
{
    auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
    row->setInt(0, 42);

    auto* batch = new VectorBatch(4);
    batch->Append(new omniruntime::vec::Vector<int32_t>(4));

    std::vector<omniruntime::type::DataTypeId> columnTypes = {omniruntime::type::DataTypeId::OMNI_INT};
    VectorBatchRestoreUtil::populateVectorBatchFromRow(batch, columnTypes, row, 0);

    EXPECT_EQ(batch->GetValueAt<int32_t>(0, 0), 42);

    delete row;
    delete batch;
}

TEST(VectorBatchRestoreUtilTest, PopulateVectorBatchFromRowHandlesMixedTypes)
{
    auto* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setInt(0, 100);
    row->setLong(1, 200L);

    auto* batch = new VectorBatch(4);
    batch->Append(new omniruntime::vec::Vector<int32_t>(4));
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));

    std::vector<omniruntime::type::DataTypeId> columnTypes = {
        omniruntime::type::DataTypeId::OMNI_INT, omniruntime::type::DataTypeId::OMNI_LONG};
    VectorBatchRestoreUtil::populateVectorBatchFromRow(batch, columnTypes, row, 1);

    EXPECT_EQ(batch->GetValueAt<int32_t>(0, 1), 100);
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, 1), 200L);

    delete row;
    delete batch;
}

TEST(VectorBatchRestoreUtilTest, PopulateVectorBatchFromRowHandlesNullValues)
{
    auto* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setLong(0, 42L);

    auto* batch = new VectorBatch(4);
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));

    std::vector<omniruntime::type::DataTypeId> columnTypes = makeLongColumnTypes(2);
    VectorBatchRestoreUtil::populateVectorBatchFromRow(batch, columnTypes, row, 0);

    EXPECT_EQ(batch->GetValueAt<int64_t>(0, 0), 42L);

    delete row;
    delete batch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchHandlesMultipleColumns)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 1;
    std::vector<omniruntime::type::DataTypeId> columnTypes = {
        omniruntime::type::DataTypeId::OMNI_LONG, omniruntime::type::DataTypeId::OMNI_LONG};
    auto valueBytes = serializeBinaryRow(2, {100L, 200L});

    ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, 1024, keyGroupId);
    EXPECT_NE(comboId, INVALID_COMBO_ID);
    EXPECT_EQ(vbState.currentRowId, 1);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsInvalidForUnsupportedType)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 1;
    std::vector<omniruntime::type::DataTypeId> columnTypes = {omniruntime::type::DataTypeId::OMNI_DOUBLE};
    auto valueBytes = serializeBinaryRow(1, {42L});

    ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, 1024, keyGroupId);
    EXPECT_EQ(comboId, INVALID_COMBO_ID);
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchComboIdIncreases)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 2;
    auto columnTypes = makeLongColumnTypes(1);

    std::vector<ComboId> comboIds;
    for (int64_t val = 1; val <= 10; val++) {
        auto valueBytes = serializeBinaryRow(1, {val});
        ComboId comboId =
            VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, 1024, keyGroupId);
        EXPECT_NE(comboId, INVALID_COMBO_ID);
        comboIds.push_back(comboId);
    }

    for (size_t i = 1; i < comboIds.size(); i++) {
        EXPECT_LT(comboIds[i - 1], comboIds[i]);
    }
    EXPECT_EQ(vbState.currentRowId, 10);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsInvalidForDifferentKeyGroupId)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId1 = 1;
    constexpr int32_t keyGroupId2 = 2;
    auto columnTypes = makeLongColumnTypes(1);

    auto bytes1 = serializeBinaryRow(1, {10L});
    ComboId combo1 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes1, columnTypes, 1024, keyGroupId1);
    EXPECT_NE(combo1, INVALID_COMBO_ID);

    auto bytes2 = serializeBinaryRow(1, {20L});
    ComboId combo2 = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes2, columnTypes, 1024, keyGroupId2);
    EXPECT_EQ(combo2, INVALID_COMBO_ID);

    delete vbState.currentBatch;
}

TEST(VectorBatchRestoreUtilTest, CalculateVbSerializableSizeReturnsPositive)
{
    auto* batch = new VectorBatch(4);
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));
    for (int i = 0; i < 4; i++) {
        batch->SetValueAt<int64_t>(0, i, static_cast<int64_t>(i) * 100);
    }

    int32_t size = VectorBatchRestoreUtil::calculateVbSerializableSize(batch);
    EXPECT_GT(size, 0);

    delete batch;
}

TEST(VectorBatchRestoreUtilTest, SerializeVbBatchReturnsValidBuffer)
{
    auto* batch = new VectorBatch(4);
    batch->Append(new omniruntime::vec::Vector<int64_t>(4));
    for (int i = 0; i < 4; i++) {
        batch->SetValueAt<int64_t>(0, i, static_cast<int64_t>(i) * 100);
    }

    int32_t bufferSize = VectorBatchRestoreUtil::calculateVbSerializableSize(batch);
    EXPECT_GT(bufferSize, 0);

    std::vector<uint8_t> buffer(bufferSize);
    auto info = VectorBatchRestoreUtil::serializeVbBatch(batch, bufferSize, buffer.data());
    EXPECT_EQ(info.size, bufferSize);
    EXPECT_NE(info.buffer, nullptr);

    delete batch;
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsInvalidForInvalidRowLength)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 1;
    auto columnTypes = makeLongColumnTypes(1);

    std::vector<int8_t> invalidBytes = {-1, 0, 0, 0};
    ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, invalidBytes, columnTypes, 1024, keyGroupId);
    EXPECT_EQ(comboId, INVALID_COMBO_ID);
}

TEST(VectorBatchRestoreUtilTest, AppendRowReturnsInvalidForRowLengthExceedingBytes)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 1;
    auto columnTypes = makeLongColumnTypes(1);

    std::vector<int8_t> bytes = {0, 0, 1, 0};
    ComboId comboId = VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, bytes, columnTypes, 1024, keyGroupId);
    EXPECT_EQ(comboId, INVALID_COMBO_ID);
}

TEST(VectorBatchRestoreUtilTest, AppendRowToVectorBatchWithSmallBatchSize)
{
    VbBatchState vbState;
    constexpr int32_t keyGroupId = 1;
    auto columnTypes = makeLongColumnTypes(1);
    int batchSize = 1;

    auto valueBytes = serializeBinaryRow(1, {42L});
    ComboId comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState, valueBytes, columnTypes, batchSize, keyGroupId);
    EXPECT_NE(comboId, INVALID_COMBO_ID);
    EXPECT_EQ(vbState.currentRowId, 1);

    delete vbState.currentBatch;
}

} // namespace

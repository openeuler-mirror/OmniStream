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

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <rocksdb/db.h>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/restore/RocksDBRestoreKVStateVB.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/utils/VectorBatchDeserializationUtils.h"

namespace {

class RocksDBRestoreKVStateVBTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        dbPath_ = std::filesystem::temp_directory_path() /
                  ("rocks-vb-restore-writer-" + std::string(testInfo->name()) + "-" + std::to_string(nextDbPathId_++));
        std::filesystem::remove_all(dbPath_);

        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;
        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilies;
        columnFamilies.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
        columnFamilies.emplace_back("main", rocksdb::ColumnFamilyOptions());
        columnFamilies.emplace_back("vb", rocksdb::ColumnFamilyOptions());

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        auto status = rocksdb::DB::Open(options, dbPath_.string(), columnFamilies, &handles, &db_);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(handles.size(), 3U);
        defaultCf_ = handles[0];
        mainCf_ = handles[1];
        vbCf_ = handles[2];
    }

    void TearDown() override
    {
        if (db_ != nullptr) {
            if (vbCf_ != nullptr) {
                EXPECT_TRUE(db_->DestroyColumnFamilyHandle(vbCf_).ok());
            }
            if (mainCf_ != nullptr) {
                EXPECT_TRUE(db_->DestroyColumnFamilyHandle(mainCf_).ok());
            }
            if (defaultCf_ != nullptr) {
                EXPECT_TRUE(db_->DestroyColumnFamilyHandle(defaultCf_).ok());
            }
            EXPECT_TRUE(db_->Close().ok());
            delete db_;
        }
        std::filesystem::remove_all(dbPath_);
    }

    std::filesystem::path dbPath_;
    rocksdb::DB* db_ = nullptr;
    rocksdb::ColumnFamilyHandle* defaultCf_ = nullptr;
    rocksdb::ColumnFamilyHandle* mainCf_ = nullptr;
    rocksdb::ColumnFamilyHandle* vbCf_ = nullptr;
    inline static std::atomic<unsigned long> nextDbPathId_{0};
};

std::vector<int8_t> serializeBinaryLongRow(int64_t value)
{
    std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
    row->setLong(0, value);

    BinaryRowDataSerializer serializer(1);
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    serializer.serialize(row.get(), output);
    return {reinterpret_cast<int8_t*>(output.getData()), reinterpret_cast<int8_t*>(output.getData() + output.length())};
}

std::vector<int8_t> serializeLong(int64_t value)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    LongSerializer serializer;
    serializer.serialize(&value, output);
    return {
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition())};
}

std::vector<int8_t> makeVectorBatchKey(int keyGroupId, int keyGroupPrefixBytes, int64_t sequenceNumber)
{
    DataOutputSerializer output(keyGroupPrefixBytes + 8);
    CompositeKeySerializationUtils::writeKeyGroup(keyGroupId, keyGroupPrefixBytes, output);
    LongSerializer serializer;
    serializer.serialize(&sequenceNumber, output);
    return {
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition())};
}

} // namespace

TEST_F(RocksDBRestoreKVStateVBTest, ByteViewEntryIsWrittenToMainColumnFamily)
{
    int64_t mainEntryCount = 0;
    int64_t vbBatchCount = 0;
    omnistream::RocksDBWriterContext context;
    context.db = db_;
    context.writeBatchSize = 2 * 1024 * 1024;
    context.keyGroupPrefixBytes = 1;
    context.mainEntryCount = &mainEntryCount;
    context.vbBatchCount = &vbBatchCount;

    const std::vector<int8_t> key = {1, 2, 3, 4};
    const std::vector<int8_t> value = {0, 9, 8, 7, 0, 6};
    {
        omnistream::RocksDBRestoreKVStateVB<int> writer(
            context, mainCf_, vbCf_, 0, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);
        writer.writeEntry<ByteView>(key, ByteView::fromBuffer(value.data(), value.size()));
        writer.flush();
    }

    std::string restoredValue;
    auto status = db_->Get(
        rocksdb::ReadOptions(),
        mainCf_,
        rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
        &restoredValue);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(restoredValue.size(), value.size());
    EXPECT_EQ(std::vector<int8_t>(restoredValue.begin(), restoredValue.end()), value);
    EXPECT_EQ(mainEntryCount, 1);
    EXPECT_EQ(vbBatchCount, 0);
}
TEST_F(RocksDBRestoreKVStateVBTest, LongEntryUsesFlinkLongEncoding)
{
    omnistream::RocksDBWriterContext context{db_, 2 * 1024 * 1024, 1, 0, -1, nullptr, nullptr};
    const std::vector<int8_t> key{8, 6, 7};
    constexpr int64_t value = 123456789;

    omnistream::RocksDBRestoreKVStateVB<int> writer(
        context, mainCf_, vbCf_, 0, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);
    writer.writeEntry<int64_t>(key, value);
    writer.flush();

    std::string restoredValue;
    auto status = db_->Get(
        rocksdb::ReadOptions(),
        mainCf_,
        rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
        &restoredValue);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(std::vector<int8_t>(restoredValue.begin(), restoredValue.end()), serializeLong(value));
}

TEST_F(RocksDBRestoreKVStateVBTest, NullRowDataPointersAreRejectedBeforeWriting)
{
    int64_t mainEntryCount = 0;
    int64_t vbBatchCount = 0;
    omnistream::RocksDBWriterContext context{db_, 2 * 1024 * 1024, 1, 0, 3, &mainEntryCount, &vbBatchCount};
    const std::vector<int8_t> key{3, 3, 3};
    const auto valueBytes = serializeBinaryLongRow(42);
    const std::vector<omniruntime::type::DataTypeId> columnTypes{omniruntime::type::DataTypeId::OMNI_LONG};
    omnistream::RocksDBRestoreKVStateVB<int> writer(context, mainCf_, vbCf_, 0, columnTypes, 1024);

    try {
        writer.writeRowData(key, omnistream::RowDataView{nullptr, &columnTypes});
        FAIL() << "Expected null valueBytes to be rejected";
    } catch (const std::runtime_error& exception) {
        EXPECT_STREQ(exception.what(), "RocksDBRestoreKVStateVB: RowDataView has null valueBytes or columnTypes");
    }
    try {
        writer.writeRowData(key, omnistream::RowDataView{&valueBytes, nullptr});
        FAIL() << "Expected null columnTypes to be rejected";
    } catch (const std::runtime_error& exception) {
        EXPECT_STREQ(exception.what(), "RocksDBRestoreKVStateVB: RowDataView has null valueBytes or columnTypes");
    }

    EXPECT_EQ(mainEntryCount, 0);
    EXPECT_EQ(vbBatchCount, 0);
    std::string restoredValue;
    EXPECT_TRUE(db_->Get(
                       rocksdb::ReadOptions(),
                       mainCf_,
                       rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
                       &restoredValue)
                    .IsNotFound());
}

TEST_F(RocksDBRestoreKVStateVBTest, RowDataFlushWritesMatchingMainReferenceAndVectorBatch)
{
    int64_t mainEntryCount = 0;
    int64_t vbBatchCount = 0;
    constexpr int keyGroupId = 3;
    constexpr int keyGroupPrefixBytes = 1;
    constexpr int64_t rowValue = 987654321;
    omnistream::RocksDBWriterContext context{
        db_, 2 * 1024 * 1024, keyGroupPrefixBytes, 0, keyGroupId, &mainEntryCount, &vbBatchCount};
    const std::vector<int8_t> mainKey{1, 4, 1};
    const auto valueBytes = serializeBinaryLongRow(rowValue);
    const std::vector<omniruntime::type::DataTypeId> columnTypes{omniruntime::type::DataTypeId::OMNI_LONG};
    const auto expectedComboId = omnistream::VectorBatchUtil::getComboId(keyGroupId, 0, 0);

    omnistream::RocksDBRestoreKVStateVB<int> writer(context, mainCf_, vbCf_, 0, columnTypes, 1024);
    writer.writeRowData(mainKey, omnistream::RowDataView{&valueBytes, &columnTypes});
    writer.flush();

    std::string mainValue;
    ASSERT_TRUE(db_->Get(
                       rocksdb::ReadOptions(),
                       mainCf_,
                       rocksdb::Slice(reinterpret_cast<const char*>(mainKey.data()), mainKey.size()),
                       &mainValue)
                    .ok());
    EXPECT_EQ(
        std::vector<int8_t>(mainValue.begin(), mainValue.end()), serializeLong(static_cast<int64_t>(expectedComboId)));

    const auto vbKey = makeVectorBatchKey(keyGroupId, keyGroupPrefixBytes, 0);
    std::string vbValue;
    ASSERT_TRUE(db_->Get(
                       rocksdb::ReadOptions(),
                       vbCf_,
                       rocksdb::Slice(reinterpret_cast<const char*>(vbKey.data()), vbKey.size()),
                       &vbValue)
                    .ok());
    std::vector<uint8_t> serializedBatch(vbValue.begin(), vbValue.end());
    ASSERT_GT(serializedBatch.size(), sizeof(int8_t));
    EXPECT_EQ(serializedBatch[0], static_cast<uint8_t>(StreamElementTag::VECTOR_BATCH));
    auto* serializedBatchData = serializedBatch.data() + sizeof(int8_t);
    std::unique_ptr<omnistream::VectorBatch> restoredBatch(
        omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchData));
    ASSERT_NE(restoredBatch, nullptr);
    EXPECT_EQ(restoredBatch->GetValueAt<int64_t>(0, 0), rowValue);
    restoredBatch->FreeAllVectors();
    EXPECT_EQ(mainEntryCount, 1);
    EXPECT_EQ(vbBatchCount, 1);
}

TEST_F(RocksDBRestoreKVStateVBTest, FullBatchFlushesAndNextBatchUsesNextSequenceNumber)
{
    int64_t vbBatchCount = 0;
    constexpr int keyGroupId = 2;
    omnistream::RocksDBWriterContext context{db_, 2 * 1024 * 1024, 1, 0, keyGroupId, nullptr, &vbBatchCount};
    const auto valueBytes = serializeBinaryLongRow(77);
    const std::vector<omniruntime::type::DataTypeId> columnTypes{omniruntime::type::DataTypeId::OMNI_LONG};
    const omnistream::RowDataView row{&valueBytes, &columnTypes};
    omnistream::RocksDBRestoreKVStateVB<int> writer(context, mainCf_, vbCf_, 0, columnTypes, 2);

    EXPECT_EQ(writer.appendRowToVectorBatch(row), omnistream::VectorBatchUtil::getComboId(keyGroupId, 0, 0));
    EXPECT_EQ(writer.appendRowToVectorBatch(row), omnistream::VectorBatchUtil::getComboId(keyGroupId, 0, 1));
    EXPECT_EQ(vbBatchCount, 1);
    EXPECT_EQ(writer.appendRowToVectorBatch(row), omnistream::VectorBatchUtil::getComboId(keyGroupId, 1, 0));
    writer.flushVB();

    EXPECT_EQ(vbBatchCount, 2);
    for (int64_t sequenceNumber : {0, 1}) {
        const auto vbKey = makeVectorBatchKey(keyGroupId, 1, sequenceNumber);
        std::string vbValue;
        EXPECT_TRUE(db_->Get(
                           rocksdb::ReadOptions(),
                           vbCf_,
                           rocksdb::Slice(reinterpret_cast<const char*>(vbKey.data()), vbKey.size()),
                           &vbValue)
                        .ok());
    }
}

TEST_F(RocksDBRestoreKVStateVBTest, KeyGroupSwitchStartsIndependentBatchSequence)
{
    int64_t vbBatchCount = 0;
    constexpr int firstKeyGroup = 2;
    constexpr int secondKeyGroup = 5;
    omnistream::RocksDBWriterContext context{db_, 2 * 1024 * 1024, 1, 0, firstKeyGroup, nullptr, &vbBatchCount};
    const auto valueBytes = serializeBinaryLongRow(88);
    const std::vector<omniruntime::type::DataTypeId> columnTypes{omniruntime::type::DataTypeId::OMNI_LONG};
    const omnistream::RowDataView row{&valueBytes, &columnTypes};
    omnistream::RocksDBRestoreKVStateVB<int> writer(context, mainCf_, vbCf_, 0, columnTypes, 1024);

    EXPECT_EQ(writer.getKeyGroupPrefixBytes(), 1);
    EXPECT_EQ(writer.appendRowToVectorBatch(row), omnistream::VectorBatchUtil::getComboId(firstKeyGroup, 0, 0));
    writer.flushVB();
    writer.setKeyGroupId(secondKeyGroup);
    writer.resetBatchId();
    EXPECT_EQ(writer.appendRowToVectorBatch(row), omnistream::VectorBatchUtil::getComboId(secondKeyGroup, 0, 0));
    writer.flushVB();

    EXPECT_EQ(context.keyGroupId, secondKeyGroup);
    EXPECT_EQ(vbBatchCount, 2);
    for (int keyGroupId : {firstKeyGroup, secondKeyGroup}) {
        const auto vbKey = makeVectorBatchKey(keyGroupId, 1, 0);
        std::string vbValue;
        EXPECT_TRUE(db_->Get(
                           rocksdb::ReadOptions(),
                           vbCf_,
                           rocksdb::Slice(reinterpret_cast<const char*>(vbKey.data()), vbKey.size()),
                           &vbValue)
                        .ok());
    }
}

TEST_F(RocksDBRestoreKVStateVBTest, DiscardDropsUnflushedVectorBatch)
{
    int64_t vbBatchCount = 0;
    constexpr int keyGroupId = 4;
    omnistream::RocksDBWriterContext context{db_, 2 * 1024 * 1024, 1, 0, keyGroupId, nullptr, &vbBatchCount};
    const auto valueBytes = serializeBinaryLongRow(99);
    const std::vector<omniruntime::type::DataTypeId> columnTypes{omniruntime::type::DataTypeId::OMNI_LONG};
    const omnistream::RowDataView row{&valueBytes, &columnTypes};
    omnistream::RocksDBRestoreKVStateVB<int> writer(context, mainCf_, vbCf_, 0, columnTypes, 1024);

    writer.appendRowToVectorBatch(row);
    writer.discard();
    writer.flush();

    const auto vbKey = makeVectorBatchKey(keyGroupId, 1, 0);
    std::string vbValue;
    EXPECT_TRUE(db_->Get(
                       rocksdb::ReadOptions(),
                       vbCf_,
                       rocksdb::Slice(reinterpret_cast<const char*>(vbKey.data()), vbKey.size()),
                       &vbValue)
                    .IsNotFound());
    EXPECT_EQ(vbBatchCount, 0);
}

TEST_F(RocksDBRestoreKVStateVBTest, DiscardDropsPendingMainEntry)
{
    omnistream::RocksDBWriterContext context{db_, 2 * 1024 * 1024, 1, 0, 4, nullptr, nullptr};
    const std::vector<int8_t> key{4, 4, 4};
    const std::vector<int8_t> value{5, 5, 5};
    omnistream::RocksDBRestoreKVStateVB<int> writer(
        context, mainCf_, vbCf_, 0, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);

    writer.writeEntry<ByteView>(key, ByteView::fromBuffer(value.data(), value.size()));
    writer.discard();
    EXPECT_NO_THROW(writer.discard());

    std::string restoredValue;
    EXPECT_TRUE(db_->Get(
                       rocksdb::ReadOptions(),
                       mainCf_,
                       rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
                       &restoredValue)
                    .IsNotFound());
}

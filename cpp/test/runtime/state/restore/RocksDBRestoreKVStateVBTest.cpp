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
#include <string>
#include <vector>

#include <rocksdb/db.h>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/state/restore/RocksDBRestoreKVStateVB.h"

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

TEST_F(RocksDBRestoreKVStateVBTest, ComboIdListIsWrittenUsingFlinkListEncoding)
{
    int64_t mainEntryCount = 0;
    int64_t vbBatchCount = 0;
    omnistream::RocksDBWriterContext context;
    context.db = db_;
    context.writeBatchSize = 2 * 1024 * 1024;
    context.keyGroupPrefixBytes = 1;
    context.mainEntryCount = &mainEntryCount;
    context.vbBatchCount = &vbBatchCount;

    const std::vector<int8_t> key{4, 3, 2, 1};
    const std::vector<omnistream::ComboId> comboIds{11, 22, 33};
    {
        omnistream::RocksDBRestoreKVStateVB<int> writer(
            context, mainCf_, vbCf_, 0, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);
        writer.writeComboIdList(key, comboIds);
        writer.flush();
    }

    DataOutputSerializer expectedOutput;
    OutputBufferStatus expectedOutputStatus{};
    expectedOutput.setBackendBuffer(&expectedOutputStatus);
    LongSerializer longSerializer;
    for (size_t i = 0; i < comboIds.size(); ++i) {
        if (i != 0) {
            expectedOutput.write(',');
        }
        int64_t comboId = comboIds[i];
        longSerializer.serialize(&comboId, expectedOutput);
    }
    const std::vector<int8_t> expectedValue(
        reinterpret_cast<int8_t*>(expectedOutput.getData()),
        reinterpret_cast<int8_t*>(expectedOutput.getData() + expectedOutput.getPosition()));

    std::string restoredValue;
    auto status = db_->Get(
        rocksdb::ReadOptions(),
        mainCf_,
        rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
        &restoredValue);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(std::vector<int8_t>(restoredValue.begin(), restoredValue.end()), expectedValue);
    EXPECT_EQ(mainEntryCount, 1);
    EXPECT_EQ(vbBatchCount, 0);
}

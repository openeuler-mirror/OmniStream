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
#include <vector>

#include "runtime/state/restore/RestoreKVStateVB.h"

namespace {

class RecordingRestoreKVStateVB : public omnistream::RestoreKVStateVB {
public:
    omnistream::ComboId appendRowToVectorBatch(const omnistream::RowDataView& row) override
    {
        ++appendRowCount;
        const ByteView rowBytes = row.bytes();
        const auto* rowBegin = reinterpret_cast<const int8_t*>(rowBytes.data());
        appendedValueBytes.assign(rowBegin, rowBegin + rowBytes.size());
        appendedColumnTypes = *row.columnTypes;
        return comboIdToReturn;
    }

    void writeComboIdList(const std::vector<int8_t>&, const std::vector<omnistream::ComboId>&) override
    {
    }

    int getKeyGroupPrefixBytes() const override
    {
        return 1;
    }

    void resetBatchId() override
    {
    }

    void setKeyGroupId(int) override
    {
    }

    omnistream::ComboId comboIdToReturn = 123;
    int appendRowCount = 0;
    int flushVectorBatchCount = 0;
    int flushMainWriterCount = 0;
    std::vector<int8_t> appendedValueBytes;
    std::vector<omniruntime::type::DataTypeId> appendedColumnTypes;
    std::vector<int8_t> writtenKeyBytes;
    int64_t writtenComboId = 0;

protected:
    void flushVectorBatchIfNotEmpty() override
    {
        ++flushVectorBatchCount;
    }

    void flushMainWriter() override
    {
        ++flushMainWriterCount;
    }

    void discardVectorBatch() override
    {
    }

    void discardMainWriter() override
    {
    }

    void writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t value) override
    {
        writtenKeyBytes = keyBytes;
        writtenComboId = value;
    }

    void writeBytesEntry(const std::vector<int8_t>&, ByteView) override
    {
    }
};

} // namespace

TEST(RestoreKVStateVBTest, WriteRowDataAppendsRowAndWritesReturnedComboId)
{
    RecordingRestoreKVStateVB writer;
    writer.comboIdToReturn = 456;
    const std::vector<int8_t> keyBytes{1, 2, 3};
    const std::vector<int8_t> valueBytes{4, 5, 6};
    const std::vector<omniruntime::type::DataTypeId> columnTypes{omniruntime::type::DataTypeId::OMNI_LONG};
    const omnistream::RowDataView row{&valueBytes, &columnTypes};

    writer.writeRowData(keyBytes, row);

    EXPECT_EQ(writer.appendRowCount, 1);
    EXPECT_EQ(writer.appendedValueBytes, valueBytes);
    EXPECT_EQ(writer.appendedColumnTypes, columnTypes);
    EXPECT_EQ(writer.writtenKeyBytes, keyBytes);
    EXPECT_EQ(writer.writtenComboId, 456);
}

TEST(RestoreKVStateVBTest, FlushVBOnlyFlushesVectorBatch)
{
    RecordingRestoreKVStateVB writer;

    writer.flushVB();

    EXPECT_EQ(writer.flushVectorBatchCount, 1);
    EXPECT_EQ(writer.flushMainWriterCount, 0);
}

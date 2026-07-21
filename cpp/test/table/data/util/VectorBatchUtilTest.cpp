#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "table/data/util/VectorBatchUtil.h"

using omnistream::VectorBatchUtil;

TEST(VectorBatchUtilTest, CopyFlatVectorFromVectorBatch)
{
    constexpr int32_t rowCount = 6;
    omnistream::VectorBatch vectorBatch(rowCount);
    auto* input = new omniruntime::vec::Vector<int32_t>(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        input->SetValue(i, 100 + i * 10);
    }
    input->SetNull(2);
    vectorBatch.Append(input);

    std::vector<int32_t> positions = {4, 2, 4, 0};
    auto* resultBase = VectorBatchUtil::copyVectorByRowIds(&vectorBatch, 0, positions);
    auto* result = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(resultBase);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetSize(), static_cast<int32_t>(positions.size()));
    EXPECT_EQ(result->GetEncoding(), omniruntime::vec::OMNI_FLAT);
    EXPECT_EQ(result->GetValue(0), 140);
    EXPECT_TRUE(result->IsNull(1));
    EXPECT_EQ(result->GetValue(2), 140);
    EXPECT_EQ(result->GetValue(3), 100);

    vectorBatch.ClearVectors();
    delete result;
    delete input;
}

TEST(VectorBatchUtilTest, CopyDictionaryStringVectorAndFlatten)
{
    using omniruntime::mem::AlignedBuffer;
    using omniruntime::vec::DictionaryContainer;
    using omniruntime::vec::LargeStringContainer;
    using omniruntime::vec::NullsBuffer;
    using omniruntime::vec::Vector;

    constexpr int32_t dictionarySize = 5;
    auto* dictionaryValues = new Vector<LargeStringContainer<std::string_view>>(dictionarySize);
    std::vector<std::string> strings = {"zero", "one", "two", "three", "four"};
    for (int32_t i = 0; i < dictionarySize; ++i) {
        std::string_view value(strings[i]);
        dictionaryValues->SetValue(i, value);
    }

    constexpr int32_t rowCount = 6;
    int32_t ids[rowCount] = {4, 1, 3, 1, 0, 2};
    auto nullsBuffer = std::make_shared<AlignedBuffer<uint8_t>>(rowCount);
    auto* nulls = nullsBuffer->GetBuffer();
    for (int32_t i = 0; i < rowCount; ++i) {
        nulls[i] = 0;
    }

    auto dictionary = std::make_shared<DictionaryContainer<std::string_view>>(
        ids,
        rowCount,
        omniruntime::vec::unsafe::UnsafeStringVector::GetContainer(dictionaryValues),
        dictionaryValues->GetSize(),
        dictionaryValues->GetOffset());
    auto* input = new Vector<DictionaryContainer<std::string_view>>(
        rowCount, dictionary, new NullsBuffer(rowCount, nullsBuffer), false, omniruntime::type::OMNI_CHAR);
    input->SetNull(2);
    omnistream::VectorBatch vectorBatch(rowCount);
    vectorBatch.Append(input);

    std::vector<int32_t> positions = {3, 2, 5, 0};
    auto* resultBase = VectorBatchUtil::copyVectorByRowIds(&vectorBatch, 0, positions);
    auto* result = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(resultBase);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetSize(), static_cast<int32_t>(positions.size()));
    EXPECT_EQ(result->GetEncoding(), omniruntime::vec::OMNI_FLAT);
    EXPECT_EQ(result->GetTypeId(), omniruntime::type::OMNI_CHAR);
    EXPECT_EQ(result->GetValue(0), "one");
    EXPECT_TRUE(result->IsNull(1));
    EXPECT_EQ(result->GetValue(2), "two");
    EXPECT_EQ(result->GetValue(3), "four");

    vectorBatch.ClearVectors();
    delete result;
    delete input;
    delete dictionaryValues;
}

TEST(VectorBatchUtilTest, BuildNewVectorBatchByRowIds)
{
    constexpr int32_t rowCount = 6;
    omnistream::VectorBatch vectorBatch(rowCount);

    std::vector<int32_t> intValues = {10, 20, 30, 40, 50, 60};
    std::vector<int64_t> longValues = {100, 200, 300, 400, 500, 600};
    std::vector<int64_t> timestamps = {1001, 1002, 1003, 1004, 1005, 1006};
    std::vector<RowKind> rowKinds = {
        RowKind::INSERT,
        RowKind::DELETE,
        RowKind::UPDATE_AFTER,
        RowKind::INSERT,
        RowKind::UPDATE_BEFORE,
        RowKind::DELETE};

    auto* intVector = new omniruntime::vec::Vector<int32_t>(rowCount);
    auto* longVector = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int32_t row = 0; row < rowCount; ++row) {
        intVector->SetValue(row, intValues[row]);
        longVector->SetValue(row, longValues[row]);
        vectorBatch.setTimestamp(row, timestamps[row]);
        vectorBatch.setRowKind(row, rowKinds[row]);
    }
    intVector->SetNull(4);
    longVector->SetNull(1);
    vectorBatch.Append(intVector);
    vectorBatch.Append(longVector);

    std::vector<int32_t> rowIds = {5, 1, 4, 1, 0};
    auto* result = VectorBatchUtil::buildNewVectorBatchByRowIds(&vectorBatch, rowIds);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetRowCount(), static_cast<int32_t>(rowIds.size()));
    ASSERT_EQ(result->GetVectorCount(), 2);

    auto* resultInts = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(result->Get(0));
    auto* resultLongs = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(result->Get(1));
    for (int32_t row = 0; row < static_cast<int32_t>(rowIds.size()); ++row) {
        const auto sourceRow = rowIds[row];
        EXPECT_EQ(result->getTimestamp(row), timestamps[sourceRow]);
        EXPECT_EQ(result->getRowKind(row), rowKinds[sourceRow]);
        EXPECT_EQ(resultLongs->IsNull(row), longVector->IsNull(sourceRow));
        EXPECT_EQ(resultInts->IsNull(row), intVector->IsNull(sourceRow));
        if (!resultLongs->IsNull(row)) {
            EXPECT_EQ(resultLongs->GetValue(row), longValues[sourceRow]);
        }
        if (!resultInts->IsNull(row)) {
            EXPECT_EQ(resultInts->GetValue(row), intValues[sourceRow]);
        }
    }

    delete result->Get(0);
    delete result->Get(1);
    result->ClearVectors();
    delete result;

    vectorBatch.ClearVectors();
    delete intVector;
    delete longVector;
}

TEST(VectorBatchUtilTest, BuildNewVectorBatchByRowIdsRejectsNullBatch)
{
    std::vector<int32_t> rowIds = {0};
    EXPECT_THROW(VectorBatchUtil::buildNewVectorBatchByRowIds(nullptr, rowIds), std::runtime_error);
}

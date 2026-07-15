#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/utils/ByteView.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace {

constexpr int32_t kSerializedVectorBatchBufferBytes = 64 * 1024;

std::vector<int8_t> serializeVectorBatch(omnistream::VectorBatch* batch)
{
    int32_t batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(batch);
    if (batchSize <= 0 || batchSize > kSerializedVectorBatchBufferBytes) {
        throw std::runtime_error("serializeVectorBatch calculated invalid batch size.");
    }
    std::array<uint8_t, kSerializedVectorBatchBufferBytes> buffer{};
    uint8_t* writeCursor = buffer.data();
    omnistream::SerializedBatchInfo serialized =
        omnistream::VectorBatchSerializationUtils::serializeVectorBatch(batch, batchSize, writeCursor);

    std::vector<int8_t> bytes(
        reinterpret_cast<int8_t*>(serialized.buffer), reinterpret_cast<int8_t*>(serialized.buffer + serialized.size));
    return bytes;
}

std::vector<int8_t> serializeVectorBatch(int32_t rowCount)
{
    auto batch = std::make_unique<omnistream::VectorBatch>(rowCount);
    for (int32_t row = 0; row < rowCount; ++row) {
        batch->setTimestamp(row, 1000 + row);
        batch->setRowKind(row, RowKind::INSERT);
    }
    return serializeVectorBatch(batch.get());
}

class TestVectorBatchStateAccessor : public VectorBatchStateAccessor {
public:
    using VectorBatchStateAccessor::getRow;

    void putSerializedBatch(int64_t batchId, std::vector<int8_t> bytes)
    {
        batches_.emplace(batchId, std::move(bytes));
    }

    void close() override
    {
    }

    std::unique_ptr<RowData> getRowForTest(int64_t batchId, int32_t rowId)
    {
        return getRow(batchId, rowId);
    }

    std::unique_ptr<omnistream::VectorBatch> deserializeBatchForTest(const ByteView& serializedBatch)
    {
        return deserializeBatch(serializedBatch);
    }

    std::unique_ptr<RowData> extractRowForTest(omnistream::VectorBatch* batch, int32_t rowId)
    {
        return extractRow(batch, rowId);
    }

protected:
    bool getSerializedBatch(int64_t batchId, ByteView* serializedBatch) override
    {
        auto iter = batches_.find(batchId);
        if (iter == batches_.end()) {
            return false;
        }

        const std::vector<int8_t>& bytes = iter->second;
        *serializedBatch = ByteView::fromBuffer(bytes.data(), bytes.size());
        return true;
    }

private:
    std::unordered_map<int64_t, std::vector<int8_t>> batches_;
};

class VectorBatchStateAccessorTest : public ::testing::Test {
protected:
    TestVectorBatchStateAccessor accessor_;
};

// 有效 batchId/rowId 和 comboId 入口都应从序列化 VectorBatch 中提取调用方拥有的 RowData。
TEST_F(VectorBatchStateAccessorTest, GetRowReturnsOwnedRowDataForValidSerializedBatch)
{
    constexpr int64_t batchId = 42;
    accessor_.putSerializedBatch(batchId, serializeVectorBatch(2));

    std::unique_ptr<RowData> first = accessor_.getRowForTest(batchId, 1);
    std::unique_ptr<RowData> second = accessor_.getRowForTest(batchId, 1);
    std::unique_ptr<RowData> fromCombo = accessor_.getRow(VectorBatchUtil::getComboId(batchId, 1));

    ASSERT_NE(first.get(), nullptr);
    ASSERT_NE(second.get(), nullptr);
    ASSERT_NE(fromCombo.get(), nullptr);
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(first->getArity(), 0);
    EXPECT_EQ(second->getArity(), 0);
    EXPECT_EQ(fromCombo->getArity(), 0);
}

// 后端查不到 batchId 时，getRow 应返回 nullptr，不能尝试反序列化空 ByteView。
TEST_F(VectorBatchStateAccessorTest, GetRowReturnsNullWhenBatchMissing)
{
    std::unique_ptr<RowData> row = accessor_.getRowForTest(404, 0);

    EXPECT_EQ(row.get(), nullptr);
}

// rowId 越过 VectorBatch 行数时，getRow 应返回 nullptr，避免把越界访问交给下层未定义行为处理。
TEST_F(VectorBatchStateAccessorTest, GetRowReturnsNullWhenRowIdOutOfRange)
{
    constexpr int64_t batchId = 7;
    accessor_.putSerializedBatch(batchId, serializeVectorBatch(2));

    std::unique_ptr<RowData> row = accessor_.getRowForTest(batchId, 2);

    EXPECT_EQ(row.get(), nullptr);
}

// deserializeBatch 对空、非 VECTOR_BATCH tag 和仅含 tag 的 payload 返回 nullptr，有效路径必须跳过 tag byte。
TEST_F(VectorBatchStateAccessorTest, DeserializeBatchReturnsNullForEmptyOrTagOnlyPayload)
{
    std::unique_ptr<omnistream::VectorBatch> empty = accessor_.deserializeBatchForTest(ByteView());
    EXPECT_EQ(empty.get(), nullptr);

    const int8_t wrongTag[] = {static_cast<int8_t>(StreamElementTag::TAG_WATERMARK), 0, 0, 0, 0};
    std::unique_ptr<omnistream::VectorBatch> invalidTag =
        accessor_.deserializeBatchForTest(ByteView::fromBuffer(wrongTag, 5));
    EXPECT_EQ(invalidTag.get(), nullptr);

    const int8_t tagOnly[] = {static_cast<int8_t>(StreamElementTag::VECTOR_BATCH)};
    std::unique_ptr<omnistream::VectorBatch> tagOnlyBatch =
        accessor_.deserializeBatchForTest(ByteView::fromBuffer(tagOnly, 1));
    EXPECT_EQ(tagOnlyBatch.get(), nullptr);

    std::vector<int8_t> bytes = serializeVectorBatch(1);
    std::unique_ptr<omnistream::VectorBatch> decoded =
        accessor_.deserializeBatchForTest(ByteView::fromBuffer(bytes.data(), bytes.size()));

    ASSERT_NE(decoded.get(), nullptr);
    EXPECT_EQ(decoded->GetRowCount(), 1);
}

// extractRow 应主动拒绝空 VectorBatch 和负 rowId，不能把负数转换成 size_t 后继续访问。
TEST_F(VectorBatchStateAccessorTest, ExtractRowReturnsNullForNullBatchOrNegativeRowId)
{
    auto batch = std::make_unique<omnistream::VectorBatch>(1);
    std::unique_ptr<RowData> nullBatchRow = accessor_.extractRowForTest(nullptr, 0);
    std::unique_ptr<RowData> negativeRow = accessor_.extractRowForTest(batch.get(), -1);

    EXPECT_EQ(nullBatchRow.get(), nullptr);
    EXPECT_EQ(negativeRow.get(), nullptr);
}

} // namespace

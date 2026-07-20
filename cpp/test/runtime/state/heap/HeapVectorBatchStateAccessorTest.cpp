#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "core/utils/ByteView.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/heap/HeapSnapshotStateData.h"
#include "runtime/state/heap/HeapVectorBatchStateAccessor.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace {

constexpr int64_t kBatchId = 42;
constexpr int32_t kSerializedVectorBatchBufferBytes = 64 * 1024;

std::vector<int8_t> bytes(std::initializer_list<int8_t> values)
{
    return std::vector<int8_t>(values);
}

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

    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(serialized.buffer), reinterpret_cast<int8_t*>(serialized.buffer + serialized.size));
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

HeapSnapshotStateData::SerializedEntry makeEntry(std::vector<int8_t> serializedValue)
{
    HeapSnapshotStateData::SerializedEntry entry;
    entry.serializedKey = bytes({0x01, 0x02});
    entry.serializedValue = std::move(serializedValue);
    return entry;
}

std::shared_ptr<HeapSnapshotStateData> makeStateDataWithBatch(
    omnistream::VectorBatchId batchId, std::vector<int8_t> serializedValue)
{
    auto stateData = std::make_shared<HeapSnapshotStateData>();
    stateData->addVectorBatchEntry(makeEntry(std::move(serializedValue)), batchId);
    return stateData;
}

VectorBatchAccessorOptions optionsWithCacheBytes(size_t maxDecodedBatchCacheBytes)
{
    VectorBatchAccessorOptions options;
    options.maxDecodedBatchCacheBytes = maxDecodedBatchCacheBytes;
    return options;
}

class HeapVectorBatchStateAccessorTest : public ::testing::Test {};

// getSerializedBatch 应返回指向 HeapSnapshotStateData frozen serializedValue 的零拷贝 ByteView。
TEST_F(HeapVectorBatchStateAccessorTest, GetSerializedBatchReturnsFrozenBytesView)
{
    std::shared_ptr<HeapSnapshotStateData> stateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(1));
    HeapVectorBatchStateAccessor accessor(stateData, optionsWithCacheBytes(64 * 1024));

    ByteView value;
    ASSERT_TRUE(accessor.getSerializedBatch(kBatchId, &value));

    const auto* entry = stateData->findVectorBatchEntry(kBatchId);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(value.data(), reinterpret_cast<const uint8_t*>(entry->serializedValue.data()));
    EXPECT_EQ(value.size(), entry->serializedValue.size());
}

// 缺失 batchId 或 Heap snapshot data 查询应返回 false，不能误读其他普通 entry 或空占位。
TEST_F(HeapVectorBatchStateAccessorTest, GetSerializedBatchReturnsFalseForMissingBatch)
{
    std::shared_ptr<HeapSnapshotStateData> stateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(1));
    HeapVectorBatchStateAccessor accessor(stateData, optionsWithCacheBytes(64 * 1024));
    HeapVectorBatchStateAccessor nullStateAccessor(nullptr, optionsWithCacheBytes(64 * 1024));

    ByteView value;

    EXPECT_FALSE(accessor.getSerializedBatch(404, &value));
    EXPECT_FALSE(nullStateAccessor.getSerializedBatch(kBatchId, &value));
}

// 输出 ByteView 指针为空时应返回 false，即使目标 batchId 存在也不能解引用空指针。
TEST_F(HeapVectorBatchStateAccessorTest, GetSerializedBatchReturnsFalseForNullOutputPointer)
{
    std::shared_ptr<HeapSnapshotStateData> stateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(1));
    HeapVectorBatchStateAccessor accessor(stateData, optionsWithCacheBytes(64 * 1024));

    EXPECT_FALSE(accessor.getSerializedBatch(kBatchId, nullptr));
}

// 有效 batchId/rowId 应从序列化 VectorBatch 中抽出调用方独占持有的 RowData。
TEST_F(HeapVectorBatchStateAccessorTest, GetRowReturnsOwnedRowData)
{
    std::shared_ptr<HeapSnapshotStateData> stateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(2));
    HeapVectorBatchStateAccessor accessor(stateData, optionsWithCacheBytes(64 * 1024));

    std::unique_ptr<RowData> first = accessor.getRow(kBatchId, 1);
    std::unique_ptr<RowData> second = accessor.getRow(kBatchId, 1);

    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(first->getArity(), 0);
    EXPECT_EQ(second->getArity(), 0);
}

// 缺失 batchId 的 getRow 应返回 nullptr，避免反序列化空 ByteView 或错误 entry。
TEST_F(HeapVectorBatchStateAccessorTest, GetRowReturnsNullForMissingBatch)
{
    std::shared_ptr<HeapSnapshotStateData> stateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(1));
    HeapVectorBatchStateAccessor accessor(stateData, optionsWithCacheBytes(64 * 1024));

    std::unique_ptr<RowData> row = accessor.getRow(404, 0);

    EXPECT_EQ(row, nullptr);
}

// rowId 越过 VectorBatch 行数时应返回 nullptr，不能把越界访问交给 VectorBatch 下层处理。
TEST_F(HeapVectorBatchStateAccessorTest, GetRowReturnsNullForRowIdOutOfRange)
{
    std::shared_ptr<HeapSnapshotStateData> stateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(2));
    HeapVectorBatchStateAccessor accessor(stateData, optionsWithCacheBytes(64 * 1024));

    std::unique_ptr<RowData> row = accessor.getRow(kBatchId, 2);

    EXPECT_EQ(row, nullptr);
}

// decoded cache 命中后，同 batchId 后续 getRow 不应再次依赖 HeapSnapshotStateData 的 serialized lookup 结果。
TEST_F(HeapVectorBatchStateAccessorTest, CacheHitDoesNotRequireSecondSerializedLookup)
{
    auto cachedStateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(1));
    HeapVectorBatchStateAccessor cachedAccessor(cachedStateData, optionsWithCacheBytes(64 * 1024));
    ASSERT_NE(cachedAccessor.getRow(kBatchId, 0), nullptr);

    cachedStateData->addVectorBatchEntry(
        makeEntry(bytes({static_cast<int8_t>(StreamElementTag::VECTOR_BATCH)})), kBatchId);
    std::unique_ptr<RowData> cachedRow = cachedAccessor.getRow(kBatchId, 0);

    auto uncachedStateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(1));
    HeapVectorBatchStateAccessor uncachedAccessor(uncachedStateData, optionsWithCacheBytes(0));
    ASSERT_NE(uncachedAccessor.getRow(kBatchId, 0), nullptr);
    uncachedStateData->addVectorBatchEntry(
        makeEntry(bytes({static_cast<int8_t>(StreamElementTag::VECTOR_BATCH)})), kBatchId);
    std::unique_ptr<RowData> uncachedRow = uncachedAccessor.getRow(kBatchId, 0);

    EXPECT_NE(cachedRow, nullptr);
    EXPECT_EQ(uncachedRow, nullptr);
}

// close 应清理 decoded cache 并释放 accessor 持有的 snapshot data 引用，后续 serialized 和 row 查询都应失败。
TEST_F(HeapVectorBatchStateAccessorTest, CloseClearsCacheAndReleasesSnapshotData)
{
    std::shared_ptr<HeapSnapshotStateData> stateData = makeStateDataWithBatch(kBatchId, serializeVectorBatch(1));
    std::weak_ptr<HeapSnapshotStateData> weakStateData = stateData;
    HeapVectorBatchStateAccessor accessor(stateData, optionsWithCacheBytes(64 * 1024));

    stateData.reset();
    ASSERT_FALSE(weakStateData.expired());
    ASSERT_NE(accessor.getRow(kBatchId, 0), nullptr);

    accessor.close();

    EXPECT_TRUE(weakStateData.expired());
    ByteView value;
    EXPECT_FALSE(accessor.getSerializedBatch(kBatchId, &value));
    EXPECT_EQ(accessor.getRow(kBatchId, 0), nullptr);
}

} // namespace

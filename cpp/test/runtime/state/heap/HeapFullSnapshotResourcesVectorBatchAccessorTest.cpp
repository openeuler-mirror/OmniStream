#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/heap/HeapFullSnapshotResources.h"
#include "runtime/state/heap/HeapSingleStateIterator.h"
#include "runtime/state/heap/HeapSnapshotStateData.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/rocksdb/iterator/SingleStateIterator.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace {

constexpr int64_t kBatchId = 42;
constexpr const char* kLogicalStateName = "deduplicate-state";
constexpr const char* kVectorBatchStateName = "deduplicate-statevb";
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

std::shared_ptr<HeapSnapshotStateData> makeStateDataWithPlainEntry(std::vector<int8_t> serializedValue)
{
    auto stateData = std::make_shared<HeapSnapshotStateData>();
    stateData->addEntry(makeEntry(std::move(serializedValue)));
    return stateData;
}

VectorBatchAccessorOptions optionsWithCacheBytes(size_t maxDecodedBatchCacheBytes)
{
    VectorBatchAccessorOptions options;
    options.maxDecodedBatchCacheBytes = maxDecodedBatchCacheBytes;
    return options;
}

class HeapFullSnapshotResourcesVectorBatchAccessorTest : public ::testing::Test {
protected:
    static std::unique_ptr<omnistream::VectorBatch> makeVectorBatch(int32_t rowCount)
    {
        auto batch = std::make_unique<omnistream::VectorBatch>(rowCount);
        for (int32_t row = 0; row < rowCount; ++row) {
            batch->setTimestamp(row, 1000 + row);
            batch->setRowKind(row, RowKind::INSERT);
        }
        return batch;
    }

    HeapFullSnapshotResources makeResources(
        std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>> snapshotStateDataByName)
    {
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfoSnapshots;
        std::vector<std::unique_ptr<SingleStateIterator>> stateIterators;
        TypeSerializer* keySerializer = nullptr;
        constexpr int keyGroupPrefixBytes = 1;

        return HeapFullSnapshotResources(
            std::move(metaInfoSnapshots),
            std::move(stateIterators),
            &keyGroupRange_,
            keySerializer,
            keyGroupPrefixBytes,
            std::move(snapshotStateDataByName));
    }

    static void expectReadableZeroArityRow(const std::shared_ptr<VectorBatchStateAccessor>& accessor)
    {
        ASSERT_NE(accessor, nullptr);

        std::unique_ptr<RowData> row = accessor->getRow(kBatchId, 0);

        ASSERT_NE(row, nullptr);
        EXPECT_EQ(row->getArity(), 0);
    }

private:
    KeyGroupRange keyGroupRange_{0, 0};
};

// 默认 VB iterator 只构造普通 vector entries，不创建 HeapSnapshotStateData，也不会维护 batchId 索引。
TEST_F(HeapFullSnapshotResourcesVectorBatchAccessorTest, DefaultVectorBatchIteratorDoesNotCaptureAccessorData)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 0);
    auto context = std::make_unique<InternalKeyContextImpl<uint32_t>>(keyGroupRange.get(), 1);
    {
        auto* metaInfo = new RegisteredKeyValueStateBackendMetaInfo(
            StateDescriptor::Type::VALUE, kVectorBatchStateName, new VoidNamespaceSerializer(), new LongSerializer());
        CopyOnWriteStateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*> table(
            context.get(), metaInfo, IntSerializer::INSTANCE);
        std::unique_ptr<omnistream::VectorBatch> batch = makeVectorBatch(1);
        table.put(kBatchId, 0, VoidNamespace(), batch.release());

        HeapSingleStateIterator<uint32_t, VoidNamespace, omnistream::VectorBatch*> iterator(
            &table,
            0,
            1,
            HeapSingleStateIterator<uint32_t, VoidNamespace, omnistream::VectorBatch*>::VbDataTag{},
            false);

        EXPECT_EQ(iterator.getSnapshotData(), nullptr);
        EXPECT_TRUE(iterator.isValid());
        EXPECT_EQ(iterator.getEntryCount(), 1);
        iterator.close();
        EXPECT_EQ(iterator.getEntryCount(), 0);
    }
}

// 显式启用 VB accessor 数据捕获时，VB iterator 才创建 HeapSnapshotStateData 并建立 batchId 索引。
TEST_F(HeapFullSnapshotResourcesVectorBatchAccessorTest, CapturedVectorBatchIteratorIndexesAccessorData)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 0);
    auto context = std::make_unique<InternalKeyContextImpl<uint32_t>>(keyGroupRange.get(), 1);
    {
        auto* metaInfo = new RegisteredKeyValueStateBackendMetaInfo(
            StateDescriptor::Type::VALUE, kVectorBatchStateName, new VoidNamespaceSerializer(), new LongSerializer());
        CopyOnWriteStateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*> table(
            context.get(), metaInfo, IntSerializer::INSTANCE);
        std::unique_ptr<omnistream::VectorBatch> batch = makeVectorBatch(1);
        table.put(kBatchId, 0, VoidNamespace(), batch.release());

        HeapSingleStateIterator<uint32_t, VoidNamespace, omnistream::VectorBatch*> iterator(
            &table,
            0,
            1,
            HeapSingleStateIterator<uint32_t, VoidNamespace, omnistream::VectorBatch*>::VbDataTag{},
            true);

        std::shared_ptr<HeapSnapshotStateData> snapshotData = iterator.getSnapshotData();
        ASSERT_NE(snapshotData, nullptr);
        EXPECT_TRUE(iterator.isValid());
        ASSERT_NE(snapshotData->findVectorBatchEntry(kBatchId), nullptr);
        iterator.close();
        EXPECT_NE(snapshotData->findVectorBatchEntry(kBatchId), nullptr);
    }
}

// logical state 名称应只映射到显式捕获的同名 vb 侧表数据，并创建可读取 frozen VectorBatch 行的 accessor。
TEST_F(HeapFullSnapshotResourcesVectorBatchAccessorTest, CreateVectorBatchStateAccessorMapsLogicalStateToVbData)
{
    std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>> snapshotStateDataByName;
    snapshotStateDataByName.emplace(kVectorBatchStateName, makeStateDataWithBatch(kBatchId, serializeVectorBatch(1)));
    HeapFullSnapshotResources resources = makeResources(std::move(snapshotStateDataByName));

    std::shared_ptr<VectorBatchStateAccessor> accessor =
        resources.createVectorBatchStateAccessor(kLogicalStateName, optionsWithCacheBytes(64 * 1024));

    expectReadableZeroArityRow(accessor);
}

// 普通 Heap snapshot 即使存在 logical state 的 frozen entry，也不能隐式构造带 batchId 索引的 VB accessor。
TEST_F(HeapFullSnapshotResourcesVectorBatchAccessorTest, CreateVectorBatchStateAccessorReturnsNullForPlainSnapshotData)
{
    std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>> snapshotStateDataByName;
    snapshotStateDataByName.emplace(kLogicalStateName, makeStateDataWithPlainEntry(bytes({0x11, 0x12})));
    HeapFullSnapshotResources resources = makeResources(std::move(snapshotStateDataByName));

    std::shared_ptr<VectorBatchStateAccessor> accessor =
        resources.createVectorBatchStateAccessor(kLogicalStateName, optionsWithCacheBytes(64 * 1024));

    EXPECT_EQ(accessor, nullptr);
}

// 调用方误传已经带 vb 后缀的名称时，factory 仍按 logical state 追加 vb，并因找不到 vbvb 侧表返回 nullptr。
TEST_F(HeapFullSnapshotResourcesVectorBatchAccessorTest, CreateVectorBatchStateAccessorReturnsNullForAlreadyVbStateName)
{
    std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>> snapshotStateDataByName;
    snapshotStateDataByName.emplace(kVectorBatchStateName, makeStateDataWithBatch(kBatchId, serializeVectorBatch(1)));
    HeapFullSnapshotResources resources = makeResources(std::move(snapshotStateDataByName));

    std::shared_ptr<VectorBatchStateAccessor> accessor =
        resources.createVectorBatchStateAccessor(kVectorBatchStateName, optionsWithCacheBytes(64 * 1024));

    EXPECT_EQ(accessor, nullptr);
}

// 缺失目标 vb 侧表时应返回 nullptr，避免创建只能在后续 getRow 才失败的空 accessor。
TEST_F(HeapFullSnapshotResourcesVectorBatchAccessorTest, CreateVectorBatchStateAccessorReturnsNullForMissingVbState)
{
    std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>> snapshotStateDataByName;
    snapshotStateDataByName.emplace(kVectorBatchStateName, makeStateDataWithBatch(kBatchId, serializeVectorBatch(1)));
    HeapFullSnapshotResources resources = makeResources(std::move(snapshotStateDataByName));

    std::shared_ptr<VectorBatchStateAccessor> accessor =
        resources.createVectorBatchStateAccessor("unknown-state", optionsWithCacheBytes(64 * 1024));

    EXPECT_EQ(accessor, nullptr);
}

// 显式启用 VB accessor 数据捕获时，即使 createKVStateIterator move 出普通 iterator，accessor 仍应读取 frozen bytes。
TEST_F(
    HeapFullSnapshotResourcesVectorBatchAccessorTest, AccessorReadsFrozenBytesAfterCreateKVStateIteratorMovesIterators)
{
    std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>> snapshotStateDataByName;
    snapshotStateDataByName.emplace(kVectorBatchStateName, makeStateDataWithBatch(kBatchId, serializeVectorBatch(1)));
    HeapFullSnapshotResources resources = makeResources(std::move(snapshotStateDataByName));

    ASSERT_NE(resources.createKVStateIterator(), nullptr);
    std::shared_ptr<VectorBatchStateAccessor> accessor =
        resources.createVectorBatchStateAccessor(kLogicalStateName, optionsWithCacheBytes(64 * 1024));

    expectReadableZeroArityRow(accessor);
}

} // namespace

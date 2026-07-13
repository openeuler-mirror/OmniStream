#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <utility>
#include <vector>

#include "runtime/state/heap/HeapSnapshotStateData.h"

namespace {

std::vector<int8_t> bytes(std::initializer_list<int8_t> values)
{
    return std::vector<int8_t>(values);
}

HeapSnapshotStateData::SerializedEntry makeEntry(std::vector<int8_t> key, std::vector<int8_t> value)
{
    HeapSnapshotStateData::SerializedEntry entry;
    entry.serializedKey = std::move(key);
    entry.serializedValue = std::move(value);
    return entry;
}

} // namespace

// addEntry 应保留已经序列化好的 key/value 字节，默认 vectorBatchId 为 -1，且不能写入 VB 索引。
TEST(HeapSnapshotStateDataTest, AddEntryPreservesSerializedKeyAndValue)
{
    HeapSnapshotStateData data;
    auto entry = makeEntry(bytes({0x01, 0x02}), bytes({0x11, 0x12, 0x13}));
    entry.vectorBatchId = 99;

    data.addEntry(std::move(entry));

    ASSERT_EQ(data.entries().size(), 1);
    EXPECT_EQ(data.entries()[0].serializedKey, bytes({0x01, 0x02}));
    EXPECT_EQ(data.entries()[0].serializedValue, bytes({0x11, 0x12, 0x13}));
    EXPECT_EQ(data.entries()[0].vectorBatchId, -1);
    EXPECT_EQ(data.findVectorBatchEntry(99), nullptr);
    EXPECT_EQ(data.findVectorBatchEntry(-1), nullptr);
}

// addVectorBatchEntry 应设置 vectorBatchId，并允许按 batchId 找回 entries 中的同一条记录。
TEST(HeapSnapshotStateDataTest, AddVectorBatchEntryIndexesBatchId)
{
    HeapSnapshotStateData data;

    data.addVectorBatchEntry(makeEntry(bytes({0x21}), bytes({0x31, 0x32})), 42);

    ASSERT_EQ(data.entries().size(), 1);
    const auto* found = data.findVectorBatchEntry(42);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found, &data.entries()[0]);
    EXPECT_EQ(found->serializedKey, bytes({0x21}));
    EXPECT_EQ(found->serializedValue, bytes({0x31, 0x32}));
    EXPECT_EQ(found->vectorBatchId, 42);
}

// 查询不存在的 batchId 应返回 nullptr，避免调用方误读其他普通 entry 或空占位。
TEST(HeapSnapshotStateDataTest, FindVectorBatchEntryReturnsNullForMissingBatch)
{
    HeapSnapshotStateData data;

    data.addEntry(makeEntry(bytes({0x01}), bytes({0x02})));
    data.addVectorBatchEntry(makeEntry(bytes({0x03}), bytes({0x04})), 7);

    EXPECT_EQ(data.findVectorBatchEntry(404), nullptr);
}

// 同一个 batchId 重复写入时，索引应解析到最后写入的 entry，匹配后写覆盖旧引用的语义。
TEST(HeapSnapshotStateDataTest, DuplicateBatchIdPointsToLatestEntry)
{
    HeapSnapshotStateData data;

    data.addVectorBatchEntry(makeEntry(bytes({0x01}), bytes({0x11})), 9);
    data.addVectorBatchEntry(makeEntry(bytes({0x02}), bytes({0x22})), 9);

    ASSERT_EQ(data.entries().size(), 2);
    const auto* found = data.findVectorBatchEntry(9);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found, &data.entries()[1]);
    EXPECT_EQ(found->serializedKey, bytes({0x02}));
    EXPECT_EQ(found->serializedValue, bytes({0x22}));
    EXPECT_EQ(found->vectorBatchId, 9);
}

// shared_ptr 持有的快照数据在局部 owner reset 后仍应保留 frozen bytes 和 VB 索引可读。
TEST(HeapSnapshotStateDataTest, EntriesRemainAccessibleThroughSharedPtrAfterLocalOwnerReset)
{
    std::shared_ptr<HeapSnapshotStateData> retained;

    {
        auto localOwner = std::make_shared<HeapSnapshotStateData>();
        localOwner->addEntry(makeEntry(bytes({0x0A}), bytes({0x0B})));
        localOwner->addVectorBatchEntry(makeEntry(bytes({0x0C}), bytes({0x0D, 0x0E})), 88);

        retained = localOwner;
        localOwner.reset();
    }

    ASSERT_NE(retained, nullptr);
    ASSERT_EQ(retained->entries().size(), 2);
    EXPECT_EQ(retained->entries()[0].serializedKey, bytes({0x0A}));
    EXPECT_EQ(retained->entries()[0].serializedValue, bytes({0x0B}));

    const auto* found = retained->findVectorBatchEntry(88);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found, &retained->entries()[1]);
    EXPECT_EQ(found->serializedKey, bytes({0x0C}));
    EXPECT_EQ(found->serializedValue, bytes({0x0D, 0x0E}));
    EXPECT_EQ(found->vectorBatchId, 88);
}

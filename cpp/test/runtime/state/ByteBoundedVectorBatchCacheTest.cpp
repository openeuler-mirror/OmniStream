#include <gtest/gtest.h>

#include <memory>
#include <utility>

#include "runtime/state/ByteBoundedVectorBatchCache.h"
#include "table/data/vectorbatch/VectorBatch.h"

// 缺失 batchId 查询应返回 nullptr，且不改变缓存字节数和容量上限。
TEST(ByteBoundedVectorBatchCacheTest, GetMissingReturnsNull)
{
    ByteBoundedVectorBatchCache cache(128);

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.currentBytes(), 0);
    EXPECT_EQ(cache.maxBytes(), 128);
}

// put 传入空 VectorBatch 时应返回 nullptr，且不能写入占位 entry。
TEST(ByteBoundedVectorBatchCacheTest, PutNullReturnsNull)
{
    ByteBoundedVectorBatchCache cache(128);

    EXPECT_EQ(cache.put(1, nullptr, 16), nullptr);
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.currentBytes(), 0);
}

// 成功 put 后应转移 unique_ptr 所有权，并返回缓存内部持有的同一个 VectorBatch 指针。
TEST(ByteBoundedVectorBatchCacheTest, PutAndGetReturnsSameOwnedBatch)
{
    ByteBoundedVectorBatchCache cache(128);
    auto batch = std::make_unique<omnistream::VectorBatch>(2);
    auto* rawBatch = batch.get();

    auto* cachedBatch = cache.put(7, std::move(batch), 64);

    ASSERT_EQ(cachedBatch, rawBatch);
    EXPECT_EQ(batch, nullptr);
    EXPECT_EQ(cache.get(7), rawBatch);
    EXPECT_EQ(cache.currentBytes(), 64);
    EXPECT_EQ(cache.maxBytes(), 128);
    EXPECT_TRUE(cache.canCache(64));
    EXPECT_TRUE(cache.canCache(128));
}

// 单个 batch 的序列化字节数超过 maxBytes 时应拒绝缓存，且不能 consume 调用方 unique_ptr。
TEST(ByteBoundedVectorBatchCacheTest, RejectsBatchWhenSerializedBytesExceedLimit)
{
    ByteBoundedVectorBatchCache cache(64);
    auto batch = std::make_unique<omnistream::VectorBatch>(1);
    auto* rawBatch = batch.get();

    EXPECT_FALSE(cache.canCache(65));
    EXPECT_EQ(cache.put(3, std::move(batch), 65), nullptr);
    EXPECT_EQ(batch.get(), rawBatch);
    EXPECT_EQ(cache.get(3), nullptr);
    EXPECT_EQ(cache.currentBytes(), 0);
    EXPECT_EQ(cache.maxBytes(), 64);
}

// maxBytes 为 0 时缓存应禁用，put 拒绝后仍保留调用方 unique_ptr 所有权。
TEST(ByteBoundedVectorBatchCacheTest, RejectsBatchWhenCacheDisabled)
{
    ByteBoundedVectorBatchCache cache(0);
    auto batch = std::make_unique<omnistream::VectorBatch>(1);
    auto* rawBatch = batch.get();

    EXPECT_FALSE(cache.canCache(0));
    EXPECT_FALSE(cache.canCache(1));
    EXPECT_EQ(cache.put(4, std::move(batch), 0), nullptr);
    EXPECT_EQ(batch.get(), rawBatch);
    EXPECT_EQ(cache.get(4), nullptr);
    EXPECT_EQ(cache.currentBytes(), 0);
    EXPECT_EQ(cache.maxBytes(), 0);
}

// 已有 batchId 的超重更新应删除旧 entry，并且不能 consume 新传入的 unique_ptr。
TEST(ByteBoundedVectorBatchCacheTest, RejectingOverweightUpdateDropsExistingEntry)
{
    ByteBoundedVectorBatchCache cache(64);

    ASSERT_NE(cache.put(5, std::make_unique<omnistream::VectorBatch>(1), 32), nullptr);
    ASSERT_NE(cache.get(5), nullptr);
    EXPECT_EQ(cache.currentBytes(), 32);

    auto newBatch = std::make_unique<omnistream::VectorBatch>(2);
    auto* rawNewBatch = newBatch.get();

    EXPECT_EQ(cache.put(5, std::move(newBatch), 65), nullptr);
    EXPECT_EQ(newBatch.get(), rawNewBatch);
    EXPECT_EQ(cache.get(5), nullptr);
    EXPECT_EQ(cache.currentBytes(), 0);
    EXPECT_EQ(cache.maxBytes(), 64);
}

// clear 应删除所有 cached batch，归零当前字节数，并保留原始 maxBytes。
TEST(ByteBoundedVectorBatchCacheTest, ClearDropsAllEntriesAndWeight)
{
    ByteBoundedVectorBatchCache cache(256);

    ASSERT_NE(cache.put(1, std::make_unique<omnistream::VectorBatch>(1), 64), nullptr);
    ASSERT_NE(cache.put(2, std::make_unique<omnistream::VectorBatch>(2), 96), nullptr);
    EXPECT_EQ(cache.currentBytes(), 160);

    cache.clear();

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_EQ(cache.currentBytes(), 0);
    EXPECT_EQ(cache.maxBytes(), 256);
    EXPECT_TRUE(cache.canCache(256));
}

// 具有相同低 16 位的结构化 batchId 应独立寻址，并继续遵循 get 提升后的 LRU 淘汰顺序。
TEST(ByteBoundedVectorBatchCacheTest, StructuredBatchIdsRemainIndependentUnderLruEviction)
{
    constexpr omnistream::VectorBatchId batchId1 = 0x0001000000010000ULL;
    constexpr omnistream::VectorBatchId batchId2 = 0x0001000000020000ULL;
    constexpr omnistream::VectorBatchId batchId3 = 0x0002000000010000ULL;
    constexpr omnistream::VectorBatchId batchId4 = 0x0002000000020000ULL;
    ByteBoundedVectorBatchCache cache(192);

    auto batch1 = std::make_unique<omnistream::VectorBatch>(1);
    auto batch2 = std::make_unique<omnistream::VectorBatch>(2);
    auto batch3 = std::make_unique<omnistream::VectorBatch>(3);
    auto batch4 = std::make_unique<omnistream::VectorBatch>(4);
    auto* rawBatch1 = batch1.get();
    auto* rawBatch2 = batch2.get();
    auto* rawBatch3 = batch3.get();
    auto* rawBatch4 = batch4.get();

    ASSERT_EQ(cache.put(batchId1, std::move(batch1), 64), rawBatch1);
    ASSERT_EQ(cache.put(batchId2, std::move(batch2), 64), rawBatch2);
    ASSERT_EQ(cache.put(batchId3, std::move(batch3), 64), rawBatch3);
    ASSERT_EQ(cache.get(batchId1), rawBatch1);

    ASSERT_EQ(cache.put(batchId4, std::move(batch4), 64), rawBatch4);

    EXPECT_EQ(cache.get(batchId1), rawBatch1);
    EXPECT_EQ(cache.get(batchId2), nullptr);
    EXPECT_EQ(cache.get(batchId3), rawBatch3);
    EXPECT_EQ(cache.get(batchId4), rawBatch4);
    EXPECT_EQ(cache.currentBytes(), 192);
}

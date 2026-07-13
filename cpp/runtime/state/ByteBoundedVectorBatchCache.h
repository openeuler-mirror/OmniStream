/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PSL v2 for more details.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "runtime/state/cache/WeightedLruCache.h"
#include "table/data/vectorbatch/VectorBatch.h"

/*
 * 按序列化字节数限制容量的 decoded VectorBatch 缓存。
 *
 * 使用方式：调用 put(batchId, batch, bytes) 将已经反序列化出来的 VectorBatch
 * 交给缓存；bytes 必须是该 batch 对应的 serialized VectorBatch 字节数，缓存将它作为
 * LRU weight，而不是按 decoded 对象的内存占用估算。后续 accessor 可用 get(batchId)
 * 复用已经 decoded 的 batch，减少重复反序列化。
 *
 * 关键语义：本类只持有 decoded VectorBatch，不负责 RocksDB/Heap 读取或序列化逻辑。
 * put 成功后 unique_ptr 所有权转移到 cache；拒绝路径不会 consume 调用方传入的
 * unique_ptr。返回的 raw pointer 生命周期由 cache entry 决定，entry 被后续 put 淘汰、
 * 同 key 覆盖、clear 或 cache 析构后即失效。maxBytes 为 0 或 bytes 超过 maxBytes
 * 时不缓存；如果拒绝的是已有 batchId 的更新，旧 entry 会按过期数据删除。
 */
class ByteBoundedVectorBatchCache {
public:
    explicit ByteBoundedVectorBatchCache(size_t maxBytes) : cache_(maxBytes)
    {
    }

    ByteBoundedVectorBatchCache(const ByteBoundedVectorBatchCache&) = delete;
    ByteBoundedVectorBatchCache& operator=(const ByteBoundedVectorBatchCache&) = delete;

    omnistream::VectorBatch* get(int64_t batchId)
    {
        auto* cachedBatch = cache_.get(batchId);
        if (cachedBatch == nullptr) {
            return nullptr;
        }
        return cachedBatch->get();
    }

    omnistream::VectorBatch* put(int64_t batchId, std::unique_ptr<omnistream::VectorBatch>&& batch, size_t bytes)
    {
        if (batch == nullptr) {
            return nullptr;
        }

        if (!canCache(bytes)) {
            cache_.erase(batchId);
            return nullptr;
        }

        auto* cachedBatch = cache_.put(batchId, std::move(batch), bytes);
        if (cachedBatch == nullptr) {
            return nullptr;
        }
        return cachedBatch->get();
    }

    bool canCache(size_t bytes) const
    {
        return maxBytes() != 0 && bytes <= maxBytes();
    }

    void clear()
    {
        cache_.clear();
    }

    size_t currentBytes() const
    {
        return cache_.currentWeight();
    }

    size_t maxBytes() const
    {
        return cache_.maxWeight();
    }

private:
    WeightedLruCache<int64_t, std::unique_ptr<omnistream::VectorBatch>> cache_;
};

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

#include "core/utils/ByteView.h"
#include "data/util/VectorBatchUtil.h"
#include "runtime/state/ByteBoundedVectorBatchCache.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/utils/VectorBatchDeserializationUtils.h"

struct VectorBatchAccessorOptions {
    size_t maxDecodedBatchCacheBytes = 64 * 1024 * 1024;
};

/**
 * VectorBatchStateAccessor 是兼容 savepoint 转换阶段访问序列化 VectorBatch 侧表的基类。
 *
 * 子类负责按后端快照语义实现 getSerializedBatch() 和 close()：RocksDB/Heap
 * 等后端只暴露快照内稳定的 batchId 查询能力，基类提供 comboId 拆解、decoded VectorBatch 缓存、
 * payload 反序列化和行抽取的共享逻辑。
 * getSerializedBatch() 写出的 ByteView 不拥有内存，底层 buffer 必须至少在本次 getRow 调用内保持有效。
 * getRow() 返回的 RowData 由调用方独占持有；反序列化得到的 VectorBatch 可按配置缓存在 accessor
 * 内复用，调用方不能假设 cache entry 在后续 put、clear、close 或析构后仍然存在。accessor 生命周期由创建它的
 * snapshot resources 或 save-state context 管理，close() 用于释放后端迭代器、缓存或快照引用。
 */
class VectorBatchStateAccessor {
public:
    explicit VectorBatchStateAccessor(VectorBatchAccessorOptions options = VectorBatchAccessorOptions())
        : decodedBatchCache_(options.maxDecodedBatchCacheBytes)
    {
    }

    virtual ~VectorBatchStateAccessor() = default;

    virtual bool getSerializedBatch(omnistream::VectorBatchId batchId, ByteView* value) = 0;

    std::unique_ptr<RowData> getRow(omnistream::ComboId comboId);

    virtual std::unique_ptr<RowData> getRow(omnistream::VectorBatchId batchId, int32_t rowId)
    {
        return getRowWithVectorBatchCaching(batchId, rowId);
    }

    virtual void close() = 0;

protected:
    std::unique_ptr<RowData> getRowWithVectorBatchCaching(omnistream::VectorBatchId batchId, int32_t rowId)
    {
        omnistream::VectorBatch* cachedBatch = decodedBatchCache_.get(batchId);
        if (cachedBatch != nullptr) {
            return extractRow(cachedBatch, rowId);
        }

        ByteView serializedBytes;
        if (!getSerializedBatch(batchId, &serializedBytes)) {
            return nullptr;
        }

        std::unique_ptr<omnistream::VectorBatch> decodedBatch = deserializeBatch(serializedBytes);
        if (decodedBatch == nullptr) {
            return nullptr;
        }

        if (decodedBatchCache_.canCache(serializedBytes.size())) {
            cachedBatch = decodedBatchCache_.put(batchId, std::move(decodedBatch), serializedBytes.size());
            return extractRow(cachedBatch, rowId);
        }

        return extractRow(decodedBatch.get(), rowId);
    }

    void clearDecodedBatchCache()
    {
        decodedBatchCache_.clear();
    }

    static std::unique_ptr<omnistream::VectorBatch> deserializeBatch(ByteView serializedValue)
    {
        constexpr auto kVectorBatchSkipSize = sizeof(int8_t);
        constexpr auto kVectorBatchHeadSize = 3 * sizeof(int32_t);
        if (serializedValue.data() == nullptr || serializedValue.size() < kVectorBatchSkipSize + kVectorBatchHeadSize) {
            return nullptr;
        }
        if (serializedValue[0] != static_cast<uint8_t>(StreamElementTag::VECTOR_BATCH)) {
            return nullptr;
        }

        uint8_t* payload = const_cast<uint8_t*>(serializedValue.data() + sizeof(int8_t));
        return std::unique_ptr<omnistream::VectorBatch>(
            omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(payload));
    }

    static std::unique_ptr<RowData> extractRow(omnistream::VectorBatch* batch, int32_t rowId)
    {
        if (batch == nullptr || rowId < 0 || rowId >= batch->GetRowCount()) {
            return nullptr;
        }
        return std::unique_ptr<RowData>(batch->extractRowData(rowId));
    }

private:
    ByteBoundedVectorBatchCache decodedBatchCache_;
};

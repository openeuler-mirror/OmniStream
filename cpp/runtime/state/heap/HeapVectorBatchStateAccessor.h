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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "core/utils/ByteView.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/heap/HeapSnapshotStateData.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

/**
 * HeapVectorBatchStateAccessor 提供 Heap snapshot frozen data 上的 VectorBatch 侧表访问能力。
 *
 * 使用方式：构造时传入 HeapSnapshotStateData shared_ptr 和缓存选项，调用 getSerializedBatch()
 * 按 batchId 获取 frozen serializedValue 的零拷贝 ByteView，或调用 getRow(batchId,rowId) /
 * getRow(comboId) 通过基类缓存/反序列化 VectorBatch，并抽取调用方独占持有的 RowData。
 *
 * 所有权语义：本类不拥有 serialized bytes 的拷贝，只通过 shared_ptr 延长 HeapSnapshotStateData
 * 的生命周期；snapshot/frozen data 由 HeapSnapshotStateData 持有。decoded VectorBatch
 * 可被 ByteBoundedVectorBatchCache 持有并在 accessor 内复用，close() 会清空缓存并释放
 * frozen data 引用。
 *
 * 生命周期约束：getSerializedBatch() 返回的 ByteView 指向 HeapSnapshotStateData 内部
 * serializedValue vector，只保证到下一次 getSerializedBatch() lookup 或 close() 前有效，
 * 调用方不能长期保存。getRow() 返回的 RowData 由调用方持有；它不能假设底层 decoded
 * VectorBatch 或 ByteView 在后续 cache 淘汰、close() 或 accessor 析构后仍然存在。
 */
class HeapVectorBatchStateAccessor : public VectorBatchStateAccessor {
public:
    using VectorBatchStateAccessor::getRow;

    HeapVectorBatchStateAccessor(std::shared_ptr<HeapSnapshotStateData> stateData, VectorBatchAccessorOptions options)
        : VectorBatchStateAccessor(options),
          stateData_(std::move(stateData))
    {
    }

    bool getSerializedBatch(int64_t batchId, ByteView* value) override
    {
        if (value == nullptr || stateData_ == nullptr) {
            return false;
        }

        const HeapSnapshotStateData::SerializedEntry* entry = stateData_->findVectorBatchEntry(batchId);
        if (entry == nullptr) {
            return false;
        }

        const std::vector<int8_t>& serializedValue = entry->serializedValue;
        *value = ByteView::fromBuffer(serializedValue.data(), serializedValue.size());
        return true;
    }

    void close() override
    {
        clearDecodedBatchCache();
        stateData_.reset();
    }

private:
    std::shared_ptr<HeapSnapshotStateData> stateData_;
};

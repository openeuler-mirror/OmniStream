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
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/snapshot.h>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

/**
 * RocksDBVectorBatchStateAccessor 提供 RocksDB snapshot 上的 VectorBatch 侧表 point lookup 能力。
 *
 * 使用方式：构造时传入 RocksDB DB、VB column family、当前 full snapshot 持有的 snapshot、
 * key-group prefix 宽度、VB side table 使用的 keyGroup，以及 decoded cache 配置。调用
 * getSerializedBatch(batchId, value) 会按运行态 VB key 格式读取当前 snapshot 中的 serialized
 * VectorBatch；调用 getRow(batchId,rowId) 或 getRow(comboId) 会通过基类缓存/反序列化并返回调用方独占持有的
 * RowData。
 *
 * 生命周期语义：本类不拥有 DB、column family 或 RocksDB snapshot，snapshot 释放仍由
 * RocksDBFullSnapshotResources 负责。getSerializedBatch() 返回的 ByteView 指向内部
 * PinnableSlice，只保证到下一次 getSerializedBatch() 或 close() 前有效；getRow() 返回的
 * RowData 由调用方持有。close() 只释放本 accessor 的 pinned slice 和 decoded cache，不释放
 * RocksDB snapshot。
 */
class RocksDBVectorBatchStateAccessor : public VectorBatchStateAccessor {
public:
    using VectorBatchStateAccessor::getRow;

    RocksDBVectorBatchStateAccessor(
        rocksdb::DB* db,
        rocksdb::ColumnFamilyHandle* columnFamily,
        const rocksdb::Snapshot* snapshot,
        int keyGroupPrefixBytes,
        int keyGroup,
        VectorBatchAccessorOptions options)
        : VectorBatchStateAccessor(options),
          db_(db),
          columnFamily_(columnFamily),
          snapshot_(snapshot),
          keyGroupPrefixBytes_(keyGroupPrefixBytes),
          keyGroup_(keyGroup)
    {
        if (db_ == nullptr) {
            throw std::runtime_error("RocksDBVectorBatchStateAccessor requires non-null DB.");
        }
        if (columnFamily_ == nullptr) {
            throw std::runtime_error("RocksDBVectorBatchStateAccessor requires non-null column family.");
        }
        if (snapshot_ == nullptr) {
            throw std::runtime_error("RocksDBVectorBatchStateAccessor requires non-null snapshot.");
        }
        if (keyGroupPrefixBytes_ <= 0) {
            throw std::runtime_error("RocksDBVectorBatchStateAccessor requires positive key-group prefix bytes.");
        }
    }

    bool getSerializedBatch(omnistream::VectorBatchId batchId, ByteView* value) override
    {
        if (value == nullptr) {
            return false;
        }
        *value = ByteView();
        if (closed_) {
            return false;
        }

        lastValue_.Reset();
        std::vector<int8_t> key = serializeVectorBatchKey(batchId);
        rocksdb::ReadOptions readOptions;
        readOptions.snapshot = snapshot_;

        rocksdb::Status status = readSerializedBatch(readOptions, key);
        if (status.IsNotFound()) {
            return false;
        }
        if (!status.ok()) {
            throw std::runtime_error(
                "RocksDBVectorBatchStateAccessor::getSerializedBatch failed: " + status.ToString());
        }

        *value = ByteView::fromBuffer(lastValue_.data(), lastValue_.size());
        return true;
    }

    void close() override
    {
        lastValue_.Reset();
        clearDecodedBatchCache();
        closed_ = true;
    }

protected:
    virtual rocksdb::Status readSerializedBatch(const rocksdb::ReadOptions& readOptions, const std::vector<int8_t>& key)
    {
        return db_->Get(
            readOptions,
            columnFamily_,
            rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
            &lastValue_);
    }

private:
    std::vector<int8_t> serializeVectorBatchKey(omnistream::VectorBatchId batchId) const
    {
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer outputSerializer;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        CompositeKeySerializationUtils::writeKeyGroup(keyGroup_, keyGroupPrefixBytes_, outputSerializer);
        LongSerializer longSerializer;
        longSerializer.serialize(&batchId, outputSerializer);

        return std::vector<int8_t>(
            reinterpret_cast<int8_t*>(outputSerializer.getData()),
            reinterpret_cast<int8_t*>(outputSerializer.getData() + outputSerializer.getPosition()));
    }

    rocksdb::DB* db_;
    rocksdb::ColumnFamilyHandle* columnFamily_;
    const rocksdb::Snapshot* snapshot_;
    int keyGroupPrefixBytes_;
    int keyGroup_;
    rocksdb::PinnableSlice lastValue_;
    bool closed_ = false;
};

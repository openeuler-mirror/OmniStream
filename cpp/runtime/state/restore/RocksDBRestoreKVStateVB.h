/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <rocksdb/slice.h>

#include "core/memory/DataOutputSerializer.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/restore/RocksDBRestoreKVState.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"

namespace omnistream {

template <typename K>
class RocksDBRestoreKVStateVB : public RestoreKVStateVB {
public:
    RocksDBRestoreKVStateVB(
        RocksDBWriterContext& ctx,
        rocksdb::ColumnFamilyHandle* mainCF,
        rocksdb::ColumnFamilyHandle* vbCF,
        int kvStateId,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes,
        int vectorBatchSize);

protected:
    int64_t appendRowToVectorBatch(const RowDataView& row) override;
    void flushVectorBatchIfNotEmpty() override;
    void flushMainWriter() override;
    void discardVectorBatch() override;
    void discardMainWriter() override;

    void writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t value) override;
    void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value) override;

    RocksDBWriterContext& ctx_;
    rocksdb::ColumnFamilyHandle* mainCF_;
    rocksdb::ColumnFamilyHandle* vbCF_;
    int kvStateId_;
    std::vector<omniruntime::type::DataTypeId> columnTypes_;
    int vectorBatchSize_;
    VbBatchState vbState_;
    std::unique_ptr<RocksDBWriteBatchWrapper> mainWriter_;
};

// ============================================================================
// 实现
// ============================================================================

template <typename K>
RocksDBRestoreKVStateVB<K>::RocksDBRestoreKVStateVB(
    RocksDBWriterContext& ctx,
    rocksdb::ColumnFamilyHandle* mainCF,
    rocksdb::ColumnFamilyHandle* vbCF,
    int kvStateId,
    const std::vector<omniruntime::type::DataTypeId>& columnTypes,
    int vectorBatchSize)
    : ctx_(ctx),
      mainCF_(mainCF),
      vbCF_(vbCF),
      kvStateId_(kvStateId),
      columnTypes_(columnTypes),
      vectorBatchSize_(vectorBatchSize)
{
}

template <typename K>
void RocksDBRestoreKVStateVB<K>::writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t value)
{
    rocksdb::Slice keySlice(reinterpret_cast<const char*>(keyBytes.data()), keyBytes.size());

    DataOutputSerializer valOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    valOutputSerializer.setBackendBuffer(&outputBufferStatus);

    LongSerializer longSerializer;
    longSerializer.serialize(&value, valOutputSerializer);

    rocksdb::Slice valueSlice(
        reinterpret_cast<const char*>(valOutputSerializer.getData()), (int32_t)(valOutputSerializer.getPosition()));

    if (mainWriter_ == nullptr) {
        mainWriter_ = std::make_unique<RocksDBWriteBatchWrapper>(ctx_.db, ctx_.writeBatchSize);
    }
    mainWriter_->Put(mainCF_, keySlice, valueSlice);
    if (ctx_.mainEntryCount) (*ctx_.mainEntryCount)++;
    INFO_RELEASE(
        "RocksDBRestoreKVStateVB: writeLongEntry kvStateId=" << kvStateId_ << ", mainEntry#" << *ctx_.mainEntryCount
                                                             << ", keySize=" << keyBytes.size()
                                                             << ", comboId=" << value);
}

template <typename K>
void RocksDBRestoreKVStateVB<K>::writeBytesEntry(const std::vector<int8_t>& /*keyBytes*/, ByteView /*value*/)
{
    throw std::runtime_error("RocksDBRestoreKVStateVB: writeBytesEntry not supported, use writeRowData instead");
}

template <typename K>
int64_t RocksDBRestoreKVStateVB<K>::appendRowToVectorBatch(const RowDataView& row)
{
    if (row.valueBytes == nullptr || row.columnTypes == nullptr) {
        throw std::runtime_error("RocksDBRestoreKVStateVB: RowDataView has null valueBytes or columnTypes");
    }
    int64_t comboId =
        VectorBatchRestoreUtil::appendRowToVectorBatch(vbState_, *row.valueBytes, columnTypes_, vectorBatchSize_);
    if (comboId < 0) {
        throw std::runtime_error("RocksDBRestoreKVStateVB: appendRowToVectorBatch failed");
    }
    if (vbState_.currentRowId >= vectorBatchSize_) {
        flushVectorBatchIfNotEmpty();
    }
    INFO_RELEASE(
        "RocksDBRestoreKVStateVB: appendRowToVB kvStateId=" << kvStateId_ << ", batchId=" << vbState_.currentBatchId
                                                            << ", rowId=" << vbState_.currentRowId << ", comboId="
                                                            << comboId << ", valueSize=" << row.valueBytes->size());
    return comboId;
}

template <typename K>
void RocksDBRestoreKVStateVB<K>::flushVectorBatchIfNotEmpty()
{
    if (vbState_.currentBatch == nullptr) {
        return;
    }

    INFO_RELEASE(
        "RocksDBRestoreKVStateVB: flushVectorBatch - kvStateId="
        << kvStateId_ << ", batchId=" << vbState_.currentBatchId << ", rows=" << vbState_.currentRowId);

    int32_t actualRowCnt = vbState_.currentRowId;
    VectorBatch* sliceForSerialization = nullptr;
    VectorBatch* batchToSerialize = vbState_.currentBatch;
    if (actualRowCnt < VB_RESTORE_BATCH_SIZE) {
        sliceForSerialization = VectorBatchRestoreUtil::sliceVectorBatch(vbState_.currentBatch, 0, actualRowCnt);
        if (sliceForSerialization != nullptr) {
            batchToSerialize = sliceForSerialization;
        }
    }

    int32_t bufferSize = VectorBatchRestoreUtil::calculateVbSerializableSize(batchToSerialize);
    std::vector<uint8_t> buf(bufferSize);
    auto* buffer = buf.data();
    auto serializedBatchInfo = VectorBatchRestoreUtil::serializeVbBatch(batchToSerialize, bufferSize, buffer);

    if (serializedBatchInfo.buffer == nullptr) {
        INFO_RELEASE(
            "RocksDBRestoreKVStateVB: ERROR - VB serialization failed kvStateId=" << kvStateId_ << ", batchId="
                                                                                  << vbState_.currentBatchId);
        delete vbState_.currentBatch;
        vbState_.currentBatch = nullptr;
        if (sliceForSerialization != nullptr) {
            delete sliceForSerialization;
        }
        return;
    }

    DataOutputSerializer keyTarget(ctx_.keyGroupPrefixBytes + 8);
    CompositeKeySerializationUtils::writeKeyGroup(ctx_.startKeyGroup, ctx_.keyGroupPrefixBytes, keyTarget);
    keyTarget.writeLong(static_cast<int64_t>(vbState_.currentBatchId));

    std::vector<int8_t> vbKey(keyTarget.getData(), keyTarget.getData() + keyTarget.getPosition());

    rocksdb::Slice keySlice(reinterpret_cast<const char*>(vbKey.data()), vbKey.size());
    rocksdb::Slice valueSlice(reinterpret_cast<const char*>(serializedBatchInfo.buffer), serializedBatchInfo.size);

    {
        RocksDBWriteBatchWrapper vbWriter(ctx_.db, ctx_.writeBatchSize);
        vbWriter.Put(vbCF_, keySlice, valueSlice);
        vbWriter.Flush();
    }

    if (mainWriter_ != nullptr) {
        mainWriter_->Flush();
    }

    delete vbState_.currentBatch;
    vbState_.currentBatch = nullptr;

    if (sliceForSerialization != nullptr) {
        delete sliceForSerialization;
    }

    vbState_.currentBatchId++;
    vbState_.currentRowId = 0;
    if (ctx_.vbBatchCount) (*ctx_.vbBatchCount)++;
    INFO_RELEASE(
        "RocksDBRestoreKVStateVB: flushed VB kvStateId=" << kvStateId_ << ", batchId=" << vbState_.currentBatchId - 1
                                                         << ", rows=" << actualRowCnt
                                                         << ", totalVbBatches=" << *ctx_.vbBatchCount);
}

template <typename K>
void RocksDBRestoreKVStateVB<K>::flushMainWriter()
{
    if (mainWriter_ != nullptr) {
        mainWriter_->Flush();
    }
}

template <typename K>
void RocksDBRestoreKVStateVB<K>::discardVectorBatch()
{
    if (vbState_.currentBatch != nullptr) {
        delete vbState_.currentBatch;
        vbState_.currentBatch = nullptr;
    }
}

template <typename K>
void RocksDBRestoreKVStateVB<K>::discardMainWriter()
{
    mainWriter_.reset();
}
} // namespace omnistream

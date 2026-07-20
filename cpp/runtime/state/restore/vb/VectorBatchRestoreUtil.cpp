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

#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"

#include <cstring>
#include <string_view>

#include "common.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/VoidSerializer.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace omnistream {

void VectorBatchRestoreUtil::populateVectorBatchFromRow(
    VectorBatch* batch, const std::vector<omniruntime::type::DataTypeId>& columnTypes, ::BinaryRowData* row, int rowPos)
{
    int numFields = static_cast<int>(columnTypes.size());
    for (int col = 0; col < numFields; col++) {
        if (row->isNullAt(col)) {
            auto* baseVec = batch->Get(col);
            if (baseVec != nullptr) {
                baseVec->SetNull(rowPos);
            }
            continue;
        }

        switch (columnTypes[col]) {
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_DATE32:
            case omniruntime::type::DataTypeId::OMNI_SHORT: {
                int32_t val = *(row->getInt(col));
                batch->template SetValueAt<int32_t>(col, rowPos, val);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_DATE64:
            case omniruntime::type::DataTypeId::OMNI_TIME64:
            case omniruntime::type::DataTypeId::OMNI_DECIMAL64: {
                int64_t val = *(row->getLong(col));
                batch->template SetValueAt<int64_t>(col, rowPos, val);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                int64_t bits = *(row->getLong(col));
                double val;
                std::memcpy(&val, &bits, sizeof(double));
                batch->template SetValueAt<double>(col, rowPos, val);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                bool val = *(row->getBool(col));
                batch->template SetValueAt<bool>(col, rowPos, val);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                std::string_view sv = row->getStringView(col);
                auto* baseVec = batch->Get(col);
                if (baseVec != nullptr && baseVec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    auto* strVec = reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(baseVec);
                    strVec->SetValue(rowPos, sv);
                }
                break;
            }
            default: break;
        }
    }
}

int64_t VectorBatchRestoreUtil::appendRowToVectorBatch(
    VbBatchState& vbState,
    const std::vector<int8_t>& valueBytes,
    const std::vector<omniruntime::type::DataTypeId>& columnTypes,
    int batchSize)
{
    if (valueBytes.empty()) {
        INFO_RELEASE("VectorBatchRestoreUtil: empty value bytes, cannot parse RowData");
        return -1;
    }

    int numFields = static_cast<int>(columnTypes.size());
    if (numFields == 0) {
        INFO_RELEASE("VectorBatchRestoreUtil: no column types, value has " << valueBytes.size() << " bytes");
        return -1;
    }

    DataInputDeserializer valInput(
        reinterpret_cast<const uint8_t*>(valueBytes.data()), static_cast<int>(valueBytes.size()), 0);

    BinaryRowDataSerializer rowDataSer(numFields);
    void* rawRow = rowDataSer.deserialize(valInput);
    BinaryRowData* row = static_cast<BinaryRowData*>(rawRow);

    if (row == nullptr) {
        INFO_RELEASE("VectorBatchRestoreUtil: deserialized null row");
        return -1;
    }

    if (vbState.currentBatch == nullptr) {
        INFO_RELEASE(
            "VectorBatchRestoreUtil: creating new VectorBatch, batchId=" << vbState.currentBatchId << ", numFields="
                                                                         << numFields << ", batchSize=" << batchSize);

        try {
            vbState.currentBatch = VectorBatch::CreateVectorBatch(batchSize, columnTypes);
        } catch (const std::runtime_error& e) {
            INFO_RELEASE(
                "VectorBatchRestoreUtil: CreateVectorBatch failed (" << e.what() << "), building batch manually");

            vbState.currentBatch = new VectorBatch(batchSize);
            for (size_t i = 0; i < columnTypes.size(); i++) {
                switch (columnTypes[i]) {
                    case omniruntime::type::DataTypeId::OMNI_INT:
                    case omniruntime::type::DataTypeId::OMNI_DATE32:
                    case omniruntime::type::DataTypeId::OMNI_SHORT:
                        vbState.currentBatch->Append(new omniruntime::vec::Vector<int32_t>(batchSize));
                        break;
                    case omniruntime::type::DataTypeId::OMNI_LONG:
                    case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                    case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                    case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    case omniruntime::type::DataTypeId::OMNI_DATE64:
                    case omniruntime::type::DataTypeId::OMNI_TIME64:
                    case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
                        vbState.currentBatch->Append(new omniruntime::vec::Vector<int64_t>(batchSize));
                        break;
                    case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                        vbState.currentBatch->Append(new omniruntime::vec::Vector<double>(batchSize));
                        break;
                    case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                        vbState.currentBatch->Append(new omniruntime::vec::Vector<bool>(batchSize));
                        break;
                    case omniruntime::type::DataTypeId::OMNI_CHAR:
                    case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                        vbState.currentBatch->Append(
                            new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                                batchSize));
                        break;
                    default: vbState.currentBatch->Append(new omniruntime::vec::Vector<int64_t>(batchSize)); break;
                }
            }
        }
    }

    populateVectorBatchFromRow(vbState.currentBatch, columnTypes, row, vbState.currentRowId);

    int64_t comboId = VectorBatchUtil::getComboId(vbState.currentBatchId, vbState.currentRowId);
    vbState.currentRowId++;
    return comboId;
}

StateMetaInfoSnapshot VectorBatchRestoreUtil::buildOmniMainMetaInfo(
    const StateMetaInfoSnapshot& flinkMetaInfo, TypeSerializer* valueSerializer)
{
    TypeSerializer* nsSerializer = flinkMetaInfo.getTypeSerializer("NAMESPACE_SERIALIZER");
    if (nsSerializer == nullptr) {
        nsSerializer = VoidSerializer::INSTANCE;
    }

    auto stateTypeString = flinkMetaInfo.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
    StateDescriptor::Type stateType = StateDescriptor::StringToType(stateTypeString);
    if (stateType == StateDescriptor::Type::UNKNOWN) {
        stateType = StateDescriptor::Type::VALUE;
    }

    RegisteredKeyValueStateBackendMetaInfo correctedMetaInfo(
        stateType, flinkMetaInfo.getName(), nsSerializer, valueSerializer);

    return *correctedMetaInfo.snapshot();
}

VectorBatch* VectorBatchRestoreUtil::sliceVectorBatch(VectorBatch* batch, int startRow, int endRow)
{
    return VectorBatchUtil::sliceVectorBatch(batch, startRow, endRow);
}

int32_t VectorBatchRestoreUtil::calculateVbSerializableSize(VectorBatch* batch)
{
    return VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(batch);
}

SerializedVbBatchInfo VectorBatchRestoreUtil::serializeVbBatch(VectorBatch* batch, int32_t bufferSize, uint8_t* buffer)
{
    auto info = VectorBatchSerializationUtils::serializeVectorBatch(batch, bufferSize, buffer);
    return {info.buffer, info.size};
}

} // namespace omnistream

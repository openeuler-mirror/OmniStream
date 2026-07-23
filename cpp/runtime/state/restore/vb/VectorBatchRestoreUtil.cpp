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
#include <memory>
#include <string_view>

#include "common.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/VoidSerializer.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/vectorbatch/VectorBatch.h"
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
            case omniruntime::type::DataTypeId::OMNI_INT: {
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
            default:
                INFO_RELEASE(
                    "VectorBatchRestoreUtil: unsupported DataTypeId " << static_cast<int>(columnTypes[col])
                                                                      << " at column " << col);
                throw std::runtime_error(
                    "VectorBatchRestoreUtil: unsupported DataTypeId " +
                    std::to_string(static_cast<int>(columnTypes[col])) + " at column " + std::to_string(col));
        }
    }
}

// 检查 DataTypeId 是否在当前 restore 路径中被支持
static bool isRestoreSupportedType(omniruntime::type::DataTypeId typeId)
{
    switch (typeId) {
        case omniruntime::type::DataTypeId::OMNI_INT:
        case omniruntime::type::DataTypeId::OMNI_LONG:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case omniruntime::type::DataTypeId::OMNI_CHAR:
        case omniruntime::type::DataTypeId::OMNI_VARCHAR: return true;
        default: return false;
    }
}

omnistream::ComboId VectorBatchRestoreUtil::appendRowToVectorBatch(
    VbBatchState& vbState,
    const std::vector<int8_t>& valueBytes,
    const std::vector<omniruntime::type::DataTypeId>& columnTypes,
    int batchSize,
    int32_t keyGroup)
{
    if (valueBytes.empty()) {
        INFO_RELEASE("VectorBatchRestoreUtil: empty value bytes, cannot parse RowData");
        return omnistream::INVALID_COMBO_ID;
    }

    int numFields = static_cast<int>(columnTypes.size());
    if (numFields == 0) {
        INFO_RELEASE("VectorBatchRestoreUtil: no column types, value has " << valueBytes.size() << " bytes");
        return omnistream::INVALID_COMBO_ID;
    }

    // 前置校验：不支持的类型必须在写入任何状态前显式拒绝
    for (int col = 0; col < numFields; col++) {
        if (!isRestoreSupportedType(columnTypes[col])) {
            INFO_RELEASE(
                "VectorBatchRestoreUtil: unsupported column type " << static_cast<int>(columnTypes[col])
                                                                   << " at column index " << col);
            return -1;
        }
    }

    DataInputDeserializer valInput(
        reinterpret_cast<const uint8_t*>(valueBytes.data()), static_cast<int>(valueBytes.size()), 0);

    // 按输入长度动态构造 BinaryRowData，替代固定 SEG_SIZE 的 BinaryRowDataSerializer::deserialize
    int rowLen = valInput.readInt();
    if (rowLen <= 0 || rowLen > static_cast<int>(valueBytes.size())) {
        INFO_RELEASE("VectorBatchRestoreUtil: invalid serialized row length " << rowLen);
        return omnistream::INVALID_COMBO_ID;
    }

    std::unique_ptr<BinaryRowData> row(new BinaryRowData(numFields, rowLen));
    valInput.readFully(row->getSegment(), rowLen, 0, rowLen);

    if (vbState.currentBatch == nullptr) {
        LOG("VectorBatchRestoreUtil: creating new VectorBatch, batchId=" << vbState.currentBatchId << ", numFields="
                                                                         << numFields << ", batchSize=" << batchSize);

        try {
            vbState.currentBatch = VectorBatch::CreateVectorBatch(batchSize, columnTypes);
        } catch (const std::runtime_error& e) {
            INFO_RELEASE("VectorBatchRestoreUtil: CreateVectorBatch failed (" << e.what() << ").");
            throw;
        }
    }

    populateVectorBatchFromRow(vbState.currentBatch, columnTypes, row.get(), vbState.currentRowId);

    auto comboId = VectorBatchUtil::getComboId(keyGroup, vbState.currentBatchId, vbState.currentRowId);
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

    // 直接构造 StateMetaInfoSnapshot，使用 commonSerializerKeyToString (UPPERCASE) key，
    // 与 fromMetaInfoSnapshot() 的读取 key 保持一致。
    // 避免通过 RegisteredKeyValueStateBackendMetaInfo::computeSnapshot() 间接构造时
    // 因 key 大小写不一致导致序列化器丢失。
    std::unordered_map<std::string, std::string> optionsMap;
    optionsMap[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = std::to_string(static_cast<int>(stateType));

    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    serializerMap.emplace(
        StateMetaInfoSnapshot::commonSerializerKeyToString(
            StateMetaInfoSnapshot::CommonSerializerKeys::NAMESPACE_SERIALIZER),
        nsSerializer);
    serializerMap.emplace(
        StateMetaInfoSnapshot::commonSerializerKeyToString(
            StateMetaInfoSnapshot::CommonSerializerKeys::VALUE_SERIALIZER),
        valueSerializer);

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    return StateMetaInfoSnapshot(
        flinkMetaInfo.getName(),
        flinkMetaInfo.getBackendStateType(),
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
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

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "WindowJoinSavepointAdaptor.h"

#include <cstdint>
#include <stdexcept>

#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "runtime/checkpoint/StateMetaInfoValidator.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"
#include "table/typeutils/RowDataSerializer.h"

namespace omnistream {

void WindowJoinSavepointAdaptor::prepareForSave(const nlohmann::json& operatorDescription)
{
    (void)operatorDescription;
}

void WindowJoinSavepointAdaptor::prepareForRestore(const nlohmann::json& operatorDescription)
{
    leftColumnTypes_ = convertToDataTypes(parseStringArray(operatorDescription, "leftInputTypes"));
    rightColumnTypes_ = convertToDataTypes(parseStringArray(operatorDescription, "rightInputTypes"));
    inputSideByKvStateId_.clear();

    if (leftColumnTypes_.empty() || rightColumnTypes_.empty()) {
        ERROR_RELEASE(
            "WindowJoinSavepointAdaptor: cannot parse leftColumnTypes or rightColumnTypes from operatorDescription.");
        throw std::runtime_error("error occurred in WindowJoinSavepointAdaptor::prepareForRestore");
    }
}

void WindowJoinSavepointAdaptor::validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    (void)metaInfos;
}

void WindowJoinSavepointAdaptor::validateForRestore(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedListStates({LEFT_RECORDS_STATE_NAME, RIGHT_RECORDS_STATE_NAME});
    validator.requirePriorityQueueStates();
    validator.requireNoMoreStates();

    // validate namespace serializer and value serializer
    for (const auto* stateName : {LEFT_RECORDS_STATE_NAME, RIGHT_RECORDS_STATE_NAME}) {
        TypeSerializer* namespaceSerializer = validator.get(stateName)->getTypeSerializer("NAMESPACE_SERIALIZER");
        TypeSerializer* valueSerializer = validator.get(stateName)->getTypeSerializer("VALUE_SERIALIZER");

        // namespace serializer must be LongSerializer
        if (namespaceSerializer == nullptr || namespaceSerializer->getBackendId() != BackendDataType::BIGINT_BK) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: state '" + std::string(stateName) +
                "' must use a BIGINT window namespace serializer");
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::validateForRestore");
        }

        // value serializer must be ListSerializer
        auto* listSerializer = dynamic_cast<ListSerializer*>(valueSerializer);
        if (listSerializer == nullptr) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: state '" + std::string(stateName) +
                "' must use the List serializer as value serializer");
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::validateForRestore");
        }

        // element serializer must be RowDataSerializer
        RowDataSerializer* rowSerializer = dynamic_cast<RowDataSerializer*>(listSerializer->getElementSerializer());
        if (rowSerializer == nullptr) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: state '" + std::string(stateName) +
                "' must use the RowData serializer as ListState element serializer");
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::validateForRestore");
        }
        std::vector<omniruntime::type::DataTypeId>& columnTypes =
            stateName == LEFT_RECORDS_STATE_NAME ? leftColumnTypes_ : rightColumnTypes_;
        if (columnTypes.size() != rowSerializer->getArity()) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: the column type schema does not match the element serializer on state '" +
                std::string(stateName) + "'.");
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::validateForRestore");
        }
    }
}

void WindowJoinSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& keyGroupOffsets,
    FullSnapshotResources& snapshotResources,
    std::string keySerializer)
{
    (void)stream;
    (void)keyGroupOffsets;
    (void)snapshotResources;
    (void)keySerializer;
}

void WindowJoinSavepointAdaptor::restore(
    SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
{
    VectorBatchRestoreFlow::executeRestore(*this, restoreIterator, backend);
}

RestoreStateType WindowJoinSavepointAdaptor::getStateType(const StateMetaInfoSnapshot& metaInfo)
{
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
        return RestoreStateType::PQ;
    }
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::KEY_VALUE &&
        (metaInfo.getName() == LEFT_RECORDS_STATE_NAME || metaInfo.getName() == RIGHT_RECORDS_STATE_NAME)) {
        return RestoreStateType::KV_WITH_VB;
    }
    return RestoreStateType::UNSUPPORT;
}

StateMetaInfoSnapshot WindowJoinSavepointAdaptor::buildOmniMainMetaInfo(
    int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo)
{
    // record the state id
    if (flinkMetaInfo.getName() == LEFT_RECORDS_STATE_NAME) {
        inputSideByKvStateId_[kvStateId] = InputSide::LEFT;
    } else if (flinkMetaInfo.getName() == RIGHT_RECORDS_STATE_NAME) {
        inputSideByKvStateId_[kvStateId] = InputSide::RIGHT;
    } else {
        ERROR_RELEASE(
            "WindowJoinSavepointAdaptor: cannot build Omni metadata for unexpected state '" + flinkMetaInfo.getName() +
            "'");
        throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::buildOmniMainMetaInfo");
    }

    return VectorBatchRestoreUtil::buildOmniMainMetaInfo(flinkMetaInfo, &mainValueSerializer());
}

void WindowJoinSavepointAdaptor::retrieveKVRowData(
    const std::vector<int8_t>& keyBytes, const std::vector<int8_t>& valueBytes, int kvStateId, RestoreKVStateVB* writer)
{
    if (writer == nullptr) {
        ERROR_RELEASE("WindowJoinSavepointAdaptor: null VectorBatch restore writer");
        throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::retrieveKVRowData");
    }
    if (keyBytes.empty()) {
        ERROR_RELEASE(
            "WindowJoinSavepointAdaptor: empty serialized key for state '" + std::string(stateNameFor(kvStateId)) +
            "'");
        throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::retrieveKVRowData");
    }

    // window join operator state backend type is Key:List<Value>
    // try to get the list from valueBytes
    std::vector<std::vector<int8_t>> rows;
    deserializeRows(valueBytes, rows);

    // append row value to vb table and collect all comboId in the list
    std::vector<uint64_t> comboIds;
    comboIds.reserve(rows.size());
    const auto& types = columnTypesFor(kvStateId);
    for (const auto& rowBytes : rows) {
        RowDataView rowView{&rowBytes, &types};
        // append single value of the list
        uint64_t comboId = writer->appendRowToVectorBatch(rowView);
        if (comboId == omnistream::INVALID_COMBO_ID) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: failed to restore RowData for state '" +
                std::string(stateNameFor(kvStateId)) + "'");
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::retrieveKVRowData");
        }
        comboIds.push_back(comboId);
    }

    // Key:List<ComboId>
    if (!comboIds.empty()) {
        writer->writeComboIdList(keyBytes, comboIds);
    }
}

int WindowJoinSavepointAdaptor::batchSize(int kvStateId) const
{
    (void)kvStateId;
    return VB_RESTORE_BATCH_SIZE;
}

std::vector<omniruntime::type::DataTypeId> WindowJoinSavepointAdaptor::columnTypes(int kvStateId) const
{
    return columnTypesFor(kvStateId);
}

void WindowJoinSavepointAdaptor::deserializeRows(
    const std::vector<int8_t>& valueBytes, std::vector<std::vector<int8_t>>& rows)
{
    rows.clear();
    if (valueBytes.size() < sizeof(int32_t)) {
        ERROR_RELEASE("WindowJoinSavepointAdaptor: invalid value bytes size: " + std::to_string(valueBytes.size()));
        throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::deserializeRows");
    }
    DataInputDeserializer input(
        reinterpret_cast<const uint8_t*>(valueBytes.data()), static_cast<int>(valueBytes.size()));

    // deserialize each row
    while (input.Available() > 0) {
        auto rowStart = input.getPosition();

        if (input.Available() < sizeof(int32_t)) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: The available input is insufficient to read an int32_t, "
                "input.Available: " +
                std::to_string(input.Available()));
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::deserializeRows");
        }

        // get row data length
        int32_t rowLength = input.readInt();
        if (rowLength <= 0 || rowLength > input.Available()) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: row length invalid or available input is shorter than row length: "
                "rowLength: " +
                std::to_string(rowLength) + ", input.Available: " + std::to_string(input.Available()));
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::deserializeRows");
        }

        input.setPosition(input.getPosition() + rowLength);

        auto rowEnd = input.getPosition();
        // [rowLength][valueBytes]
        rows.emplace_back(valueBytes.begin() + rowStart, valueBytes.begin() + rowEnd);

        if (input.Available() <= 0) {
            break;
        }

        uint8_t delimiter = input.readByte();
        if (delimiter != ',') {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor: delimiter invalid: " + std::string(1, static_cast<char>(delimiter)));
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::deserializeRows");
        }
        // The delimiter must be followed by a new row
        if (input.Available() <= 0) {
            ERROR_RELEASE("WindowJoinSavepointAdaptor: expected a new row, but the input is empty");
            throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::deserializeRows");
        }
    }
}

const std::vector<omniruntime::type::DataTypeId>& WindowJoinSavepointAdaptor::columnTypesFor(int kvStateId) const
{
    auto it = inputSideByKvStateId_.find(kvStateId);
    if (it == inputSideByKvStateId_.end()) {
        ERROR_RELEASE("WindowJoinSavepointAdaptor: no input-side mapping for kvStateId=" + std::to_string(kvStateId));
        throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::columnTypesFor");
    }
    return it->second == InputSide::LEFT ? leftColumnTypes_ : rightColumnTypes_;
}

const char* WindowJoinSavepointAdaptor::stateNameFor(int kvStateId) const
{
    auto it = inputSideByKvStateId_.find(kvStateId);
    if (it == inputSideByKvStateId_.end()) {
        ERROR_RELEASE("WindowJoinSavepointAdaptor: no state name for kvStateId=" + std::to_string(kvStateId));
        throw std::runtime_error("error occured on WindowJoinSavepointAdaptor::stateNameFor");
    }
    return it->second == InputSide::LEFT ? LEFT_RECORDS_STATE_NAME : RIGHT_RECORDS_STATE_NAME;
}

TypeSerializer& WindowJoinSavepointAdaptor::mainValueSerializer()
{
    // ListSerializer will manage the LongSerializer lifecycle so we don't use the LongSerializer::INSTANCE as the
    // parameter.
    static ListSerializer serializer{new LongSerializer()};
    return serializer;
}

} // namespace omnistream

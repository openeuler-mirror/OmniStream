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
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/SerializerJsonInfo.h"
#include "runtime/checkpoint/StateMetaInfoValidator.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "state/bridge/OmniTaskBridge.h"
#include "table/types/logical/RowType.h"
#include "table/types/logical/LogicTypeUtils.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/typeutils/RowDataSerializer.h"

namespace omnistream {

void WindowJoinSavepointAdaptor::prepareForSave(const nlohmann::json& operatorDescription)
{
    prepareWindowSidePlans(operatorDescription);
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

void WindowJoinSavepointAdaptor::prepareWindowSidePlans(const nlohmann::json& operatorDescription)
{
    WindowSidePlan leftPlan;
    leftPlan.stateName = LEFT_RECORDS_STATE_NAME;
    parseWindowInputTypes(leftPlan, operatorDescription, StreamingJoinSavepointUtil::LEFT_INPUT_TYPES_FIELD);

    WindowSidePlan rightPlan;
    rightPlan.stateName = RIGHT_RECORDS_STATE_NAME;
    parseWindowInputTypes(rightPlan, operatorDescription, StreamingJoinSavepointUtil::RIGHT_INPUT_TYPES_FIELD);

    if (leftPlan.inputTypeNames.empty() || rightPlan.inputTypeNames.empty()) {
        ERROR_RELEASE(
            "WindowJoinSavepointAdaptor::prepareWindowSidePlans ->"
            << " leftInputTypeCount=" << leftPlan.inputTypeNames.size()
            << ", rightInputTypeCount=" << rightPlan.inputTypeNames.size());
        throw std::runtime_error(
            "WindowJoinSavepointAdaptor::prepareWindowSidePlans left/right inputTypes must not be empty");
    }

    leftPlan_ = std::move(leftPlan);
    rightPlan_ = std::move(rightPlan);
}

void WindowJoinSavepointAdaptor::parseWindowInputTypes(
    WindowSidePlan& sidePlan, const nlohmann::json& description, const std::string& fieldName)
{
    sidePlan.inputTypeNames.clear();
    sidePlan.inputTypes.clear();
    sidePlan.ownedInputTypes.clear();

    if (!description.contains(fieldName) || !description[fieldName].is_array()) {
        ERROR_RELEASE(
            "WindowJoinSavepointAdaptor::parseWindowInputTypes ->"
            << " fieldName=" << fieldName << ", containsField=" << description.contains(fieldName));
        throw std::runtime_error(
            "WindowJoinSavepointAdaptor::parseWindowInputTypes missing input type array field=" + fieldName);
    }

    const auto& inputTypes = description[fieldName];
    sidePlan.inputTypeNames.reserve(inputTypes.size());
    sidePlan.inputTypes.reserve(inputTypes.size());
    sidePlan.ownedInputTypes.reserve(inputTypes.size());
    for (size_t idx = 0; idx < inputTypes.size(); ++idx) {
        const auto& type = inputTypes[idx];
        if (!type.is_string() || type.get<std::string>().empty()) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor::parseWindowInputTypes ->" << " fieldName=" << fieldName
                                                                       << ", fieldSize=" << inputTypes.size());
            throw std::runtime_error(
                "WindowJoinSavepointAdaptor::parseWindowInputTypes invalid input type field=" + fieldName);
        }
        std::string inputTypeName = type.get<std::string>();
        sidePlan.inputTypeNames.push_back(inputTypeName);

        // 确定 nullable：从类型字符串检测 "NOT NULL"，默认 true
        bool nullable = !LogicTypeUtils::isNotNullType(inputTypeName);

        // 用带 nullable 的 options 构建 LogicalType
        std::string stripped = LogicTypeUtils::stripFlinkTypeExtras(inputTypeName);
        nlohmann::json options = LogicTypeUtils::optionsFromFlinkType(stripped);
        options["nullable"] = nullable;
        int typeId = LogicalType::flinkTypeToOmniTypeId(stripped);
        LogicalType* logicalType =
            BasicLogicalType::getTypeBy(static_cast<omniruntime::type::DataTypeId>(typeId), options);
        sidePlan.inputTypes.push_back(logicalType);
        if (!LogicalType::isSharedLogicalType(logicalType)) {
            sidePlan.ownedInputTypes.emplace_back(logicalType);
        }
    }
}

void WindowJoinSavepointAdaptor::validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedListStateWithVB(LEFT_RECORDS_STATE_NAME);
    validator.requireKeyedListStateWithVB(RIGHT_RECORDS_STATE_NAME);
    validator.requirePriorityQueueStates();
    validator.requireNoMoreStates();
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

const WindowJoinSavepointAdaptor::WindowSidePlan& WindowJoinSavepointAdaptor::windowSidePlanForState(
    const std::string& stateName) const
{
    if (stateName == LEFT_RECORDS_STATE_NAME) {
        return leftPlan_;
    }
    if (stateName == RIGHT_RECORDS_STATE_NAME) {
        return rightPlan_;
    }
    ERROR_RELEASE("WindowJoinSavepointAdaptor::windowSidePlanForState ->" << " stateName=" << stateName);
    throw std::runtime_error("WindowJoinSavepointAdaptor::windowSidePlanForState unsupported state=" + stateName);
}

// ===== 保存方向：构建保存计划 =====

VectorBatchSavePlan WindowJoinSavepointAdaptor::buildWindowSavePlan(FullSnapshotResources& snapshotResources)
{
    VectorBatchSavePlan plan;
    const auto& metaInfos = snapshotResources.getMetaInfoSnapshots();
    plan.keyGroupRange = snapshotResources.getKeyGroupRange();
    plan.isHeapBackend = snapshotResources.isHeapBackend();

    int newKvStateId = 0;
    for (size_t i = 0; i < metaInfos.size(); ++i) {
        const auto& meta = metaInfos[i];
        if (meta == nullptr || VectorBatchSaveTools::isVbStateName(meta->getName())) {
            continue;
        }
        const std::string& stateName = meta->getName();

        // PRIORITY_QUEUE 状态（如 _timer_state/*）由 VectorBatchSaveFlow 按 PQ 透传路径
        // 直接输出 key/value 字节。保持 OmniStream 原生元信息，使用 PQ 状态类型。
        if (meta->getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
            plan.targetMetaInfos.push_back(std::make_shared<StateMetaInfoSnapshot>(*meta));
            int mappedKvStateId = newKvStateId++;
            plan.kvStateIdMapping[static_cast<int>(i)] = mappedKvStateId;
            plan.mainStateIds.push_back(static_cast<int>(i));
            VectorBatchSavePlan::StateContextSpec spec;
            spec.sourceKvStateId = static_cast<int>(i);
            spec.logicalStateName = stateName;
            spec.stateType = VectorBatchStateType::PQ;
            plan.stateContextSpecs.push_back(std::move(spec));
            continue;
        }

        if (stateName != LEFT_RECORDS_STATE_NAME && stateName != RIGHT_RECORDS_STATE_NAME) {
            continue;
        }

        const WindowSidePlan& sidePlan = windowSidePlanForState(stateName);

        // 使用已解析好的 inputTypes（含正确 nullable）构建 RowType
        std::vector<omnistream::RowField> rowFields;
        for (size_t k = 0; k < sidePlan.inputTypes.size(); ++k) {
            rowFields.emplace_back("f" + std::to_string(k), sidePlan.inputTypes[k], "");
        }
        auto flinkRowType = std::make_unique<RowType>(true, rowFields);
        auto* rowDataSerializer = new RowDataSerializer(flinkRowType.get());
        auto* stateSerializer = new ListSerializer(rowDataSerializer);
        plan.ownedSerializers.emplace_back(stateSerializer);

        RegisteredKeyValueStateBackendMetaInfo convertedMetaInfo(
            StateDescriptor::Type::LIST, stateName, LongSerializer::INSTANCE, stateSerializer);
        plan.targetMetaInfos.push_back(convertedMetaInfo.snapshot());

        int mappedKvStateId = newKvStateId++;
        plan.kvStateIdMapping[static_cast<int>(i)] = mappedKvStateId;
        plan.mainStateIds.push_back(static_cast<int>(i));

        VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = static_cast<int>(i);
        spec.logicalStateName = stateName;
        spec.valueSerializer = rowDataSerializer;
        spec.stateType = VectorBatchStateType::KV_WITH_VB;
        spec.accessorOptions.maxDecodedBatchCacheBytes = VB_SAVE_CACHE_BYTES;
        plan.stateContextSpecs.push_back(std::move(spec));
    }

    return plan;
}

// ===== VectorBatchSaveHooks =====

std::vector<VectorBatchSaveStateContext> WindowJoinSavepointAdaptor::buildSaveStateContexts(
    FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan)
{
    std::vector<VectorBatchSaveStateContext> contexts(snapshotResources.getMetaInfoSnapshots().size());
    for (const auto& spec : plan.stateContextSpecs) {
        if (spec.sourceKvStateId < 0 || static_cast<size_t>(spec.sourceKvStateId) >= contexts.size()) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor::buildSaveStateContexts ->" << " sourceKvStateId=" << spec.sourceKvStateId);
            throw std::runtime_error(
                "WindowJoinSavepointAdaptor: sourceKvStateId=" + std::to_string(spec.sourceKvStateId) +
                " out of range");
        }
        auto& ctx = contexts[spec.sourceKvStateId];
        ctx.writable = true;
        ctx.mappedKvStateId = plan.kvStateIdMapping.at(spec.sourceKvStateId);
        ctx.logicalStateName = spec.logicalStateName;
        ctx.valueSerializer = spec.valueSerializer;
        ctx.stateType = spec.stateType;
        // 仅 KV_WITH_VB 状态需要 VB accessor，PQ 状态直接透传 key/value 字节
        if (spec.stateType == VectorBatchStateType::KV_WITH_VB) {
            ctx.vbAccessor =
                snapshotResources.createVectorBatchStateAccessor(spec.logicalStateName, spec.accessorOptions);
            if (ctx.vbAccessor == nullptr) {
                ERROR_RELEASE(
                    "WindowJoinSavepointAdaptor::buildSaveStateContexts ->"
                    << " failed to create VB accessor for state=" << spec.logicalStateName);
                throw std::runtime_error(
                    "WindowJoinSavepointAdaptor: failed to create VB accessor for state=" + spec.logicalStateName);
            }
        }
    }
    return contexts;
}

// ===== 保存方向：自定义聚合实现 =====

// 将一组 RowData 字节序列化为 Flink MapState<Long, List<RowData>> 的 value 格式
std::vector<int8_t> WindowJoinSavepointAdaptor::serializeFlinkRowDataList(
    const std::vector<std::vector<int8_t>>& rowDataBytesList, const std::vector<std::string>& /*inputTypeNames*/)
{
    // Flink ListDelimitedSerializer 格式（与 Flink 1.16 源码一致）:
    //   [element_1 bytes][delimiter=','][element_2 bytes][delimiter=',']...[element_N bytes]
    // delimiter 写在每对元素之间（第一个元素前和最后一个元素后都没有 delimiter）。
    // 无 size 前缀，无 per-element length 前缀。
    static const int8_t DELIMITER = static_cast<int8_t>(',');

    size_t totalSize = 0;
    for (size_t i = 0; i < rowDataBytesList.size(); ++i) {
        totalSize += rowDataBytesList[i].size();
        if (i > 0) {
            totalSize += 1; // delimiter before this element
        }
    }

    std::vector<int8_t> result;
    result.reserve(totalSize);
    for (size_t i = 0; i < rowDataBytesList.size(); ++i) {
        if (i > 0) {
            result.push_back(DELIMITER);
        }
        const auto& rowBytes = rowDataBytesList[i];
        result.insert(result.end(), rowBytes.begin(), rowBytes.end());
    }
    return result;
}

// 保存方向：OmniStream → Flink
// 使用标准 VectorBatchSaveFlow 流程，combId 解析和 RowData 转换由 convertKVRowData 钩子完成。
void WindowJoinSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& keyGroupOffsets,
    FullSnapshotResources& snapshotResources,
    std::string keySerializer)
{
    VectorBatchSavePlan plan = buildWindowSavePlan(snapshotResources);
    VectorBatchSaveFlow::executeSave(*this, plan, stream, keyGroupOffsets, snapshotResources, std::move(keySerializer));
}

void WindowJoinSavepointAdaptor::convertKVRowData(
    const KeyValueStateIterator::CurrentEntry& entry,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan& plan,
    std::function<void(ConvertedEntry)> output)
{
    // 解析 comboId 列表
    auto comboIds =
        VectorBatchSaveTools::parseComboIdList(ByteView(entry.value.data(), entry.value.size()), plan.isHeapBackend);

    // 解引用 VB 获取 RowData
    if (!context.vbAccessor) {
        ERROR_RELEASE(
            "WindowJoinSavepointAdaptor::convertKVRowData - null vbAccessor for state=" << context.logicalStateName);
        throw std::runtime_error("WindowJoinSavepointAdaptor: null vbAccessor for state=" + context.logicalStateName);
    }

    const auto& sidePlan = windowSidePlanForState(context.logicalStateName);

    // 遍历 comboId 列表，逐个解引用 VB 获取行数据
    std::vector<std::vector<int8_t>> rowDataBytesList;
    rowDataBytesList.reserve(comboIds.size());
    for (auto comboId : comboIds) {
        omnistream::VectorBatchId batchId = VectorBatchUtil::getVectorBatchId(comboId);
        int32_t rowId = VectorBatchUtil::getRowId(comboId);

        auto row = context.vbAccessor->getRow(batchId, rowId);
        if (!row) {
            ERROR_RELEASE(
                "WindowJoinSavepointAdaptor::convertKVRowData - null row for comboId="
                << comboId << ", batchId=" << batchId << ", rowId=" << rowId << ", valueSize=" << entry.value.size()
                << ", comboIdCount=" << comboIds.size());
            throw std::runtime_error(
                "WindowJoinSavepointAdaptor: null row for comboId=" + std::to_string(comboId) +
                ", batchId=" + std::to_string(batchId) + ", rowId=" + std::to_string(rowId) + ", valueSize=" +
                std::to_string(entry.value.size()) + ", comboIdCount=" + std::to_string(comboIds.size()));
        }

        auto rowDataBytes = VectorBatchSaveTools::serializeRowData(row.get(), context.valueSerializer);
        rowDataBytesList.push_back(std::move(rowDataBytes));
    }

    // 序列化为 Flink List<RowData> 格式，输出一条 ConvertedEntry
    auto valueBytes = serializeFlinkRowDataList(rowDataBytesList, sidePlan.inputTypeNames);
    ConvertedEntry converted;
    converted.context = &context;
    converted.keyBytes.assign(
        reinterpret_cast<const int8_t*>(entry.key.data()),
        reinterpret_cast<const int8_t*>(entry.key.data()) + entry.key.size());
    converted.valueBytes = std::move(valueBytes);
    output(std::move(converted));
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

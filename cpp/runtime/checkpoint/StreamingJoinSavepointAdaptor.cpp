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

#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "StreamingJoinSavepointAdaptor.h"
#include "StreamingJoinSavepointUtil.h"
#include "../state/restore/RestoreKVStateVB.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/JoinTupleSerializer2.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "runtime/checkpoint/StateMetaInfoValidator.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "table/data/binary/BinaryRowData.h"

namespace omnistream {

StreamingJoinSavepointAdaptor::StreamingJoinSavepointAdaptor(FlinkSavepointAdaptorType adaptorType)
    : adaptorType_(adaptorType)
{
}

void StreamingJoinSavepointAdaptor::prepareSidePlans(const nlohmann::json& operatorDescription)
{
    SidePlan leftPlan;
    leftPlan.stateName = StreamingJoinSavepointUtil::LEFT_STATE_NAME;
    leftPlan.outerJoinState = adaptorType_ == FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor;
    parseInputTypes(leftPlan, operatorDescription, StreamingJoinSavepointUtil::LEFT_INPUT_TYPES_FIELD);

    SidePlan rightPlan;
    rightPlan.stateName = StreamingJoinSavepointUtil::RIGHT_STATE_NAME;
    rightPlan.outerJoinState = false;
    parseInputTypes(rightPlan, operatorDescription, StreamingJoinSavepointUtil::RIGHT_INPUT_TYPES_FIELD);

    if (leftPlan.inputTypeNames.empty() || rightPlan.inputTypeNames.empty()) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::prepareSidePlans ->"
            << " leftInputTypeCount=" << leftPlan.inputTypeNames.size() << ", rightInputTypeCount="
            << rightPlan.inputTypeNames.size() << ", adaptorType=" << static_cast<int>(adaptorType_));
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::prepareSidePlans left/right inputTypes must not be empty");
    }

    leftPlan_ = std::move(leftPlan);
    rightPlan_ = std::move(rightPlan);
}

void StreamingJoinSavepointAdaptor::parseInputTypes(
    SidePlan& sidePlan, const nlohmann::json& description, const std::string& fieldName)
{
    sidePlan.inputTypeNames.clear();
    sidePlan.inputTypes.clear();
    sidePlan.ownedInputTypes.clear();

    if (!description.contains(fieldName) || !description[fieldName].is_array()) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::parseInputTypes ->"
            << " fieldName=" << fieldName << ", containsField=" << description.contains(fieldName)
            << ", descriptionSize=" << description.size());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseInputTypes missing input type array field=" + fieldName);
    }

    const auto& inputTypes = description[fieldName];
    sidePlan.inputTypeNames.reserve(inputTypes.size());
    sidePlan.inputTypes.reserve(inputTypes.size());
    sidePlan.ownedInputTypes.reserve(inputTypes.size());
    for (const auto& type : inputTypes) {
        if (!type.is_string() || type.get<std::string>().empty()) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointAdaptor::parseInputTypes ->" << " fieldName=" << fieldName
                                                                           << ", fieldSize=" << inputTypes.size()
                                                                           << ", isString=" << type.is_string());
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::parseInputTypes invalid input type field=" + fieldName);
        }
        std::string inputTypeName = type.get<std::string>();
        sidePlan.inputTypeNames.push_back(inputTypeName);
        LogicalType* logicalType = LogicalType::flinkTypeToOmniType(inputTypeName);
        sidePlan.inputTypes.push_back(logicalType);
        if (!LogicalType::isSharedLogicalType(logicalType)) {
            sidePlan.ownedInputTypes.emplace_back(logicalType);
        }
    }
}

void StreamingJoinSavepointAdaptor::prepareForSave(const nlohmann::json& operatorDescription)
{
    prepareSidePlans(operatorDescription);
}

void StreamingJoinSavepointAdaptor::prepareForRestore(const nlohmann::json& operatorDescription)
{
    prepareSidePlans(operatorDescription);
}

void StreamingJoinSavepointAdaptor::validateForSave(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedMapStateWithVB(StreamingJoinSavepointUtil::LEFT_STATE_NAME);
    validator.requireKeyedMapStateWithVB(StreamingJoinSavepointUtil::RIGHT_STATE_NAME);
    validator.requireNoMoreStates();
}

void StreamingJoinSavepointAdaptor::validateForRestore(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedMapState(StreamingJoinSavepointUtil::LEFT_STATE_NAME);
    validator.requireKeyedMapState(StreamingJoinSavepointUtil::RIGHT_STATE_NAME);
    validator.requireNoMoreStates();
}

void StreamingJoinSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& keyGroupOffsets,
    FullSnapshotResources& snapshotResources,
    std::string keySerializer)
{
    VectorBatchSavePlan plan = buildSavePlan(snapshotResources);
    VectorBatchSaveFlow::executeSave(*this, plan, stream, keyGroupOffsets, snapshotResources, std::move(keySerializer));
}

void StreamingJoinSavepointAdaptor::restore(
    SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
{
    leftRestoreKvStateId_ = -1;
    rightRestoreKvStateId_ = -1;
    VectorBatchRestoreFlow::executeRestore(*this, restoreIterator, backend);
}

StateMetaInfoSnapshot StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo(
    int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo)
{
    const SidePlan& sidePlan = sidePlanForState(flinkMetaInfo.getName());
    if (&sidePlan == &leftPlan_) {
        leftRestoreKvStateId_ = kvStateId;
    } else {
        rightRestoreKvStateId_ = kvStateId;
    }
    TypeSerializer* namespaceSerializer = flinkMetaInfo.getNamespaceSerializer();
    if (namespaceSerializer == nullptr) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo -> stateName="
            << flinkMetaInfo.getName() << ", namespaceSerializer=null");
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo missing namespace serializer for state=" +
            flinkMetaInfo.getName());
    }
    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo ->"
            << " stateName=" << flinkMetaInfo.getName()
            << ", namespaceBackendId=" << namespaceSerializer->getBackendId());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo StreamingJoin requires VoidNamespace for state=" +
            flinkMetaInfo.getName());
    }

    TypeSerializer* joinValueSerializer = sidePlan.outerJoinState
                                              ? static_cast<TypeSerializer*>(new JoinTupleSerializer2())
                                              : static_cast<TypeSerializer*>(new JoinTupleSerializer());
    auto* stateSerializer = new MapSerializer(new XxH128_hashSerializer(), joinValueSerializer);

    std::unordered_map<std::string, std::string> optionsMap;
    optionsMap.emplace(
        StateMetaInfoSnapshot::commonOptionsKeyToString(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE),
        std::to_string(static_cast<int>(StateDescriptor::Type::MAP)));

    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    serializerMap.emplace(StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, namespaceSerializer);
    serializerMap.emplace(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, stateSerializer);

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    return StateMetaInfoSnapshot(
        flinkMetaInfo.getName(),
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}

RestoreStateType StreamingJoinSavepointAdaptor::getStateType(const StateMetaInfoSnapshot& metaInfo) const
{
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
        return RestoreStateType::PQ;
    }
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
        const std::string& stateName = metaInfo.getName();
        if (stateName != StreamingJoinSavepointUtil::LEFT_STATE_NAME &&
            stateName != StreamingJoinSavepointUtil::RIGHT_STATE_NAME) {
            return RestoreStateType::KV;
        }
        return RestoreStateType::KV_WITH_VB;
    }
    return RestoreStateType::UNSUPPORT;
}

std::vector<omniruntime::type::DataTypeId> StreamingJoinSavepointAdaptor::columnTypes(int kvStateId) const
{
    return StreamingJoinSavepointUtil::convertToDataTypes(restoreSidePlan(kvStateId).inputTypes);
}

int StreamingJoinSavepointAdaptor::batchSize(int /*kvStateId*/) const
{
    return VB_RESTORE_BATCH_SIZE;
}

void StreamingJoinSavepointAdaptor::retrieveKVRowData(
    const std::vector<int8_t>& keyBytes, const std::vector<int8_t>& valueBytes, int kvStateId, RestoreKVStateVB* writer)
{
    const SidePlan& sidePlan = restoreSidePlan(kvStateId);
    auto keyParts =
        StreamingJoinSavepointUtil::splitFlinkMapKey(keyBytes, sidePlan.inputTypes, writer->getKeyGroupPrefixBytes());
    auto joinValue = StreamingJoinSavepointUtil::parseFlinkJoinValue(
        ByteView::fromBuffer(valueBytes.data(), valueBytes.size()), sidePlan.outerJoinState);

    std::vector<int8_t> rowBytes = keyParts.rowDataBytes;
    const auto columnTypes = StreamingJoinSavepointUtil::convertToDataTypes(sidePlan.inputTypes);
    RowDataView row;
    row.valueBytes = &rowBytes;
    row.columnTypes = &columnTypes;

    XXH128_hash_t rowHash = calculateRestoreRowHash(rowBytes, columnTypes);
    omnistream::ComboId comboId = writer->appendRowToVectorBatch(row);

    std::vector<int8_t> mainKeyBytes = StreamingJoinSavepointUtil::serializeOmniMapKey(keyParts.keyPrefix, rowHash);
    std::vector<int8_t> mainValueBytes = StreamingJoinSavepointUtil::serializeOmniJoinValue(joinValue, comboId);
    writer->writeEntry<ByteView>(mainKeyBytes, ByteView::fromBuffer(mainValueBytes.data(), mainValueBytes.size()));
}

XXH128_hash_t StreamingJoinSavepointAdaptor::calculateRestoreRowHash(
    const std::vector<int8_t>& rowBytes, const std::vector<omniruntime::type::DataTypeId>& columnTypes) const
{
    /*
     * StreamingJoin 运行态主 MapState 的 map key 使用 VectorBatch 行级 XXH128(row)。
     * restore 时直接按 getXXH128s() 的字段语义从 Flink logical key 中的 RowData 计算，
     * 避免为每行构造临时 VectorBatch，也避免在公共 writer 中暴露 StreamingJoin 专用 hash 语义。
     */
    if (rowBytes.empty() || columnTypes.empty()) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::calculateRestoreRowHash ->"
            << " rowBytesSize=" << rowBytes.size() << ", columnTypeCount=" << columnTypes.size());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::calculateRestoreRowHash row hash requires non-empty row bytes and columns");
    }
    DataInputDeserializer rowInput(
        reinterpret_cast<const uint8_t*>(rowBytes.data()), static_cast<int>(rowBytes.size()), 0);
    int rowLen = rowInput.readInt();
    if (rowLen <= 0 || rowLen > static_cast<int>(rowBytes.size())) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::calculateRestoreRowHash ->"
            << " rowBytesSize=" << rowBytes.size() << ", rowLen=" << rowLen
            << ", columnTypeCount=" << columnTypes.size());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::calculateRestoreRowHash invalid row length when calculating row hash");
    }

    BinaryRowData binaryRow(static_cast<int>(columnTypes.size()), rowLen);
    rowInput.readFully(binaryRow.getSegment(), rowLen, 0, rowLen);
    static thread_local std::unique_ptr<XXH3_state_t, decltype(&XXH3_freeState)> hashState(
        XXH3_createState(), &XXH3_freeState);
    if (hashState == nullptr) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::calculateRestoreRowHash ->"
            << " rowBytesSize=" << rowBytes.size() << ", rowLen=" << rowLen
            << ", columnTypeCount=" << columnTypes.size() << ", hashState=null");
        throw std::runtime_error("StreamingJoinSavepointAdaptor::calculateRestoreRowHash failed to create hash state");
    }
    XXH3_128bits_reset(hashState.get());
    for (int col = 0; col < static_cast<int>(columnTypes.size()); ++col) {
        switch (columnTypes[col]) {
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                int64_t value = binaryRow.isNullAt(col) ? 0 : *binaryRow.getLong(col);
                XXH3_128bits_update(hashState.get(), &value, sizeof(value));
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                if (!binaryRow.isNullAt(col)) {
                    std::string_view value = binaryRow.getStringView(col);
                    XXH3_128bits_update(hashState.get(), value.data(), value.size());
                }
                break;
            }
            default:
                INFO_RELEASE(
                    "Error: StreamingJoinSavepointAdaptor::calculateRestoreRowHash ->"
                    << " rowBytesSize=" << rowBytes.size() << ", rowLen=" << rowLen
                    << ", columnTypeCount=" << columnTypes.size() << ", column=" << col
                    << ", columnType=" << static_cast<int>(columnTypes[col]));
                throw std::runtime_error("StreamingJoinSavepointAdaptor::calculateRestoreRowHash unsupported type");
        }
    }
    return XXH3_128bits_digest(hashState.get());
}

std::vector<VectorBatchSaveStateContext> StreamingJoinSavepointAdaptor::buildSaveStateContexts(
    FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan)
{
    std::vector<VectorBatchSaveStateContext> contexts(snapshotResources.getMetaInfoSnapshots().size());
    for (const auto& spec : plan.stateContextSpecs) {
        if (spec.sourceKvStateId < 0 || static_cast<size_t>(spec.sourceKvStateId) >= contexts.size()) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointAdaptor::buildSaveStateContexts ->"
                << " sourceKvStateId=" << spec.sourceKvStateId << ", contextCount=" << contexts.size()
                << ", logicalStateName=" << spec.logicalStateName);
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::buildSaveStateContexts invalid source kvStateId=" +
                std::to_string(spec.sourceKvStateId));
        }
        auto& ctx = contexts[spec.sourceKvStateId];
        ctx.writable = true;
        ctx.mappedKvStateId = plan.kvStateIdMapping.at(spec.sourceKvStateId);
        ctx.logicalStateName = spec.logicalStateName;
        ctx.valueSerializer = spec.valueSerializer;
        ctx.stateType = VectorBatchStateType::KV_WITH_VB;
        ctx.vbAccessor = snapshotResources.createVectorBatchStateAccessor(spec.logicalStateName, spec.accessorOptions);
        if (ctx.vbAccessor == nullptr) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointAdaptor::buildSaveStateContexts ->"
                << " sourceKvStateId=" << spec.sourceKvStateId << ", logicalStateName=" << spec.logicalStateName
                << ", vbAccessor=null");
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::buildSaveStateContexts failed to create VB accessor for state=" +
                spec.logicalStateName);
        }
    }
    return contexts;
}

omnistream::ComboId StreamingJoinSavepointAdaptor::parseVectorBatchReference(
    ByteView value, const VectorBatchSaveStateContext& context, const VectorBatchSavePlan& /*plan*/)
{
    auto parsed = StreamingJoinSavepointUtil::parseOmniJoinValue(value);
    const SidePlan& sidePlan = sidePlanForState(context.logicalStateName);
    if (parsed.outerJoinState != sidePlan.outerJoinState) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::parseVectorBatchReference ->"
            << " stateName=" << context.logicalStateName << ", parsedOuterJoinState=" << parsed.outerJoinState
            << ", expectedOuterJoinState=" << sidePlan.outerJoinState << ", valueSize=" << value.size());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseVectorBatchReference source Join value layout does not match state=" +
            context.logicalStateName);
    }
    return parsed.comboId;
}

std::vector<int8_t> StreamingJoinSavepointAdaptor::encodeFlinkLogicalKey(
    const KeyValueStateIterator::CurrentEntry& entry,
    RowData& row,
    const VectorBatchSaveStateContext& /*context*/,
    const VectorBatchSavePlan& /*plan*/)
{
    return StreamingJoinSavepointUtil::serializeFlinkMapKey(entry.key, row);
}

std::vector<int8_t> StreamingJoinSavepointAdaptor::encodeFlinkLogicalValue(
    const KeyValueStateIterator::CurrentEntry& entry,
    RowData& /*row*/,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan& /*plan*/)
{
    const SidePlan& sidePlan = sidePlanForState(context.logicalStateName);
    auto parsed = StreamingJoinSavepointUtil::parseOmniJoinValue(entry.value);
    if (parsed.outerJoinState != sidePlan.outerJoinState) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::encodeFlinkLogicalValue ->"
            << " stateName=" << context.logicalStateName << ", parsedOuterJoinState=" << parsed.outerJoinState
            << ", expectedOuterJoinState=" << sidePlan.outerJoinState << ", valueSize=" << entry.value.size());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::encodeFlinkLogicalValue source Join value layout does not match state=" +
            context.logicalStateName);
    }
    return StreamingJoinSavepointUtil::serializeFlinkMapValue(parsed, sidePlan.outerJoinState);
}

void StreamingJoinSavepointAdaptor::convertKVRowData(
    const KeyValueStateIterator::CurrentEntry& entry,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan& plan,
    std::function<void(ConvertedEntry)> output)
{
    if (context.vbAccessor == nullptr) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointAdaptor::convertKVRowData -> stateName=" << context.logicalStateName
                                                                                   << ", vbAccessor=null");
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::convertKVRowData missing VectorBatch accessor for state=" +
            context.logicalStateName);
    }

    const SidePlan& sidePlan = sidePlanForState(context.logicalStateName);
    parseSourceMapEntries(entry, sidePlan, [&](ByteView keyBytes, ByteView valueBytes, omnistream::ComboId comboId) {
        KeyValueStateIterator::CurrentEntry convertedSource = entry;
        convertedSource.key = keyBytes;
        convertedSource.value = valueBytes;
        auto row = context.vbAccessor->getRow(comboId);
        if (row == nullptr) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointAdaptor::convertKVRowData ->"
                << " stateName=" << context.logicalStateName << ", comboId=" << comboId
                << ", sourceKeySize=" << keyBytes.size() << ", sourceValueSize=" << valueBytes.size());
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::convertKVRowData missing VectorBatch row for comboId=" +
                std::to_string(comboId));
        }

        ConvertedEntry converted;
        converted.context = &context;
        converted.keyBytes = encodeFlinkLogicalKey(convertedSource, *row, context, plan);
        converted.valueBytes = encodeFlinkLogicalValue(convertedSource, *row, context, plan);
        converted.comboRef = comboId;
        output(std::move(converted));
    });
}

void StreamingJoinSavepointAdaptor::parseSourceMapEntries(
    const KeyValueStateIterator::CurrentEntry& entry,
    const SidePlan& sidePlan,
    const std::function<void(ByteView keyBytes, ByteView valueBytes, omnistream::ComboId comboId)>& emit) const
{
    const size_t singleValueSize = sidePlan.outerJoinState ? 1 + sizeof(int32_t) + sizeof(int32_t) + sizeof(int64_t)
                                                           : 1 + sizeof(int32_t) + sizeof(int64_t);
    if (entry.value.size() == singleValueSize) {
        auto parsed = StreamingJoinSavepointUtil::parseOmniJoinValue(entry.value);
        if (parsed.outerJoinState != sidePlan.outerJoinState ||
            entry.key.size() <= StreamingJoinSavepointUtil::XXH128_SERIALIZED_BYTES) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointAdaptor::parseSourceMapEntries ->"
                << " stateName=" << sidePlan.stateName << ", keySize=" << entry.key.size()
                << ", valueSize=" << entry.value.size() << ", parsedOuterJoinState=" << parsed.outerJoinState
                << ", expectedOuterJoinState=" << sidePlan.outerJoinState);
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::parseSourceMapEntries invalid expanded Omni MapState entry, state=" +
                sidePlan.stateName);
        }
        emit(
            ByteView::fromBuffer(entry.key.data(), entry.key.size()),
            ByteView::fromBuffer(entry.value.data(), entry.value.size()),
            parsed.comboId);
        return;
    }

    const auto parsedEntries =
        StreamingJoinSavepointUtil::parseOmniMapStateEntries(entry.value, sidePlan.outerJoinState);
    ByteView keyPrefix = ByteView::fromBuffer(entry.key.data(), entry.key.size());
    for (const auto& parsedEntry : parsedEntries) {
        std::vector<int8_t> keyBytes = StreamingJoinSavepointUtil::serializeOmniMapKey(keyPrefix, parsedEntry.mapKey);
        std::vector<int8_t> valueBytes =
            StreamingJoinSavepointUtil::serializeOmniJoinValue(parsedEntry.value, parsedEntry.value.comboId);
        emit(
            ByteView::fromBuffer(keyBytes.data(), keyBytes.size()),
            ByteView::fromBuffer(valueBytes.data(), valueBytes.size()),
            parsedEntry.value.comboId);
    }
}

VectorBatchSavePlan StreamingJoinSavepointAdaptor::buildSavePlan(FullSnapshotResources& snapshotResources)
{
    VectorBatchSavePlan plan;
    const auto& metaInfos = snapshotResources.getMetaInfoSnapshots();

    for (size_t i = 0; i < metaInfos.size(); ++i) {
        const auto& meta = metaInfos[i];
        if (meta == nullptr || VectorBatchSaveTools::isVbStateName(meta->getName())) {
            continue;
        }
        const std::string& stateName = meta->getName();
        const SidePlan& sidePlan = sidePlanForState(stateName);
        int mappedKvStateId = static_cast<int>(plan.targetMetaInfos.size());
        plan.kvStateIdMapping[static_cast<int>(i)] = mappedKvStateId;
        plan.targetMetaInfos.push_back(
            StreamingJoinSavepointUtil::createFlinkMapStateSnapshot(
                stateName, *meta, sidePlan.inputTypeNames, sidePlan.outerJoinState));
        plan.mainStateIds.push_back(static_cast<int>(i));

        VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = static_cast<int>(i);
        spec.logicalStateName = stateName;
        spec.valueSerializer = meta->getValueSerializer();
        if (spec.valueSerializer == nullptr) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointAdaptor::buildSavePlan ->"
                << " stateName=" << stateName << ", sourceKvStateId=" << i << ", valueSerializer=null");
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::buildSavePlan missing source value serializer for state=" + stateName);
        }
        spec.accessorOptions.maxDecodedBatchCacheBytes = VB_SAVE_CACHE_BYTES;
        plan.stateContextSpecs.push_back(std::move(spec));
    }
    return plan;
}

const StreamingJoinSavepointAdaptor::SidePlan& StreamingJoinSavepointAdaptor::sidePlanForState(
    const std::string& stateName) const
{
    if (stateName == StreamingJoinSavepointUtil::LEFT_STATE_NAME) {
        return leftPlan_;
    }
    if (stateName == StreamingJoinSavepointUtil::RIGHT_STATE_NAME) {
        return rightPlan_;
    }
    INFO_RELEASE(
        "Error: StreamingJoinSavepointAdaptor::sidePlanForState ->"
        << " stateName=" << stateName << ", leftState=" << StreamingJoinSavepointUtil::LEFT_STATE_NAME
        << ", rightState=" << StreamingJoinSavepointUtil::RIGHT_STATE_NAME);
    throw std::runtime_error("StreamingJoinSavepointAdaptor::sidePlanForState unsupported state=" + stateName);
}

const StreamingJoinSavepointAdaptor::SidePlan& StreamingJoinSavepointAdaptor::restoreSidePlan(int kvStateId) const
{
    if (kvStateId >= 0 && kvStateId == leftRestoreKvStateId_) {
        return leftPlan_;
    }
    if (kvStateId >= 0 && kvStateId == rightRestoreKvStateId_) {
        return rightPlan_;
    }
    INFO_RELEASE(
        "Error: StreamingJoinSavepointAdaptor::restoreSidePlan ->" << " kvStateId=" << kvStateId
                                                                   << ", leftKvStateId=" << leftRestoreKvStateId_
                                                                   << ", rightKvStateId=" << rightRestoreKvStateId_);
    throw std::runtime_error(
        "StreamingJoinSavepointAdaptor::restoreSidePlan missing side plan for kvStateId=" + std::to_string(kvStateId));
}

} // namespace omnistream

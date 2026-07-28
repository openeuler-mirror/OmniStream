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

#include <sstream>
#include <stdexcept>

#include "table/types/logical/RowType.h"
#include "table/typeutils/SortedVectorLong.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/checkpoint/StateMetaInfoValidator.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/utils/HashFunctor.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "AppendOnlyTopNSavepointAdaptor.h"

namespace omnistream {

// ===== 构造 / 析构 =====

AppendOnlyTopNSavepointAdaptor::AppendOnlyTopNSavepointAdaptor()
{
    outputSerializer_.setBackendBuffer(&outputBufferStatus_);
}

// ===== OperatorSavepointAdaptor 重写 =====

void AppendOnlyTopNSavepointAdaptor::prepareForSave(const nlohmann::json& operatorDescription)
{
    compatibleColumnTypes_ = parseStringArray(operatorDescription, "inputTypes");
    inputRowType_ = convertToDataTypes(compatibleColumnTypes_);
    sortKeyIndices_ = operatorDescription["sortFieldIndices"].get<std::vector<int>>();
    for (const auto& keyCol : sortKeyIndices_) {
        sortKeyTypeIds_.push_back(inputRowType_[keyCol]);
    }
    sortKeySelector_ = KeySelector<RowData*>(sortKeyTypeIds_, sortKeyIndices_);

    rowSerializer_ = std::make_unique<RowDataSerializer>(new omnistream::RowType(false, compatibleColumnTypes_));
}

void AppendOnlyTopNSavepointAdaptor::prepareForRestore(const nlohmann::json& operatorDescription)
{
    compatibleColumnTypes_ = parseStringArray(operatorDescription, "inputTypes");
    inputRowType_ = convertToDataTypes(compatibleColumnTypes_);
    sortKeyIndices_ = operatorDescription["sortFieldIndices"].get<std::vector<int>>();
    for (const auto& keyCol : sortKeyIndices_) {
        sortKeyTypeIds_.push_back(inputRowType_[keyCol]);
    }
    sortKeySelector_ = KeySelector<RowData*>(sortKeyTypeIds_, sortKeyIndices_);

    rowSerializer_ = std::make_unique<RowDataSerializer>(new omnistream::RowType(false, compatibleColumnTypes_));
}

void AppendOnlyTopNSavepointAdaptor::validateForSave(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedValueStateWithVB(TOPN_STATE_NAME);
}

void AppendOnlyTopNSavepointAdaptor::validateForRestore(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedMapState(TOPN_STATE_NAME);
}

void AppendOnlyTopNSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& keyGroupOffsets,
    FullSnapshotResources& snapshotResources,
    std::string keySerializer)
{
    INFO_RELEASE(
        "AppendOnlyTopNSavepointAdaptor::save - start, sourceMetaCount="
        << snapshotResources.getMetaInfoSnapshots().size());

    auto plan = buildTopNSavePlan(snapshotResources);
    INFO_RELEASE("AppendOnlyTopNSavepointAdaptor::save - plan built, targetMetaCount=" << plan.targetMetaInfos.size());

    VectorBatchSaveFlow::executeSave(*this, plan, stream, keyGroupOffsets, snapshotResources, std::move(keySerializer));
    INFO_RELEASE("AppendOnlyTopNSavepointAdaptor::save complete");
}

void AppendOnlyTopNSavepointAdaptor::restore(
    SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
{
    INFO_RELEASE("AppendOnlyTopNSavepointAdaptor::restore - start");
    VectorBatchRestoreFlow::executeRestore(*this, restoreIterator, backend);
    INFO_RELEASE("AppendOnlyTopNSavepointAdaptor::restore - complete");
}

// ===== VectorBatchSaveHooks 重写 =====

std::vector<VectorBatchSaveStateContext> AppendOnlyTopNSavepointAdaptor::buildSaveStateContexts(
    FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan)
{
    std::vector<VectorBatchSaveStateContext> contexts(snapshotResources.getMetaInfoSnapshots().size());
    INFO_RELEASE(
        "AppendOnlyTopNSavepointAdaptor::buildSaveStateContexts - specCount=" << plan.stateContextSpecs.size()
                                                                              << ", contextSlots=" << contexts.size());
    for (const auto& spec : plan.stateContextSpecs) {
        if (spec.sourceKvStateId < 0 || static_cast<size_t>(spec.sourceKvStateId) >= contexts.size()) {
            INFO_RELEASE(
                "AppendOnlyTopNSavepointAdaptor::buildSaveStateContexts - skip out-of-range sourceKvStateId="
                << spec.sourceKvStateId);
            continue;
        }
        auto& ctx = contexts[spec.sourceKvStateId];
        ctx.writable = true;
        auto mapIt = plan.kvStateIdMapping.find(spec.sourceKvStateId);
        ctx.mappedKvStateId = (mapIt != plan.kvStateIdMapping.end()) ? mapIt->second : spec.sourceKvStateId;
        ctx.logicalStateName = spec.logicalStateName;
        ctx.valueSerializer = spec.valueSerializer;
        if (ctx.logicalStateName == TOPN_STATE_NAME) {
            ctx.vbAccessor =
                snapshotResources.createVectorBatchStateAccessor(spec.logicalStateName, spec.accessorOptions);
            if (ctx.vbAccessor == nullptr) {
                throw std::runtime_error(
                    "AppendOnlyTopNSavepointAdaptor: failed to create VB accessor for state '" + spec.logicalStateName +
                    "'");
            }
            ctx.stateType = VectorBatchStateType::KV_WITH_VB;
        }
        INFO_RELEASE(
            "AppendOnlyTopNSavepointAdaptor::buildSaveStateContexts - accessor created for state='"
            << spec.logicalStateName << "', sourceKvStateId=" << spec.sourceKvStateId
            << ", mappedKvStateId=" << ctx.mappedKvStateId);
    }
    return contexts;
}

void AppendOnlyTopNSavepointAdaptor::convertKVRowData(
    const KeyValueStateIterator::CurrentEntry& entry,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan& plan,
    std::function<void(ConvertedEntry)> output)
{
    // TopN 状态：主表 value 是 comboId 列表，每个 comboId 对应一个 Flink MapState entry
    std::vector<int64_t> comboIds = deserializeComboIdList(entry.value);

    std::vector<int8_t> flinkKey;
    std::unordered_map<std::vector<int8_t>, std::vector<std::unique_ptr<RowData>>, utils::Fnv1a64Hash> sortKeyToRows;
    for (int64_t comboId : comboIds) {
        std::unique_ptr<RowData> row = context.vbAccessor->getRow(comboId);

        // 通过 sortKeySelector_ 获取 sortKey，拼接到原 key 后面作为 MapState key
        std::unique_ptr<RowData> sortKey(sortKeySelector_.getKey(row.get()));

        outputSerializer_.clear();
        BinaryRowDataSerializer sortKeySerializer(sortKey->getArity());
        sortKeySerializer.serialize(sortKey.get(), outputSerializer_);

        flinkKey.clear();
        flinkKey.insert(flinkKey.end(), entry.key.begin(), entry.key.end());
        flinkKey.insert(
            flinkKey.end(), outputSerializer_.getData(), outputSerializer_.getData() + outputSerializer_.getPosition());

        sortKeyToRows[flinkKey].push_back(std::move(row));
    }

    std::vector<int8_t> flinkValue;
    for (auto& pair : sortKeyToRows) {
        const auto& flinkKey = pair.first;
        auto& rows = pair.second;

        // ListSerializer format: [int size][element1][element2]...
        outputSerializer_.clear();
        outputSerializer_.writeBoolean(false);
        outputSerializer_.writeInt(static_cast<uint32_t>(rows.size()));

        for (const auto& row : rows) {
            rowSerializer_->serialize(static_cast<void*>(row.get()), outputSerializer_);
        }

        flinkValue.clear();
        flinkValue.insert(
            flinkValue.end(),
            outputSerializer_.getData(),
            outputSerializer_.getData() + outputSerializer_.getPosition());

        ConvertedEntry converted;
        converted.context = &context;
        converted.keyBytes = flinkKey;
        converted.valueBytes = flinkValue;
        output(std::move(converted));
    }
}

// ===== 类自有公共方法 =====

StateMetaInfoSnapshot AppendOnlyTopNSavepointAdaptor::buildOmniMainMetaInfo(
    int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo)
{
    (void)kvStateId;
    if (flinkMetaInfo.getName() == TOPN_STATE_NAME) {
        std::unordered_map<std::string, std::string> omniOptions;
        omniOptions[StateMetaInfoSnapshot::commonOptionsKeyToString(
            StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = "VALUE";

        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> omniConfigSnapshotMap;

        std::unordered_map<std::string, TypeSerializer*> omniSerializerMap;
        omniSerializerMap.emplace("NAMESPACE_SERIALIZER", VoidNamespaceSerializer::INSTANCE);
        omniSerializerMap.emplace("VALUE_SERIALIZER", SortedVectorLong::INSTANCE);

        return StateMetaInfoSnapshot(
            TOPN_STATE_NAME,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            omniOptions,
            omniConfigSnapshotMap,
            omniSerializerMap);
    }
    return flinkMetaInfo;
}

RestoreStateType AppendOnlyTopNSavepointAdaptor::getStateType(const StateMetaInfoSnapshot& metaInfo)
{
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
        return RestoreStateType::PQ;
    } else if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
        if (metaInfo.getName() == TOPN_STATE_NAME) {
            return RestoreStateType::KV_WITH_VB;
        }
        return RestoreStateType::KV;
    }
    return RestoreStateType::UNSUPPORT;
}

void AppendOnlyTopNSavepointAdaptor::retrieveKVRowData(
    const std::vector<int8_t>& keyBytes, const std::vector<int8_t>& valueBytes, int kvStateId, RestoreKVStateVB* writer)
{
    (void)kvStateId;

    // 分离前缀和 sortKey
    std::vector<int8_t> prefix;

    if (sortKeyLength != -1) {
        prefix.assign(keyBytes.begin(), keyBytes.end() - sortKeyLength);
    }

    // 前缀变化时，刷新上一组的 comboId 列表
    if (!currentRestoreComboIds_.empty() && prefix != currentRestorePrefix_) {
        currentRestorePrefix_.clear();
        currentRestoreComboIds_.clear();
    }

    // 解析 Flink MapState value: [null marker][list size][RowData items...]
    DataInputDeserializer valInput(
        reinterpret_cast<const uint8_t*>(valueBytes.data()), static_cast<int>(valueBytes.size()), 0);

    // 跳过 null 标记
    valInput.readBoolean();

    // 读取 list size
    int32_t listSize = valInput.readInt();

    for (int32_t i = 0; i < listSize; ++i) {
        // 格式：[int32 size][size bytes]
        size_t rowStart = valInput.getPosition();
        int32_t rowSize = valInput.readInt();
        valInput.setPosition(rowStart + sizeof(int32_t) + rowSize);

        // 构造 [int32 size][data] 的连续字节
        std::vector<int8_t> rowBytes(
            valueBytes.data() + rowStart, valueBytes.data() + rowStart + sizeof(int32_t) + rowSize);

        if (sortKeyLength == -1) {
            DataInputDeserializer sortKeyInput(rowBytes.data(), static_cast<int>(rowBytes.size()), 0);
            auto rowData = static_cast<RowData*>(rowSerializer_->deserialize(sortKeyInput));

            std::unique_ptr<RowData> sortKey(sortKeySelector_.getKey(rowData));

            BinaryRowDataSerializer sortKeySerializer(sortKey->getArity());
            DataOutputSerializer sortKeyOutput;
            OutputBufferStatus sortKeyBufStatus;
            sortKeyOutput.setBackendBuffer(&sortKeyBufStatus);
            sortKeySerializer.serialize(sortKey.get(), sortKeyOutput);

            sortKeyLength = sortKeyOutput.getPosition();
            prefix.assign(keyBytes.begin(), keyBytes.end() - sortKeyLength);
        }

        // 追加到 VectorBatch，获取 comboId
        RowDataView rowView;
        rowView.valueBytes = &rowBytes;
        rowView.columnTypes = &inputRowType_;
        int64_t comboId = writer->appendRowToVectorBatch(rowView);

        // 记录当前前缀和 comboId
        if (currentRestoreComboIds_.empty()) {
            currentRestorePrefix_ = prefix;
        }
        currentRestoreComboIds_.push_back(comboId);
    }

    if (currentRestoreComboIds_.empty()) {
        return;
    }

    // 序列化 comboId 列表
    std::vector<int8_t> comboIds = serializeComboIdList(currentRestoreComboIds_);

    // 写入主表
    ByteView valueView(comboIds.data(), comboIds.size());
    writer->writeEntry<ByteView>(currentRestorePrefix_, valueView);
}

// ===== 私有工具方法 =====

std::shared_ptr<StateMetaInfoSnapshot> AppendOnlyTopNSavepointAdaptor::buildFlinkMainMetaInfo(
    std::shared_ptr<StateMetaInfoSnapshot> omniMetaInfo)
{
    std::unordered_map<std::string, std::string> omniOptions;
    omniOptions[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = "MAP";

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> omniConfigSnapshotMap;

    std::unordered_map<std::string, TypeSerializer*> omniSerializerMap;
    TypeSerializer* nsSerializer = omniMetaInfo->getTypeSerializer("namespaceSerializer");
    omniSerializerMap.emplace("namespaceSerializer", nsSerializer);

    // key
    std::vector<std::string> typeNames = {compatibleColumnTypes_[sortKeyIndices_[0]]};
    auto keyRowSerializer = std::make_unique<RowDataSerializer>(new omnistream::RowType(false, typeNames));

    // value
    auto valRowSerializer = std::make_unique<RowDataSerializer>(new omnistream::RowType(false, compatibleColumnTypes_));
    auto listSerializer = std::make_unique<ListSerializer>(valRowSerializer.release());

    stateSerializer_ = std::make_unique<MapSerializer>(keyRowSerializer.release(), listSerializer.release());

    omniSerializerMap.emplace("stateSerializer", stateSerializer_.get());

    return std::make_shared<StateMetaInfoSnapshot>(
        TOPN_STATE_NAME,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        omniOptions,
        omniConfigSnapshotMap,
        omniSerializerMap);
}

VectorBatchSavePlan AppendOnlyTopNSavepointAdaptor::buildTopNSavePlan(FullSnapshotResources& snapshotResources)
{
    VectorBatchSavePlan plan;
    auto metaInfos = snapshotResources.getMetaInfoSnapshots();
    plan.keyGroupRange = snapshotResources.getKeyGroupRange();
    plan.kvStateIdMapping = buildKvStateIdMapping(metaInfos);

    for (size_t i = 0; i < metaInfos.size(); ++i) {
        auto omniMeta = metaInfos[i];
        if (omniMeta == nullptr) {
            continue;
        }
        const std::string& stateName = omniMeta->getName();
        if (VectorBatchSaveTools::isVbStateName(stateName)) {
            continue;
        }

        VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = static_cast<int>(i);
        spec.logicalStateName = stateName;
        plan.mainStateIds.push_back(static_cast<int>(i));

        if (stateName == TOPN_STATE_NAME) {
            plan.targetMetaInfos.push_back(buildFlinkMainMetaInfo(omniMeta));
            spec.valueSerializer = stateSerializer_.get();
        } else {
            plan.targetMetaInfos.push_back(omniMeta);
            spec.valueSerializer = omniMeta->getTypeSerializer("stateSerializer");
        }

        spec.accessorOptions.maxDecodedBatchCacheBytes = VB_SAVE_CACHE_BYTES;
        plan.stateContextSpecs.push_back(spec);
    }

    INFO_RELEASE(
        "AppendOnlyTopNSavepointAdaptor::buildTopNSavePlan - targetMetaCount="
        << plan.targetMetaInfos.size() << ", mainStateCount=" << plan.mainStateIds.size());
    return plan;
}

std::unordered_map<int, int> AppendOnlyTopNSavepointAdaptor::buildKvStateIdMapping(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfoSnapshots) const
{
    std::unordered_map<int, int> mapping;
    int newKvStateId = 0;
    for (size_t i = 0; i < metaInfoSnapshots.size(); ++i) {
        const auto& meta = metaInfoSnapshots[i];
        if (meta != nullptr && !VectorBatchSaveTools::isVbStateName(meta->getName())) {
            mapping[static_cast<int>(i)] = newKvStateId++;
        }
    }
    return mapping;
}

std::vector<int64_t> AppendOnlyTopNSavepointAdaptor::deserializeComboIdList(ByteView value)
{
    std::vector<int64_t> comboIds;
    DataInputDeserializer input(value.data(), static_cast<int>(value.size()), 0);
    auto* vecPtr = static_cast<std::vector<long>*>(SortedVectorLong::INSTANCE->deserialize(input));
    if (vecPtr != nullptr) {
        comboIds.reserve(vecPtr->size());
        for (long v : *vecPtr) {
            comboIds.push_back(static_cast<int64_t>(v));
        }
        delete vecPtr;
    }
    return comboIds;
}

std::vector<int8_t> AppendOnlyTopNSavepointAdaptor::serializeComboIdList(std::vector<int64_t>& comboIds)
{
    outputSerializer_.clear();
    SortedVectorLong::INSTANCE->serialize(&comboIds, outputSerializer_);
    auto* data = outputSerializer_.getData();
    return std::vector<int8_t>(data, data + outputSerializer_.getPosition());
}
} // namespace omnistream

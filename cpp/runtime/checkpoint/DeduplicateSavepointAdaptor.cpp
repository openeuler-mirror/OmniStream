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

#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "DeduplicateSavepointAdaptor.h"

#include "core/api/common/state/StateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/checkpoint/StateMetaInfoValidator.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "state/bridge/OmniTaskBridge.h"
#include "table/types/logical/RowType.h"

namespace omnistream {
DeduplicateSavepointAdaptor::~DeduplicateSavepointAdaptor()
{
    delete mainValueSerializer_;
    mainValueSerializer_ = nullptr;
}

void DeduplicateSavepointAdaptor::prepareForRestore(const nlohmann::json& operatorDescription)
{
    if (mainValueSerializer_ == nullptr) {
        mainValueSerializer_ = new LongSerializer();
    }
    // 从算子描述中解析恢复方向的列类型并缓存
    restoreColumnTypes_ = convertToDataTypes(parseInputTypes(operatorDescription));
}

void DeduplicateSavepointAdaptor::prepareForSave(const nlohmann::json& operatorDescription)
{
    compatibleColumnTypes_ = parseInputTypes(operatorDescription);
    buildStateSerializerMap();
}

void DeduplicateSavepointAdaptor::validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    // 保存方向 source：Flink logical deduplicate-state（keyed value）。
    // PRIORITY_QUEUE 状态通过 requirePriorityQueueStates 显式消费，
    // 由 CompatibleFullSnapshotAsyncWriter 在 adaptor->save() 返回后单独写入。
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedValueStateWithVB(DEDUPLICATE_STATE_NAME);
    validator.requireNoMoreStates();
}

void DeduplicateSavepointAdaptor::validateForRestore(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    // 恢复方向 source：Flink logical deduplicate-state（keyed value，无 VB 侧表）。
    // PRIORITY_QUEUE 状态通过 requirePriorityQueueStates 显式消费，
    // 由 CompatibleFullRestoreOperation 的 Pass 2 单独恢复。
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedValueState(DEDUPLICATE_STATE_NAME);
    validator.requirePriorityQueueStates();
    validator.requireNoMoreStates();
}

std::vector<std::string> DeduplicateSavepointAdaptor::parseInputTypes(const nlohmann::json& operatorDescription)
{
    std::vector<std::string> result;
    if (operatorDescription.contains("inputTypes") && operatorDescription["inputTypes"].is_array()) {
        for (const auto& type : operatorDescription["inputTypes"]) {
            if (type.is_string()) {
                result.push_back(type.get<std::string>());
            }
        }
    }
    return result;
}

std::vector<omniruntime::type::DataTypeId> DeduplicateSavepointAdaptor::convertToDataTypes(
    const std::vector<std::string>& typeNames)
{
    std::vector<omniruntime::type::DataTypeId> result;
    for (const auto& name : typeNames) {
        result.push_back(LogicalType::flinkTypeToOmniTypeId(name));
    }
    return result;
}

void DeduplicateSavepointAdaptor::buildStateSerializerMap()
{
    stateSerializerMap_.clear();
    if (compatibleColumnTypes_.empty()) {
        INFO_RELEASE("Error:DeduplicateSavepointAdaptor::buildStateSerializerMap - compatibleColumnTypes_ is empty");
        throw std::runtime_error(
            "DeduplicateSavepointAdaptor: no column types to build state serializer, "
            "check prepareForSave/operatorDescription");
    }
    auto* serializer = new RowDataSerializer(new omnistream::RowType(false, compatibleColumnTypes_));
    stateSerializerMap_[DEDUPLICATE_STATE_NAME] = std::shared_ptr<RowDataSerializer>(serializer);
}

TypeSerializer* DeduplicateSavepointAdaptor::getRecordStateSerializer()
{
    auto it = stateSerializerMap_.find(DEDUPLICATE_STATE_NAME);
    return it != stateSerializerMap_.end() ? it->second.get() : nullptr;
}

std::unordered_map<int, int> DeduplicateSavepointAdaptor::buildKvStateIdMapping(
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

// ===== 保存方向：新模型 =====

VectorBatchSavePlan DeduplicateSavepointAdaptor::buildDeduplicateSavePlan(FullSnapshotResources& snapshotResources)
{
    VectorBatchSavePlan plan;
    const auto& metaInfos = snapshotResources.getMetaInfoSnapshots();
    plan.keyGroupRange = snapshotResources.getKeyGroupRange();
    plan.kvStateIdMapping = buildKvStateIdMapping(metaInfos);

    for (size_t i = 0; i < metaInfos.size(); ++i) {
        const auto& omniMeta = metaInfos[i];
        if (omniMeta == nullptr) {
            continue;
        }
        const std::string& stateName = omniMeta->getName();
        if (VectorBatchSaveTools::isVbStateName(stateName)) {
            // VB 侧表不写入 target Flink metadata，只作为 accessor 的 source side table
            continue;
        }

        TypeSerializer* omniNsSer = omniMeta->getTypeSerializer("namespaceSerializer");
        TypeSerializer* flinkValueSer = getRecordStateSerializer();
        if (omniNsSer == nullptr || flinkValueSer == nullptr) {
            INFO_RELEASE(
                "Error:DeduplicateSavepointAdaptor::buildDeduplicateSavePlan - state="
                << stateName << ", omniNsSer=" << omniNsSer << ", flinkValueSer=" << flinkValueSer);
            throw std::runtime_error(
                "DeduplicateSavepointAdaptor: missing namespace/value serializer for state '" + stateName + "'");
        }

        auto stateTypeString = omniMeta->getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
        StateDescriptor::Type stateType = StateDescriptor::StringToType(stateTypeString);

        RegisteredKeyValueStateBackendMetaInfo convertedMetaInfo(stateType, stateName, omniNsSer, flinkValueSer);
        plan.targetMetaInfos.push_back(convertedMetaInfo.snapshot());
        plan.mainStateIds.push_back(static_cast<int>(i));

        VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = static_cast<int>(i);
        spec.logicalStateName = stateName;
        spec.valueSerializer = flinkValueSer;
        spec.accessorOptions.maxDecodedBatchCacheBytes = VB_SAVE_CACHE_BYTES;
        plan.stateContextSpecs.push_back(std::move(spec));
    }

    return plan;
}

std::vector<VectorBatchSaveStateContext> DeduplicateSavepointAdaptor::buildSaveStateContexts(
    FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan)
{
    std::vector<VectorBatchSaveStateContext> contexts(snapshotResources.getMetaInfoSnapshots().size());
    for (const auto& spec : plan.stateContextSpecs) {
        if (spec.sourceKvStateId < 0 || static_cast<size_t>(spec.sourceKvStateId) >= contexts.size()) {
            INFO_RELEASE(
                "Error:DeduplicateSavepointAdaptor::buildSaveStateContexts - out-of-range sourceKvStateId="
                << spec.sourceKvStateId);
            throw std::runtime_error(
                "DeduplicateSavepointAdaptor: sourceKvStateId=" + std::to_string(spec.sourceKvStateId) +
                " out of range [0, " + std::to_string(contexts.size()) + ")");
        }
        auto& ctx = contexts[spec.sourceKvStateId];
        ctx.writable = true;
        ctx.stateType = VectorBatchStateType::KV_WITH_VB;
        auto mapIt = plan.kvStateIdMapping.find(spec.sourceKvStateId);
        ctx.mappedKvStateId = (mapIt != plan.kvStateIdMapping.end()) ? mapIt->second : spec.sourceKvStateId;
        ctx.logicalStateName = spec.logicalStateName;
        ctx.valueSerializer = spec.valueSerializer;
        ctx.vbAccessor = snapshotResources.createVectorBatchStateAccessor(spec.logicalStateName, spec.accessorOptions);
        if (ctx.vbAccessor == nullptr) {
            INFO_RELEASE(
                "Error:DeduplicateSavepointAdaptor: failed to create VB accessor for state '" << spec.logicalStateName
                                                                                              << "'");
            throw std::runtime_error(
                "DeduplicateSavepointAdaptor: failed to create VB accessor for state '" + spec.logicalStateName + "'");
        }
    }
    return contexts;
}

omnistream::ComboId DeduplicateSavepointAdaptor::parseVectorBatchReference(
    ByteView value, const VectorBatchSaveStateContext& context, const VectorBatchSavePlan& plan)
{
    if (value.size() < sizeof(int64_t)) {
        INFO_RELEASE(
            "Error:DeduplicateSavepointAdaptor::parseVectorBatchReference - state="
            << context.logicalStateName << ", valueSize=" << value.size() << ", requiredSize=" << sizeof(int64_t));
        throw std::runtime_error(
            "DeduplicateSavepointAdaptor: main state value too short to parse comboId, state=" +
            context.logicalStateName);
    }
    return VectorBatchSaveTools::parseComboId(value);
}

std::vector<int8_t> DeduplicateSavepointAdaptor::encodeFlinkLogicalValue(
    const KeyValueStateIterator::CurrentEntry& entry,
    RowData& row,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan& plan)
{
    return VectorBatchSaveTools::serializeRowData(&row, context.valueSerializer);
}

void DeduplicateSavepointAdaptor::convertKVRowData(
    const KeyValueStateIterator::CurrentEntry& entry,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan& plan,
    std::function<void(ConvertedEntry)> output)
{
    // Deduplicate 为 1:1 映射，解析 comboId、解引用 VB RowData、编码后输出
    auto comboId = parseVectorBatchReference(ByteView(entry.value.data(), entry.value.size()), context, plan);
    int64_t batchId = VectorBatchUtil::getVectorBatchId(comboId);
    int32_t rowId = VectorBatchUtil::getRowId(comboId);
    if (context.vbAccessor) {
        auto row = context.vbAccessor->getRow(batchId, rowId);
        if (row) {
            ConvertedEntry converted;
            converted.context = &context;
            converted.keyBytes = encodeFlinkLogicalKey(entry, *row, context, plan);
            converted.valueBytes = encodeFlinkLogicalValue(entry, *row, context, plan);
            converted.comboRef = comboId;
            output(std::move(converted));
        } else {
            INFO_RELEASE(
                "Error:DeduplicateSavepointAdaptor::convertKVRowData - null row for comboId="
                << comboId << ", batchId=" << batchId << ", rowId=" << rowId);
            throw std::runtime_error(
                "DeduplicateSavepointAdaptor: null row for comboId=" + std::to_string(comboId) +
                ", batchId=" + std::to_string(batchId) + ", rowId=" + std::to_string(rowId));
        }
    } else {
        INFO_RELEASE(
            "Error:DeduplicateSavepointAdaptor::convertKVRowData - null vbAccessor for state="
            << context.logicalStateName << ", comboId=" << comboId);
        throw std::runtime_error(
            "DeduplicateSavepointAdaptor: null vbAccessor for state='" + context.logicalStateName +
            "', comboId=" + std::to_string(comboId));
    }
}

void DeduplicateSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& keyGroupOffsets,
    FullSnapshotResources& snapshotResources,
    std::string keySerializer)
{
    auto plan = buildDeduplicateSavePlan(snapshotResources);
    VectorBatchSaveFlow::executeSave(*this, plan, stream, keyGroupOffsets, snapshotResources, std::move(keySerializer));
}

// ===== 恢复方向：plan + state dispatch 模型 =====

namespace {
// Deduplicate 的轻量 RestorePlan：包含 deduplicate-state 的 kvStateId 和 PQ timer 状态
struct DeduplicateRestorePlan : RestorePlan {
    int deduplicateKvStateId = -1;
    std::vector<int> pqKvStateIds;
    std::vector<omniruntime::type::DataTypeId> cachedColumnTypes;
    int vbBatchSize = VB_RESTORE_BATCH_SIZE;
    const StateMetaInfoSnapshot* flinkMetaInfo = nullptr; // 缓存的 Flink 元信息引用

    std::vector<int> sourceKvStateIds() const override
    {
        std::vector<int> ids;
        if (deduplicateKvStateId >= 0) {
            ids.push_back(deduplicateKvStateId);
        }
        ids.insert(ids.end(), pqKvStateIds.begin(), pqKvStateIds.end());
        return ids;
    }

    std::vector<omniruntime::type::DataTypeId> columnTypes(int kvStateId) const override
    {
        (void)kvStateId;
        return cachedColumnTypes;
    }

    int batchSize(int kvStateId) const override
    {
        (void)kvStateId;
        return vbBatchSize;
    }
};
} // namespace

std::unique_ptr<RestorePlan> DeduplicateSavepointAdaptor::buildRestorePlan(
    const std::vector<StateMetaInfoSnapshot>& flinkMetaInfos)
{
    auto plan = std::make_unique<DeduplicateRestorePlan>();
    plan->cachedColumnTypes = restoreColumnTypes_;
    plan->vbBatchSize = VB_RESTORE_BATCH_SIZE;

    for (size_t i = 0; i < flinkMetaInfos.size(); ++i) {
        if (flinkMetaInfos[i].getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::KEY_VALUE &&
            flinkMetaInfos[i].getName() == DEDUPLICATE_STATE_NAME) {
            plan->deduplicateKvStateId = static_cast<int>(i);
            plan->flinkMetaInfo = &flinkMetaInfos[i]; // 缓存引用供 buildOmniMainMetaInfo 使用
        } else if (flinkMetaInfos[i].getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
            plan->pqKvStateIds.push_back(static_cast<int>(i));
        }
    }

    if (plan->deduplicateKvStateId < 0) {
        INFO_RELEASE("Error:DeduplicateSavepointAdaptor::buildRestorePlan - deduplicate-state not found in metadata");
        throw std::runtime_error(
            "DeduplicateSavepointAdaptor: Flink logical state '" + std::string(DEDUPLICATE_STATE_NAME) +
            "' not found in metadata");
    }
    return plan;
}

RestoreStateType DeduplicateSavepointAdaptor::getStateType(int kvStateId, const RestorePlan& plan)
{
    auto& dedupPlan = static_cast<const DeduplicateRestorePlan&>(plan);
    for (int pqId : dedupPlan.pqKvStateIds) {
        if (pqId == kvStateId) {
            return RestoreStateType::PQ;
        }
    }
    return RestoreStateType::KV_WITH_VB;
}

StateMetaInfoSnapshot DeduplicateSavepointAdaptor::buildOmniMainMetaInfo(int kvStateId, const RestorePlan& plan)
{
    auto& dedupPlan = static_cast<const DeduplicateRestorePlan&>(plan);
    if (dedupPlan.flinkMetaInfo != nullptr) {
        return VectorBatchRestoreUtil::buildOmniMainMetaInfo(*dedupPlan.flinkMetaInfo, mainValueSerializer_);
    }
    // 回退：无 Flink metaInfo 时直接构造（不会发生，仅防御）
    std::unordered_map<std::string, std::string> options;
    options[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = "VALUE";
    return StateMetaInfoSnapshot(
        DEDUPLICATE_STATE_NAME,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>());
}

void DeduplicateSavepointAdaptor::restore(
    SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
{
    VectorBatchRestoreFlow::executeRestore(*this, restoreIterator, backend);
}

} // namespace omnistream

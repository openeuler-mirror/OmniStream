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

#include "GroupWindowAggSavepointAdaptor.h"

#include <stdexcept>
#include <utility>

#include "StateMetaInfoValidator.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "runtime/state/heap/HeapFullSnapshotResources.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "table/runtime/operators/window/TimeWindow.h"

namespace omnistream {

namespace {

std::string trimWindowType(std::string value)
{
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return "";
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string getWindowTypeName(const nlohmann::json& description)
{
    for (const char* field : {"windowKind", "windowTypeName"}) {
        if (description.contains(field) && description[field].is_string()) {
            return trimWindowType(description[field].get<std::string>());
        }
    }

    if (!description.contains("windowType")) {
        return "";
    }
    const auto& windowType = description["windowType"];
    if (windowType.is_object()) {
        for (const char* field : {"kind", "type"}) {
            if (windowType.contains(field) && windowType[field].is_string()) {
                return trimWindowType(windowType[field].get<std::string>());
            }
        }
        return "";
    }
    if (!windowType.is_string()) {
        return "";
    }

    std::string typeName = windowType.get<std::string>();
    const auto argumentsStart = typeName.find('(');
    if (argumentsStart != std::string::npos) {
        typeName.resize(argumentsStart);
    }
    return trimWindowType(std::move(typeName));
}

} // namespace

void GroupWindowAggSavepointAdaptor::prepare(const nlohmann::json& description)
{
    const std::string windowTypeName = getWindowTypeName(description);
    if (windowTypeName == "SessionGroupWindow" || windowTypeName == "SESSION") {
        requireSessionWindowMapping_ = true;
        return;
    }
    if (windowTypeName == "TumblingGroupWindow" || windowTypeName == "SlidingGroupWindow" ||
        windowTypeName == "TUMBLE" || windowTypeName == "HOP") {
        requireSessionWindowMapping_ = false;
        return;
    }

    ERROR_RELEASE(
        "GroupWindowAggSavepointAdaptor: missing or unsupported window type, parsedType="
        << (windowTypeName.empty() ? "<empty>" : windowTypeName));
    throw std::runtime_error(
        "GroupWindowAggSavepointAdaptor: missing or unsupported window type: " +
        (windowTypeName.empty() ? std::string("<empty>") : windowTypeName));
}

void GroupWindowAggSavepointAdaptor::prepareForSave(const nlohmann::json& description)
{
    prepare(description);
}

void GroupWindowAggSavepointAdaptor::prepareForRestore(const nlohmann::json& description)
{
    prepare(description);
}

void GroupWindowAggSavepointAdaptor::validateForSave(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    // Omni Heap registers an internal VectorBatch side table for every logical
    // keyed state. Consume it during source validation, but do not expose it in
    // the Flink-compatible savepoint metadata or payload.
    validator.requireKeyedValueStateWithVB(WINDOW_AGG_STATE_NAME);
    validator.requirePriorityQueueStates();
    if (requireSessionWindowMapping_) {
        validator.requireKeyedMapStateWithVB(SESSION_WINDOW_MAPPING_STATE_NAME);
    }
    validateSerializers(validator, metaInfos);
    validator.requireNoMoreStates();
}

void GroupWindowAggSavepointAdaptor::validateForRestore(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedValueState(WINDOW_AGG_STATE_NAME);
    validator.requirePriorityQueueStates();
    if (requireSessionWindowMapping_) {
        validator.requireKeyedMapState(SESSION_WINDOW_MAPPING_STATE_NAME);
    }
    validateSerializers(validator, metaInfos);
    validator.requireNoMoreStates();
}

void GroupWindowAggSavepointAdaptor::validateSerializers(
    const StateMetaInfoValidator& validator, const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) const
{
    const auto& windowAggMeta = validator.get(WINDOW_AGG_STATE_NAME);
    auto* namespaceSerializer = windowAggMeta->getNamespaceSerializer();
    auto* valueSerializer = windowAggMeta->getValueSerializer();
    if (namespaceSerializer == nullptr || namespaceSerializer->getBackendId() != BackendDataType::TIME_WINDOW_BK ||
        valueSerializer == nullptr || valueSerializer->getBackendId() != BackendDataType::ROW_BK) {
        ERROR_RELEASE(
            "GroupWindowAggSavepointAdaptor: window-aggs requires TimeWindow namespace and RowData value "
            "serializers");
        throw std::runtime_error(
            "GroupWindowAggSavepointAdaptor: window-aggs requires TimeWindow namespace and RowData value serializers");
    }

    if (requireSessionWindowMapping_) {
        const auto& mappingMeta = validator.get(SESSION_WINDOW_MAPPING_STATE_NAME);
        auto* mappingSerializer = mappingMeta->getValueSerializer();
        auto* mapSerializer = dynamic_cast<MapSerializer*>(mappingSerializer);
        if (mapSerializer == nullptr || mapSerializer->getKeySerializer() == nullptr ||
            mapSerializer->getValueSerializer() == nullptr ||
            mapSerializer->getKeySerializer()->getBackendId() != BackendDataType::TIME_WINDOW_BK ||
            mapSerializer->getValueSerializer()->getBackendId() != BackendDataType::TIME_WINDOW_BK) {
            ERROR_RELEASE(
                "GroupWindowAggSavepointAdaptor: session-window-mapping requires Map<TimeWindow, TimeWindow>");
            throw std::runtime_error(
                "GroupWindowAggSavepointAdaptor: session-window-mapping requires Map<TimeWindow, TimeWindow>");
        }
    }

    for (const auto& meta : metaInfos) {
        if (meta == nullptr || meta->getBackendStateType() != StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
            continue;
        }
        auto* timerSerializer = meta->getValueSerializer();
        const std::string serializerName =
            timerSerializer == nullptr || timerSerializer->getName() == nullptr ? "" : timerSerializer->getName();
        if (timerSerializer == nullptr || serializerName != "TimerSerializer") {
            ERROR_RELEASE(
                "GroupWindowAggSavepointAdaptor: timer state '"
                << meta->getName() << "' requires TimerSerializer, actual=" << serializerName);
            throw std::runtime_error(
                "GroupWindowAggSavepointAdaptor: timer state '" + meta->getName() + "' requires TimerSerializer");
        }
    }
}

VectorBatchSavePlan GroupWindowAggSavepointAdaptor::buildSavePlan(FullSnapshotResources& resources)
{
    VectorBatchSavePlan plan;
    const auto& metas = resources.getMetaInfoSnapshots();
    plan.keyGroupRange = resources.getKeyGroupRange();
    const bool heapSnapshot = dynamic_cast<HeapFullSnapshotResources*>(&resources) != nullptr;

    int targetId = 0;
    for (size_t i = 0; i < metas.size(); ++i) {
        const auto& meta = metas[i];
        // States whose names end in "vb" are Omni-internal VectorBatch side
        // tables. They must never become Flink-compatible column families.
        if (meta == nullptr) {
            continue;
        }
        if (VectorBatchSaveTools::isVbStateName(meta->getName())) {
            continue;
        }
        const auto backendType = meta->getBackendStateType();
        if (backendType != StateMetaInfoSnapshot::BackendStateType::KEY_VALUE &&
            backendType != StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
            ERROR_RELEASE(
                "GroupWindowAggSavepointAdaptor: unsupported backend state type for state="
                << meta->getName() << ", backendType=" << static_cast<int>(backendType));
            throw std::runtime_error(
                "GroupWindowAggSavepointAdaptor: unsupported backend state type for " + meta->getName());
        }

        plan.kvStateIdMapping[static_cast<int>(i)] = targetId++;
        plan.mainStateIds.push_back(static_cast<int>(i));
        plan.targetMetaInfos.push_back(meta);

        VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = static_cast<int>(i);
        spec.logicalStateName = meta->getName();
        spec.stateType = backendType == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE
                             ? VectorBatchStateType::PQ
                             : VectorBatchStateType::KV;
        spec.valueSerializer = meta->getValueSerializer();

        const auto stateType =
            StateDescriptor::StringToType(meta->getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE));
        if (heapSnapshot && meta->getName() == SESSION_WINDOW_MAPPING_STATE_NAME &&
            stateType == StateDescriptor::Type::MAP) {
            auto* mapSerializer = dynamic_cast<MapSerializer*>(spec.valueSerializer);
            if (mapSerializer == nullptr || mapSerializer->getKeySerializer() == nullptr ||
                mapSerializer->getValueSerializer() == nullptr) {
                ERROR_RELEASE("GroupWindowAggSavepointAdaptor: Heap session-window-mapping requires MapSerializer");
                throw std::runtime_error(
                    "GroupWindowAggSavepointAdaptor: Heap session-window-mapping requires MapSerializer");
            }
            spec.stateType = VectorBatchStateType::KV_MAP_TRANSFORM;
            spec.mapKeySerializer = mapSerializer->getKeySerializer();
            spec.mapValueSerializer = mapSerializer->getValueSerializer();
        }
        plan.stateContextSpecs.push_back(std::move(spec));
    }
    return plan;
}

std::vector<VectorBatchSaveStateContext> GroupWindowAggSavepointAdaptor::buildSaveStateContexts(
    FullSnapshotResources& resources, const VectorBatchSavePlan& plan)
{
    std::vector<VectorBatchSaveStateContext> contexts(resources.getMetaInfoSnapshots().size());
    for (const auto& spec : plan.stateContextSpecs) {
        auto& context = contexts.at(static_cast<size_t>(spec.sourceKvStateId));
        context.writable = true;
        context.mappedKvStateId = plan.kvStateIdMapping.at(spec.sourceKvStateId);
        context.logicalStateName = spec.logicalStateName;
        context.stateType = spec.stateType;
        context.valueSerializer = spec.valueSerializer;
        context.mapKeySerializer = spec.mapKeySerializer;
        context.mapValueSerializer = spec.mapValueSerializer;
    }
    return contexts;
}

template <typename Emit>
void GroupWindowAggSavepointAdaptor::convertKVRowData(
    const KeyValueStateIterator::CurrentEntry& entry,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan&,
    Emit&& output)
{
    if (context.stateType != VectorBatchStateType::KV_MAP_TRANSFORM || context.mapKeySerializer == nullptr ||
        context.mapValueSerializer == nullptr) {
        ERROR_RELEASE(
            "GroupWindowAggSavepointAdaptor: invalid session mapping conversion context, state="
            << context.logicalStateName << ", stateType=" << static_cast<int>(context.stateType)
            << ", mapKeySerializer=" << context.mapKeySerializer
            << ", mapValueSerializer=" << context.mapValueSerializer);
        throw std::runtime_error(
            "GroupWindowAggSavepointAdaptor: only Heap session-window-mapping requires save conversion");
    }

    DataInputDeserializer input(entry.value.data(), static_cast<int>(entry.value.size()), 0);
    if (input.Available() < static_cast<int>(sizeof(uint32_t))) {
        ERROR_RELEASE(
            "GroupWindowAggSavepointAdaptor: truncated session-window-mapping size, available=" << input.Available());
        throw std::runtime_error("GroupWindowAggSavepointAdaptor: truncated session-window-mapping size");
    }
    const int mapSize = input.readInt();
    if (mapSize < 0) {
        ERROR_RELEASE("GroupWindowAggSavepointAdaptor: negative session-window-mapping size=" << mapSize);
        throw std::runtime_error("GroupWindowAggSavepointAdaptor: negative session-window-mapping size");
    }
    for (int i = 0; i < mapSize; ++i) {
        std::unique_ptr<TimeWindow> mapKey(static_cast<TimeWindow*>(context.mapKeySerializer->deserialize(input)));
        if (mapKey == nullptr) {
            ERROR_RELEASE(
                "GroupWindowAggSavepointAdaptor: failed to deserialize session window key, mapIndex="
                << i << ", mapSize=" << mapSize);
            throw std::runtime_error("GroupWindowAggSavepointAdaptor: failed to deserialize session window key");
        }
        const bool isNull = input.readBoolean();

        DataOutputSerializer keyOutput(static_cast<int>(entry.key.size()) + 64);
        keyOutput.write(
            const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(entry.key.data())),
            static_cast<int>(entry.key.size()),
            0,
            static_cast<int>(entry.key.size()));
        context.mapKeySerializer->serialize(mapKey.get(), keyOutput);

        DataOutputSerializer valueOutput(32);
        valueOutput.writeBoolean(isNull);
        if (!isNull) {
            std::unique_ptr<TimeWindow> mapValue(
                static_cast<TimeWindow*>(context.mapValueSerializer->deserialize(input)));
            if (mapValue == nullptr) {
                ERROR_RELEASE(
                    "GroupWindowAggSavepointAdaptor: failed to deserialize session window value, mapIndex="
                    << i << ", mapSize=" << mapSize);
                throw std::runtime_error("GroupWindowAggSavepointAdaptor: failed to deserialize session window value");
            }
            context.mapValueSerializer->serialize(mapValue.get(), valueOutput);
        }

        ConvertedEntry converted;
        converted.context = &context;
        converted.keyBytes.assign(keyOutput.getData(), keyOutput.getData() + keyOutput.getPosition());
        converted.valueBytes.assign(valueOutput.getData(), valueOutput.getData() + valueOutput.getPosition());
        output(std::move(converted));
    }
    if (input.Available() != 0) {
        ERROR_RELEASE(
            "GroupWindowAggSavepointAdaptor: trailing bytes in session-window-mapping, remaining="
            << input.Available());
        throw std::runtime_error(
            "GroupWindowAggSavepointAdaptor: trailing bytes in session-window-mapping, remaining=" +
            std::to_string(input.Available()));
    }
}

void GroupWindowAggSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& offsets,
    FullSnapshotResources& resources,
    std::string keySerializer)
{
    auto plan = buildSavePlan(resources);
    VectorBatchSaveFlow::executeSave(*this, plan, stream, offsets, resources, std::move(keySerializer));
}

RestoreStateType GroupWindowAggSavepointAdaptor::getStateType(const StateMetaInfoSnapshot& metaInfo)
{
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
        return RestoreStateType::PQ;
    }
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
        return RestoreStateType::KV;
    }
    return RestoreStateType::UNSUPPORT;
}

StateMetaInfoSnapshot GroupWindowAggSavepointAdaptor::buildOmniMainMetaInfo(
    int, const StateMetaInfoSnapshot& flinkMetaInfo)
{
    return flinkMetaInfo;
}

void GroupWindowAggSavepointAdaptor::transformKVData(
    const std::vector<int8_t>&, const std::vector<int8_t>&, int, RestoreKVState*)
{
    ERROR_RELEASE("GroupWindowAggSavepointAdaptor: KV transform is not used");
    throw std::logic_error("GroupWindowAggSavepointAdaptor: KV transform is not used");
}

void GroupWindowAggSavepointAdaptor::restore(
    SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
{
    VectorBatchRestoreFlow::executeRestore(*this, restoreIterator, backend);
}

} // namespace omnistream

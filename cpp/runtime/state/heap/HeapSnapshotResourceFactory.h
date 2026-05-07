/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef OMNISTREAM_HEAPSNAPSHOTRESOURCEFACTORY_H
#define OMNISTREAM_HEAPSNAPSHOTRESOURCEFACTORY_H

#include <memory>
#include <tuple>
#include <vector>
#include <emhash7.hpp>
#include "common.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/InternalKeyContext.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/heap/HeapFullSnapshotResources.h"
#include "runtime/state/heap/HeapSingleStateIterator.h"
#include "table/data/RowData.h"
#include "table/runtime/operators/window/TimeWindow.h"

/**
 * Builds the synchronous Heap snapshot resources for checkpoint/savepoint.
 *
 * This mirrors Flink's heap backend architecture more closely than preparing
 * snapshot resources directly inside HeapKeyedStateBackend::snapshot(). The
 * factory owns the sync-phase responsibility of freezing a point-in-time view
 * for async writing.
 *
 * Current implementation materializes KV state into immutable serialized
 * iterator entries during the sync phase. PQ collection is intentionally kept
 * as a dedicated extension point so future integration can participate in the
 * same syncPrepareResources stage without reshaping the call chain again.
 */
template <typename K>
class HeapSnapshotResourceFactory {
public:
    static std::string describeStateBackendId(StateDescriptor *desc)
    {
        if (desc == nullptr) {
            return "null";
        }
        if (desc->getType() == StateDescriptor::Type::MAP) {
            return std::string("mapKey=")
                + std::to_string(static_cast<int>(desc->getKeyDataId()))
                + ",mapValue="
                + std::to_string(static_cast<int>(desc->getValueDataId()));
        }
        return std::to_string(static_cast<int>(desc->getBackendId()));
    }

    using RegisteredKvStates =
        emhash7::HashMap<std::string, std::tuple<uintptr_t, StateDescriptor *, BackendDataType>>;

    HeapSnapshotResourceFactory(
        TypeSerializer *keySerializer,
        InternalKeyContext<K> *context,
        RegisteredKvStates *registeredKvStates)
        : keySerializer_(keySerializer),
          context_(context),
          registeredKvStates_(registeredKvStates)
    {
    }

    std::shared_ptr<HeapFullSnapshotResources> createSnapshotResources(long checkpointId)
    {
        PreparedHeapSnapshotData preparedData = prepareSnapshotData(checkpointId);
        return std::make_shared<HeapFullSnapshotResources>(
            std::move(preparedData.metaInfoSnapshots),
            std::move(preparedData.stateIterators),
            context_->getKeyGroupRange(),
            keySerializer_,
            preparedData.keyGroupPrefixBytes);
    }

private:
    struct PreparedHeapSnapshotData {
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfoSnapshots;
        std::vector<std::unique_ptr<SingleStateIterator>> stateIterators;
        int keyGroupPrefixBytes = 0;
    };

    PreparedHeapSnapshotData prepareSnapshotData(long checkpointId)
    {
        PreparedHeapSnapshotData preparedData;
        preparedData.keyGroupPrefixBytes = CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(
            context_->getNumberOfKeyGroups());

        collectKeyValueStateSnapshots(preparedData, checkpointId);
        collectPriorityQueueStateSnapshots(preparedData, checkpointId);
        return preparedData;
    }

    void collectPriorityQueueStateSnapshots(PreparedHeapSnapshotData &preparedData, long checkpointId)
    {
        (void) preparedData;
        (void) checkpointId;
        // Reserved for future Heap PQ integration so PQ can participate in the
        // same syncPrepareResources() freeze point as KV state.
    }

    void collectKeyValueStateSnapshots(PreparedHeapSnapshotData &preparedData, long checkpointId)
    {
        int kvStateId = 0;
        for (const auto &pair : *registeredKvStates_) {
            StateDescriptor *desc = std::get<1>(pair.second);
            uintptr_t stateTablePtr = std::get<0>(pair.second);
            auto nsBackendId = std::get<2>(pair.second);
            try {
                if (desc->getType() == StateDescriptor::Type::VALUE) {
                    auto dataId = desc->getBackendId();
                    if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, RowData *> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, int64_t, RowData *>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (nsBackendId == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, TimeWindow, RowData *> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, TimeWindow, RowData *>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::OBJECT_BK || dataId == BackendDataType::POJO_BK
                            || dataId == BackendDataType::TUPLE_OBJ_OBJ_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, Object *> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, Object *>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::INT_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, int>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::BIGINT_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, int64_t>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::ROW_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData *> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, RowData *>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else {
                        INFO_RELEASE("Error:HeapSnapshotResourceFactory: unsupported VALUE type " << dataId
                            << " for snapshot, skipping state=" << desc->getName());
                    }
                } else if (desc->getType() == StateDescriptor::Type::LIST) {
                    auto dataId = desc->getBackendId();
                    if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<int64_t> *> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, int64_t, std::vector<int64_t> *>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::BIGINT_BK) {
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<int64_t> *> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, std::vector<int64_t> *>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else {
                        INFO_RELEASE("Error:HeapSnapshotResourceFactory: unsupported LIST type " << dataId
                            << " for snapshot, skipping state=" << desc->getName());
                    }
                } else if (desc->getType() == StateDescriptor::Type::MAP) {
                    auto keyId = desc->getKeyDataId();
                    auto valueId = desc->getValueDataId();
                    if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
                        using S = emhash7::HashMap<int, int> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
                        using S = emhash7::HashMap<int64_t, int64_t> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
                        using S = emhash7::HashMap<std::string, int> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
                        using S = emhash7::HashMap<RowData *, int32_t> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
                        using S = emhash7::HashMap<RowData *, RowData *> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
                        using S = emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
                        using S = emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
                        using S = emhash7::HashMap<TimeWindow, TimeWindow> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
                        using S = emhash7::HashMap<RowData *, std::vector<RowData *> *> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else if ((keyId == BackendDataType::OBJECT_BK || keyId == BackendDataType::POJO_BK) &&
                            (valueId == BackendDataType::OBJECT_BK || valueId == BackendDataType::POJO_BK)) {
                        using S = emhash7::HashMap<Object *, Object *> *;
                        auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S> *>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table,
                                kvStateId,
                                preparedData.keyGroupPrefixBytes));
                    } else {
                        INFO_RELEASE("Error:HeapSnapshotResourceFactory: unsupported MAP type key=" << keyId
                            << " value=" << valueId << " for snapshot, skipping state=" << desc->getName());
                    }
                } else {
                    INFO_RELEASE("Error:HeapSnapshotResourceFactory: unsupported state type for snapshot, skipping state="
                        << desc->getName());
                }
            } catch (const std::exception &e) {
                INFO_RELEASE("Error:HeapSnapshotResourceFactory: checkpointId=" << checkpointId
                    << ", failed while preparing stateName=" << desc->getName()
                    << ", kvStateId=" << kvStateId
                    << ", exception=" << e.what());
                throw;
            } catch (...) {
                INFO_RELEASE("Error:HeapSnapshotResourceFactory: checkpointId=" << checkpointId
                    << ", failed while preparing stateName=" << desc->getName()
                    << ", kvStateId=" << kvStateId
                    << ", exception=unknown");
                throw;
            }
            kvStateId++;
        }
    }

    TypeSerializer *keySerializer_;
    InternalKeyContext<K> *context_;
    RegisteredKvStates *registeredKvStates_;
};

#endif // OMNISTREAM_HEAPSNAPSHOTRESOURCEFACTORY_H

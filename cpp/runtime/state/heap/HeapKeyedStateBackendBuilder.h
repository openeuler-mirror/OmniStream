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
#ifndef FLINK_TNEL_HEAPKEYEDSTATEBACKENDBUILDER_H
#define FLINK_TNEL_HEAPKEYEDSTATEBACKENDBUILDER_H
#include <emhash7.hpp>
#include <set>
#include <memory>
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/restore/FullSnapshotRestoreOperation.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/api/common/state/RestoreStateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "CopyOnWriteStateTable.h"
#include "table/data/RowData.h"
#include "runtime/state/InternalKeyContextImpl.h"

template <typename K>
class HeapKeyedStateBackendBuilder {
public:
    HeapKeyedStateBackendBuilder(TypeSerializer *keySerializer, int numberOfKeyGroups, KeyGroupRange *keyGroupRange)
        : keySerializer(keySerializer), numberOfKeyGroups(numberOfKeyGroups), keyGroupRange(keyGroupRange) {};

    HeapKeyedStateBackendBuilder &setOmniTaskBridge(std::shared_ptr<omnistream::OmniTaskBridge> bridge)
    {
        omniTaskBridge = bridge;
        return *this;
    }

    HeapKeyedStateBackendBuilder &setStateHandles(const std::set<std::shared_ptr<KeyedStateHandle>> &handles)
    {
        restoreStateHandles = handles;
        return *this;
    }

    HeapKeyedStateBackend<K> *build();

protected:
    TypeSerializer *keySerializer;
    int numberOfKeyGroups;
    KeyGroupRange *keyGroupRange;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge;
    std::set<std::shared_ptr<KeyedStateHandle>> restoreStateHandles;

private:
    /** Info collected per state during restore Phase 1, used in Phase 2 for deserialization. */
    struct RestoreStateInfo {
        StateDescriptor *stateDesc;
        TypeSerializer *namespaceSerializer;
        TypeSerializer *valueSerializer;
    };

    /**
     * Creates a RestoreStateDescriptor from a StateMetaInfoSnapshot.
     * For MAP states, extracts key/value BackendDataType from MapSerializer.
     * For VALUE/LIST states, uses the value serializer's BackendDataType.
     */
    static StateDescriptor *createRestoreDescriptor(
        const StateMetaInfoSnapshot &metaInfo,
        StateDescriptor::Type stateType,
        TypeSerializer *nsSerializer,
        TypeSerializer *valSerializer)
    {
        if (stateType == StateDescriptor::Type::MAP) {
            auto *mapSer = dynamic_cast<MapSerializer *>(valSerializer);
            if (mapSer) {
                return new RestoreStateDescriptor(
                    metaInfo.getName(), stateType, valSerializer,
                    BackendDataType::INVALID_BK,
                    mapSer->getKeySerializer()->getBackendId(),
                    mapSer->getValueSerializer()->getBackendId());
            }
            // Fallback: OBJECT×OBJECT if MapSerializer cast fails
            return new RestoreStateDescriptor(
                metaInfo.getName(), stateType, valSerializer,
                BackendDataType::INVALID_BK,
                BackendDataType::OBJECT_BK,
                BackendDataType::OBJECT_BK);
        } else if (stateType == StateDescriptor::Type::LIST) {
            // ListSerializer::getBackendId() returns OBJECT_BK, not the element type.
            // Extract the element BackendDataType from ListSerializer.
            auto *listSer = dynamic_cast<ListSerializer *>(valSerializer);
            BackendDataType elemId = listSer ? listSer->getElementSerializer()->getBackendId()
                                             : valSerializer->getBackendId();
            return new RestoreStateDescriptor(
                metaInfo.getName(), stateType, valSerializer, elemId);
        } else {
            // VALUE
            return new RestoreStateDescriptor(
                metaInfo.getName(), stateType, valSerializer,
                valSerializer->getBackendId());
        }
    }

    /**
     * Deserializes a single KV entry from checkpoint bytes and inserts it into the correct
     * typed CopyOnWriteStateTable. Mirrors the type dispatch in createFullSnapshotResources().
     */
    void restoreEntryToHeap(
        HeapKeyedStateBackend<K> *backend,
        const RestoreStateInfo &info,
        int keyGroupId,
        int keyGroupPrefixBytes,
        const std::vector<int8_t> &keyBytes,
        const std::vector<int8_t> &valueBytes);

    /**
     * Deserializes an emhash7::HashMap from checkpoint bytes using the MapSerializer's
     * sub-serializers. Format: [int size] [key + bool isNull + value per entry].
     * Mirrors HeapSingleStateIterator::serializeEmhashMap().
     *
     * For Object* types, uses GetBuffer()+deserialize(Object*,...) since PojoSerializer's
     * void* deserialize is NOT_IMPL.
     */
    template<typename UK, typename UV>
    static emhash7::HashMap<UK, UV> *deserializeEmhashMap(
        TypeSerializer *keySer,
        TypeSerializer *valSer,
        DataInputDeserializer &input)
    {
        int size = input.readInt();
        auto *map = new emhash7::HashMap<UK, UV>();
        map->reserve(size);
        for (int i = 0; i < size; i++) {
            UK key;
            if constexpr (std::is_same_v<UK, Object *>) {
                // Object* path: PojoSerializer::deserialize(void*) is NOT_IMPL
                Object *buf = keySer->GetBuffer();
                keySer->deserialize(buf, input);
                key = buf;
            } else if constexpr (std::is_pointer_v<UK>) {
                key = static_cast<UK>(keySer->deserialize(input));
            } else {
                void *rawK = keySer->deserialize(input);
                key = *static_cast<UK *>(rawK);
                delete static_cast<UK *>(rawK);
            }
            bool isNull = input.readBoolean();
            UV val;
            if constexpr (std::is_pointer_v<UV>) {
                if (isNull) {
                    val = nullptr;
                } else {
                    if constexpr (std::is_same_v<UV, Object *>) {
                        Object *buf = valSer->GetBuffer();
                        valSer->deserialize(buf, input);
                        val = buf;
                    } else {
                        val = static_cast<UV>(valSer->deserialize(input));
                    }
                }
            } else {
                // For value types, null marker should be false
                void *rawV = valSer->deserialize(input);
                val = *static_cast<UV *>(rawV);
                delete static_cast<UV *>(rawV);
            }
            (*map)[key] = val;
        }
        return map;
    }

    /**
     * Deserializes a std::vector from checkpoint bytes using the ListSerializer's
     * element serializer. Format: [int size] [elem_1] [elem_2] ...
     * Mirrors HeapSingleStateIterator::serializeVector().
     */
    template<typename V>
    static std::vector<V> *deserializeVector(
        TypeSerializer *elemSer,
        DataInputDeserializer &input)
    {
        int size = input.readInt();
        auto *vec = new std::vector<V>();
        vec->reserve(size);
        for (int i = 0; i < size; i++) {
            if constexpr (std::is_pointer_v<V>) {
                vec->push_back(static_cast<V>(elemSer->deserialize(input)));
            } else {
                void *raw = elemSer->deserialize(input);
                vec->push_back(*static_cast<V *>(raw));
                delete static_cast<V *>(raw);
            }
        }
        return vec;
    }
};

template <typename K>
HeapKeyedStateBackend<K> *HeapKeyedStateBackendBuilder<K>::build()
{
    auto *keyContext = new InternalKeyContextImpl<K>(keyGroupRange, numberOfKeyGroups);
    auto *backend = new HeapKeyedStateBackend<K>(keySerializer, keyContext);

    if (omniTaskBridge) {
        backend->setOmniTaskBridge(omniTaskBridge);
    }

    // Restore from state handles if available
    if (!restoreStateHandles.empty() && omniTaskBridge) {
        int keyGroupPrefixBytes =
            CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);

        std::vector<std::shared_ptr<KeyedStateHandle>> handleVec(
            restoreStateHandles.begin(), restoreStateHandles.end());

        auto keySerializerPtr = std::shared_ptr<TypeSerializer>(keySerializer, [](TypeSerializer *) {});
        FullSnapshotRestoreOperation<K> restoreOp(
            keyGroupRange, handleVec, keySerializerPtr, omniTaskBridge);
        auto restoreIterator = restoreOp.restore();

        while (restoreIterator->hasNext()) {
            auto restoreResult = restoreIterator->next();
            auto &metaInfos = restoreResult->getStateMetaInfoSnapshots();
            auto keyGroupIterator = restoreResult->getKeyGroupIterator();

            // Phase 1: For each StateMetaInfoSnapshot, create state table via createOrUpdateInternalState()
            std::vector<RestoreStateInfo> stateInfos;
            stateInfos.reserve(metaInfos.size());

            for (size_t i = 0; i < metaInfos.size(); i++) {
                auto &metaInfo = metaInfos[i];

                std::string stateTypeStr = metaInfo.getOption(
                    StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
                StateDescriptor::Type stateType = StateDescriptor::StringToType(stateTypeStr);

                TypeSerializer *nsSerializer = metaInfo.getTypeSerializer("NAMESPACE_SERIALIZER");
                TypeSerializer *valSerializer = metaInfo.getTypeSerializer("VALUE_SERIALIZER");

                if (nsSerializer == nullptr || valSerializer == nullptr) {
                    INFO_RELEASE("HeapKeyedStateBackendBuilder: skipping state '"
                        << metaInfo.getName() << "' — missing serializer(s)");
                    stateInfos.push_back({nullptr, nullptr, nullptr});
                    continue;
                }

                StateDescriptor *desc = createRestoreDescriptor(metaInfo, stateType, nsSerializer, valSerializer);

                // Create the state table via the existing type dispatch mechanism
                backend->createOrUpdateInternalState(nsSerializer, desc);

                stateInfos.push_back({desc, nsSerializer, valSerializer});
            }

            // Phase 2: Iterate KV entries, deserialize, and write into state tables
            int totalEntriesRestored = 0;
            int totalKeyGroups = 0;
            int totalSkippedInvalidKvStateId = 0;
            int totalSkippedNullStateDesc = 0;
            while (keyGroupIterator->hasNext()) {
                auto keyGroup = keyGroupIterator->next();
                int keyGroupId = keyGroup->getKeyGroupId();
                auto entryIter = keyGroup->getKeyGroupEntries();
                int kgEntryCount = 0;
                totalKeyGroups++;

                while (entryIter->hasNext()) {
                    auto entry = entryIter->next();
                    int kvStateId = entry->getKvStateId();

                    if (kvStateId < 0 || kvStateId >= static_cast<int>(stateInfos.size())) {
                        INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: invalid kvStateId "
                            << kvStateId << ", skipping entry");
                        totalSkippedInvalidKvStateId++;
                        continue;
                    }

                    auto &info = stateInfos[kvStateId];
                    if (info.stateDesc == nullptr) {
                        totalSkippedNullStateDesc++;
                        continue;  // State was skipped in Phase 1
                    }

                    restoreEntryToHeap(backend, info, keyGroupId, keyGroupPrefixBytes,
                                       entry->getKey(), entry->getValue());
                    kgEntryCount++;
                    totalEntriesRestored++;
                    // if (totalEntriesRestored % 10000 == 0) {
                    //     INFO_RELEASE("[OS-CP-restore] restored " << totalEntriesRestored << " entries so far");
                    // }
                }
            }
        }
    }

    return backend;
}

template <typename K>
void HeapKeyedStateBackendBuilder<K>::restoreEntryToHeap(
    HeapKeyedStateBackend<K> *backend,
    const RestoreStateInfo &info,
    int keyGroupId,
    int keyGroupPrefixBytes,
    const std::vector<int8_t> &keyBytes,
    const std::vector<int8_t> &valueBytes)
{
    // Deserialize key + namespace from keyBytes (skip keyGroupPrefix)
    DataInputDeserializer keyInput(
        reinterpret_cast<const uint8_t *>(keyBytes.data()),
        static_cast<int>(keyBytes.size()),
        keyGroupPrefixBytes);

    // Deserialize key.
    // 关键分支：当 backend 的 K 是 Object*（DataStream 标准场景，例如 reduce 的 String key），
    // 各类 serializer 的 void* deserialize 返回的并不是 Object**，而是具体子类指针（StringSerializer
    // 返回 std::u32string*）。直接 *static_cast<K*>(rawKey) 会按 Object** 解 std::u32string 头 8 字节
    // → 野指针 → CopyOnWriteStateMap::put 里 SIGSEGV。
    // 这里改成走 Object* 路径：GetBuffer + deserialize(Object*, ...)，再用一个 heap-allocated
    // Object** 包一层，让下游 *static_cast<K*>(rawKey) 与 delete 的写法保持统一。
    void *rawKey = nullptr;
    Object *keyObjForObjectKBackend = nullptr;
    if constexpr (std::is_same_v<K, Object *>) {
        keyObjForObjectKBackend = keySerializer->GetBuffer();
        keySerializer->deserialize(keyObjForObjectKBackend, keyInput);
        rawKey = new Object *(keyObjForObjectKBackend);  // K = Object*; new Object*(value)
    } else {
        rawKey = keySerializer->deserialize(keyInput);
    }
    // Object* key 的引用 RAII 守卫：state map 的 put() 内部对 Object* key 会 clone() 一份并由
    // map 自己持有引用；我们 GetBuffer 拿来的这份用完应当 putRefCount 释放，否则每条 entry 都会
    // 泄漏一次引用（reuseBuffer 路径）或泄漏一个 Object（newInstance 路径）。
    struct ObjKeyRefGuard {
        Object *obj;
        ~ObjKeyRefGuard() { if (obj != nullptr) { obj->putRefCount(); } }
    } objKeyGuard{keyObjForObjectKBackend};
    // Deserialize namespace
    void *rawNs = info.namespaceSerializer->deserialize(keyInput);

    // Deserialize value from valueBytes
    DataInputDeserializer valInput(
        reinterpret_cast<const uint8_t *>(valueBytes.data()),
        static_cast<int>(valueBytes.size()),
        0);

    auto *desc = info.stateDesc;
    uintptr_t stateTablePtr = backend->getStateTablePtr(desc->getName());

    if (stateTablePtr == 0) {
        INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: state table not found for '"
            << desc->getName() << "', skipping entry");
        delete static_cast<K *>(rawKey);
        delete static_cast<VoidNamespace *>(rawNs);
        return;
    }

    auto nsBackendId = info.namespaceSerializer->getBackendId();

    if (desc->getType() == StateDescriptor::Type::VALUE) {
        auto dataId = desc->getBackendId();

        if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
            void *rawVal = info.valueSerializer->deserialize(valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, RowData *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<int64_t *>(rawNs),
                       static_cast<RowData *>(rawVal));
            delete static_cast<K *>(rawKey);
            delete static_cast<int64_t *>(rawNs);
        } else if (nsBackendId == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
            void *rawVal = info.valueSerializer->deserialize(valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, TimeWindow, RowData *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<TimeWindow *>(rawNs),
                       static_cast<RowData *>(rawVal));
            delete static_cast<K *>(rawKey);
            delete static_cast<TimeWindow *>(rawNs);
        } else if (dataId == BackendDataType::OBJECT_BK || dataId == BackendDataType::POJO_BK
                   || dataId == BackendDataType::TUPLE_OBJ_OBJ_BK) {
            // PojoSerializer/Tuple2Serializer::deserialize(void*) is NOT_IMPL, use Object* path
            Object *valObj = info.valueSerializer->GetBuffer();
            info.valueSerializer->deserialize(valObj, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, Object *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), valObj);
            delete static_cast<K *>(rawKey);
            delete static_cast<VoidNamespace *>(rawNs);
        } else if (dataId == BackendDataType::INT_BK) {
            void *rawVal = info.valueSerializer->deserialize(valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs),
                       *static_cast<int *>(rawVal));
            delete static_cast<K *>(rawKey);
            delete static_cast<VoidNamespace *>(rawNs);
            delete static_cast<int *>(rawVal);
        } else if (dataId == BackendDataType::BIGINT_BK) {
            void *rawVal = info.valueSerializer->deserialize(valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs),
                       *static_cast<int64_t *>(rawVal));
            delete static_cast<K *>(rawKey);
            delete static_cast<VoidNamespace *>(rawNs);
            delete static_cast<int64_t *>(rawVal);
        } else if (dataId == BackendDataType::ROW_BK) {
            void *rawVal = info.valueSerializer->deserialize(valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs),
                       static_cast<RowData *>(rawVal));
            delete static_cast<K *>(rawKey);
            delete static_cast<VoidNamespace *>(rawNs);
        } else {
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: unsupported VALUE restore type " << dataId);
            delete static_cast<K *>(rawKey);
            if (nsBackendId == BackendDataType::VOID_NAMESPACE_BK) {
                delete static_cast<VoidNamespace *>(rawNs);
            }
        }

    } else if (desc->getType() == StateDescriptor::Type::LIST) {
        auto dataId = desc->getBackendId();

        // ListSerializer::deserialize(void*) is NOT_IMPL, use element serializer directly
        auto *listSer = dynamic_cast<ListSerializer *>(info.valueSerializer);
        TypeSerializer *elemSer = listSer ? listSer->getElementSerializer() : nullptr;

        if (elemSer == nullptr) {
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: LIST state serializer is not ListSerializer, skipping");
            delete static_cast<K *>(rawKey);
            if (nsBackendId == BackendDataType::VOID_NAMESPACE_BK) {
                delete static_cast<VoidNamespace *>(rawNs);
            }
            return;
        }

        if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
            auto *vecVal = deserializeVector<int64_t>(elemSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<int64_t> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<int64_t *>(rawNs), vecVal);
            delete static_cast<K *>(rawKey);
            delete static_cast<int64_t *>(rawNs);
        } else if (dataId == BackendDataType::BIGINT_BK) {
            auto *vecVal = deserializeVector<int64_t>(elemSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<int64_t> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), vecVal);
            delete static_cast<K *>(rawKey);
            delete static_cast<VoidNamespace *>(rawNs);
        } else {
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: unsupported LIST restore type " << dataId);
            delete static_cast<K *>(rawKey);
            delete static_cast<VoidNamespace *>(rawNs);
        }

    } else if (desc->getType() == StateDescriptor::Type::MAP) {
        auto mapKeyId = desc->getKeyDataId();
        auto mapValId = desc->getValueDataId();

        // For MAP state, bypass MapSerializer (whose void* deserialize is NOT_IMPL)
        // and deserialize the emhash7::HashMap directly using sub-serializers.
        auto *mapSer = dynamic_cast<MapSerializer *>(info.valueSerializer);
        if (!mapSer) {
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: MAP state serializer is not MapSerializer, skipping");
            delete static_cast<K *>(rawKey);
            delete static_cast<VoidNamespace *>(rawNs);
            return;
        }
        auto *mapKeySer = mapSer->getKeySerializer();
        auto *mapValSer = mapSer->getValueSerializer();

        if (mapKeyId == BackendDataType::INT_BK && mapValId == BackendDataType::INT_BK) {
            auto *mapVal = deserializeEmhashMap<int, int>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int, int> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::BIGINT_BK && mapValId == BackendDataType::BIGINT_BK) {
            auto *mapVal = deserializeEmhashMap<int64_t, int64_t>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int64_t, int64_t> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::VARCHAR_BK && mapValId == BackendDataType::INT_BK) {
            auto *mapVal = deserializeEmhashMap<std::string, int>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<std::string, int> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if ((mapKeyId == BackendDataType::OBJECT_BK || mapKeyId == BackendDataType::POJO_BK) &&
                   (mapValId == BackendDataType::OBJECT_BK || mapValId == BackendDataType::POJO_BK)) {
            auto *mapVal = deserializeEmhashMap<Object *, Object *>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<Object *, Object *> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::INT_BK) {
            auto *mapVal = deserializeEmhashMap<RowData *, int32_t>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<RowData *, int32_t> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_BK) {
            auto *mapVal = deserializeEmhashMap<RowData *, RowData *>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<RowData *, RowData *> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT64) {
            auto *mapVal = deserializeEmhashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT32_INT64) {
            auto *mapVal = deserializeEmhashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::TIME_WINDOW_BK && mapValId == BackendDataType::TIME_WINDOW_BK) {
            auto *mapVal = deserializeEmhashMap<TimeWindow, TimeWindow>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<TimeWindow, TimeWindow> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_LIST_BK) {
            auto *mapVal = deserializeEmhashMap<RowData *, std::vector<RowData *> *>(mapKeySer, mapValSer, valInput);
            auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<RowData *, std::vector<RowData *> *> *> *>(stateTablePtr);
            table->put(*static_cast<K *>(rawKey), keyGroupId, *static_cast<VoidNamespace *>(rawNs), mapVal);
        } else {
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: unsupported MAP restore type key="
                << mapKeyId << " value=" << mapValId);
        }

        delete static_cast<K *>(rawKey);
        delete static_cast<VoidNamespace *>(rawNs);

    } else {
        INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: unsupported state type for restore, skipping");
        delete static_cast<K *>(rawKey);
        delete static_cast<VoidNamespace *>(rawNs);
    }
}

#endif // FLINK_TNEL_HEAPKEYEDSTATEBACKENDBUILDER_H

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
#ifndef OMNISTREAM_HEAPSINGLESTATEITERATOR_H
#define OMNISTREAM_HEAPSINGLESTATEITERATOR_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>
#include "runtime/state/rocksdb/iterator/SingleStateIterator.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/heap/HeapSnapshotStateData.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "basictypes/Object.h"
#include "StateTable.h"
#include "CopyOnWriteStateMap.h"
#include "table/utils/VectorBatchSerializationUtils.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "../../../core/include/common.h"

// Type traits to detect emhash7::HashMap and std::vector pointer types
template <typename T>
struct IsEmhashMapPtr : std::false_type {};
template <typename UK, typename UV>
struct IsEmhashMapPtr<emhash7::HashMap<UK, UV>*> : std::true_type {};

template <typename T>
struct IsVectorPtr : std::false_type {};
template <typename V>
struct IsVectorPtr<std::vector<V>*> : std::true_type {};

/**
 * A SingleStateIterator that iterates over a Heap CopyOnWriteStateTable,
 * serializing each entry into byte arrays compatible with the RocksDB key format:
 *   key = [keyGroupPrefix] + [serialized key] + [serialized namespace]
 *   value = [serialized state value]
 *
 * Entries are materialized eagerly during construction and iterated in
 * key-group order (ascending) so that RocksStatesPerKeyGroupMergeIterator can
 * merge them correctly without touching live state in the async phase.
 */
template <typename K, typename N, typename S>
class HeapSingleStateIterator : public SingleStateIterator {
public:
    using SerializedEntry = HeapSnapshotStateData::SerializedEntry;

    struct VbDataTag {};

    HeapSingleStateIterator(StateTable<K, N, S>* stateTable, int kvStateId, int keyGroupPrefixBytes)
        : stateTable_(stateTable),
          kvStateId_(kvStateId),
          keyGroupPrefixBytes_(keyGroupPrefixBytes)
    {
        collectAndSerializeEntries();
        currentIndex_ = 0;
        valid_ = !entries().empty();
        refreshKeyGroup();
    }

    HeapSingleStateIterator(
        StateTable<int, VoidNamespace, omnistream::VectorBatch*>* vbTable,
        int kvStateId,
        int keyGroupPrefixBytes,
        VbDataTag,
        bool captureVectorBatchAccessorData = false)
        : stateTable_(reinterpret_cast<StateTable<K, N, S>*>(vbTable)),
          kvStateId_(kvStateId),
          keyGroupPrefixBytes_(keyGroupPrefixBytes),
          snapshotData_(captureVectorBatchAccessorData ? std::make_shared<HeapSnapshotStateData>() : nullptr)
    {
        collectVbEntries();
        currentIndex_ = 0;
        valid_ = !entries().empty();
        refreshKeyGroup();
    }

    void next() override
    {
        if (valid_) {
            currentIndex_++;
            valid_ = (currentIndex_ < entries().size());
            refreshKeyGroup();
        }
    }

    bool isValid() const override
    {
        return valid_;
    }

    ByteView key() const override
    {
        const auto& key = entries()[currentIndex_].serializedKey;
        return ByteView::fromBuffer(key.data(), key.size());
    }

    ByteView value() const override
    {
        const auto& value = entries()[currentIndex_].serializedValue;
        return ByteView::fromBuffer(value.data(), value.size());
    }

    int keyGroup() const override
    {
        return currentKeyGroup_;
    }

    int getKvStateId() const override
    {
        return kvStateId_;
    }

    size_t getEntryCount() const override
    {
        return entries().size();
    }

    void close() override
    {
        // Iterator close 只标记迭代失效。若启用了 VB accessor capture，frozen bytes 由
        // HeapSnapshotStateData 持有并可继续被资源层通过 shared_ptr 读取。
        if (snapshotData_ == nullptr) {
            entries_.clear();
            currentIndex_ = 0;
        } else {
            currentIndex_ = snapshotData_->entries().size();
        }
        currentKeyGroup_ = -1;
        valid_ = false;
    }

    std::shared_ptr<HeapSnapshotStateData> getSnapshotData() const
    {
        return snapshotData_;
    }

private:
    StateTable<K, N, S>* stateTable_;
    int kvStateId_;
    int keyGroupPrefixBytes_;
    std::vector<SerializedEntry> entries_;
    std::shared_ptr<HeapSnapshotStateData> snapshotData_;
    size_t currentIndex_ = 0;
    int currentKeyGroup_ = -1;
    bool valid_ = false;

    const std::vector<SerializedEntry>& entries() const
    {
        return snapshotData_ == nullptr ? entries_ : snapshotData_->entries();
    }

    std::vector<SerializedEntry>& entries()
    {
        return snapshotData_ == nullptr ? entries_ : snapshotData_->entries();
    }

    void addSnapshotEntry(SerializedEntry entry)
    {
        if (snapshotData_ == nullptr) {
            entry.vectorBatchId = -1;
            entries_.push_back(std::move(entry));
            return;
        }
        snapshotData_->addEntry(std::move(entry));
    }

    // See RocksSingleStateIterator::refreshKeyGroup() for rationale.
    void refreshKeyGroup()
    {
        currentKeyGroup_ = -1;
        if (!valid_ || currentIndex_ >= entries().size()) {
            return;
        }
        const auto& key = entries()[currentIndex_].serializedKey;
        if (key.size() < static_cast<size_t>(keyGroupPrefixBytes_)) {
            return;
        }
        int result = 0;
        for (int i = 0; i < keyGroupPrefixBytes_; ++i) {
            result <<= 8;
            result |= static_cast<int>(static_cast<uint8_t>(key[i]));
        }
        currentKeyGroup_ = result;
    }

    void collectAndSerializeEntries()
    {
        auto* stateMaps = stateTable_->getState();
        int keyGroupOffset = stateTable_->getKeyGroupOffset();
        TypeSerializer* keySerializer = stateTable_->getKeySerializer();
        TypeSerializer* namespaceSerializer = stateTable_->getNamespaceSerializer();
        TypeSerializer* stateSerializer = stateTable_->getStateSerializer();

        for (size_t i = 0; i < stateMaps->size(); i++) {
            int keyGroup = keyGroupOffset + static_cast<int>(i);
            auto* stateMap = (*stateMaps)[i];
            if (stateMap == nullptr || stateMap->size() == 0) {
                continue;
            }

            serializeStateMap(stateMap, keyGroup, keySerializer, namespaceSerializer, stateSerializer);
        }

        // Sort by keyGroupPrefix bytes (ascending) to match MergeIterator expectation
        sortSnapshotEntries();
    }

    void collectVbEntries()
    {
        auto* stateMaps = stateTable_->getState();
        int keyGroupOffset = stateTable_->getKeyGroupOffset();

        for (size_t i = 0; i < stateMaps->size(); i++) {
            int keyGroup = keyGroupOffset + static_cast<int>(i);
            auto* stateMap = (*stateMaps)[i];
            if (stateMap == nullptr || stateMap->size() == 0) {
                continue;
            }

            auto* cowMap = dynamic_cast<omnistream::CopyOnWriteStateMap<K, N, S>*>(stateMap);
            if (cowMap == nullptr) {
                continue;
            }

            for (auto it = cowMap->begin(); it != cowMap->end(); ++it) {
                SerializedEntry entry;
                try {
                    entry.serializedKey = serializeVbKey(keyGroup, it->first, it->third);
                    entry.serializedValue = serializeVbValue(it->second);
                } catch (const std::exception& e) {
                    INFO_RELEASE(
                        "Error:HeapSingleStateIterator: collectVbEntries EXCEPTION at keyGroup="
                        << keyGroup << ", error=" << e.what());
                    throw;
                }
                if (snapshotData_ == nullptr) {
                    addSnapshotEntry(std::move(entry));
                } else {
                    snapshotData_->addVectorBatchEntry(std::move(entry), it->first);
                }
            }
        }

        sortSnapshotEntries();
    }

    void sortSnapshotEntries()
    {
        auto& entries = this->entries();
        std::stable_sort(
            entries.begin(), entries.end(), [this](const SerializedEntry& a, const SerializedEntry& b) -> bool {
                for (int i = 0; i < keyGroupPrefixBytes_ && i < static_cast<int>(a.serializedKey.size()) &&
                                i < static_cast<int>(b.serializedKey.size());
                     i++) {
                    if (static_cast<uint8_t>(a.serializedKey[i]) != static_cast<uint8_t>(b.serializedKey[i])) {
                        return static_cast<uint8_t>(a.serializedKey[i]) < static_cast<uint8_t>(b.serializedKey[i]);
                    }
                }
                return false;
            });
        if (snapshotData_ != nullptr) {
            snapshotData_->rebuildVectorBatchEntryIndices();
        }
    }

    std::vector<int8_t> serializeVbKey(int keyGroup, const int64_t& batchId, const VoidNamespace&)
    {
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer outputSerializer;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        outputSerializer.writeByte(static_cast<uint32_t>(keyGroup));

        LongSerializer longSerializer;
        longSerializer.serialize(const_cast<int64_t*>(&batchId), outputSerializer);

        std::vector<int8_t> result(outputSerializer.getPosition());
        memcpy(result.data(), outputSerializer.getData(), outputSerializer.getPosition());
        return result;
    }

    static std::vector<int8_t> serializeVbValue(omnistream::VectorBatch* vectorBatch)
    {
        if (vectorBatch == nullptr) {
            return {};
        }

        int32_t batchSize =
            omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        if (batchSize <= 0) {
            return {};
        }

        uint8_t* buffer = new uint8_t[batchSize];
        uint8_t* cursor = buffer;
        omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, cursor);

        std::vector<int8_t> result(batchSize);
        for (int32_t i = 0; i < batchSize; i++) {
            result[i] = static_cast<int8_t>(buffer[i]);
        }
        delete[] buffer;
        return result;
    }

    // Snapshot entry holding raw (unserialized) copies of key, namespace, and value.
    // Taking a snapshot of all entries BEFORE serializing avoids iterator invalidation
    // if the underlying CopyOnWriteStateMap is rehashed (e.g. by a concurrent put()).
    struct RawSnapshotEntry {
        RawSnapshotEntry(const K& snapshotKey, const N& snapshotNamespace, const S& snapshotValue)
            : key(snapshotKey),
              nmspace(snapshotNamespace),
              value(snapshotValue),
              ownsRefs_(true)
        {
            retainObjectRef(key);
            retainObjectRef(nmspace);
            retainObjectRef(value);
        }

        RawSnapshotEntry(const RawSnapshotEntry&) = delete;
        RawSnapshotEntry& operator=(const RawSnapshotEntry&) = delete;

        RawSnapshotEntry(RawSnapshotEntry&& other) noexcept
            : key(other.key),
              nmspace(other.nmspace),
              value(other.value),
              ownsRefs_(other.ownsRefs_)
        {
            other.ownsRefs_ = false;
        }

        RawSnapshotEntry& operator=(RawSnapshotEntry&& other) noexcept
        {
            if (this != &other) {
                releaseRefs();
                key = other.key;
                nmspace = other.nmspace;
                value = other.value;
                ownsRefs_ = other.ownsRefs_;
                other.ownsRefs_ = false;
            }
            return *this;
        }

        ~RawSnapshotEntry()
        {
            releaseRefs();
        }

        K key;
        N nmspace;
        S value;

    private:
        template <typename T>
        static void retainObjectRef(const T& ptr)
        {
            if constexpr (std::is_same_v<std::decay_t<T>, Object*>) {
                if (ptr != nullptr) {
                    ptr->getRefCount();
                }
            }
        }

        template <typename T>
        static void releaseObjectRef(const T& ptr)
        {
            if constexpr (std::is_same_v<std::decay_t<T>, Object*>) {
                if (ptr != nullptr) {
                    ptr->putRefCount();
                }
            }
        }

        void releaseRefs()
        {
            if (!ownsRefs_) {
                return;
            }
            releaseObjectRef(key);
            releaseObjectRef(nmspace);
            releaseObjectRef(value);
            ownsRefs_ = false;
        }

        bool ownsRefs_;
    };

    void serializeStateMap(
        StateMap<K, N, S>* stateMap,
        int keyGroup,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer,
        TypeSerializer* stateSerializer)
    {
        // Use StateMap's CopyOnWriteStateMap iterator to traverse entries
        auto* cowMap = dynamic_cast<omnistream::CopyOnWriteStateMap<K, N, S>*>(stateMap);
        if (cowMap == nullptr) {
            return;
        }

        // Phase 1: snapshot all entries into a local vector to decouple from
        // the live hash-table layout. This prevents use-after-free if rehash
        // reallocates _pairs/_bitmask while we are still serializing.
        std::vector<RawSnapshotEntry> snapshot;
        snapshot.reserve(cowMap->size());
        for (auto it = cowMap->begin(); it != cowMap->end(); ++it) {
            snapshot.emplace_back(it->first, it->third, it->second);
        }

        // Phase 2: serialize from the stable local snapshot
        int mapEntryCount = 0;
        for (auto& raw : snapshot) {
            SerializedEntry entry;
            try {
                entry.serializedKey = serializeKey(keyGroup, raw.key, raw.nmspace, keySerializer, namespaceSerializer);
                entry.serializedValue = serializeValue(raw.value, stateSerializer);
            } catch (const std::exception& e) {
                INFO_RELEASE(
                    "Error:HeapSingleStateIterator: serializeStateMap EXCEPTION at keyGroup="
                    << keyGroup << ", entryIndex=" << mapEntryCount << ", error=" << e.what());
                throw;
            }
            addSnapshotEntry(std::move(entry));
            mapEntryCount++;
        }
    }

    std::vector<int8_t> serializeKey(
        int keyGroup,
        const K& key,
        const N& nmspace,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer)
    {
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer outputSerializer;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        // Write key group prefix (same format as RocksdbStateTable::GetKeyNameSpaceSlice)
        if (keyGroupPrefixBytes_ == 1) {
            outputSerializer.writeByte(static_cast<uint32_t>(keyGroup));
        } else {
            outputSerializer.writeByte(static_cast<uint32_t>((keyGroup >> 8) & 0xFF));
            outputSerializer.writeByte(static_cast<uint32_t>(keyGroup & 0xFF));
        }

        // Serialize key
        if constexpr (std::is_pointer_v<K>) {
            keySerializer->serialize(const_cast<K>(key), outputSerializer);
        } else if constexpr (is_shared_ptr_v<K>) {
            if (!key) {
                THROW_LOGIC_EXCEPTION("Heap snapshot cannot serialize a null shared_ptr key");
            }
            keySerializer->serialize(key.get(), outputSerializer);
        } else {
            K mutableKey = key;
            keySerializer->serialize(&mutableKey, outputSerializer);
        }

        // Serialize namespace
        if constexpr (std::is_pointer_v<N>) {
            namespaceSerializer->serialize(const_cast<N>(nmspace), outputSerializer);
        } else if constexpr (is_shared_ptr_v<N>) {
            if (!nmspace) {
                THROW_LOGIC_EXCEPTION("Heap snapshot cannot serialize a null shared_ptr key");
            }
            namespaceSerializer->serialize(nmspace.get(), outputSerializer);
        } else {
            N mutableNs = nmspace;
            namespaceSerializer->serialize(&mutableNs, outputSerializer);
        }

        auto* data = outputSerializer.getData();
        size_t len = outputSerializer.length();
        std::vector<int8_t> result(len);
        for (size_t i = 0; i < len; i++) {
            result[i] = static_cast<int8_t>(data[i]);
        }
        return result;
    }

    /**
     * Serializes a single emhash7::HashMap entry-by-entry using the MapSerializer's
     * sub-serializers. Format: [int size] [key + bool isNull + value per entry].
     *
     * For Object* types, uses serialize(Object*,...) since PojoSerializer's void* path is NOT_IMPL.
     * For other pointer types (RowData*, etc.), uses serialize(void*,...).
     * For value types (int, int64_t, etc.), uses serialize(void*,...) with address.
     */
    template <typename UK, typename UV>
    static void serializeEmhashMap(
        const emhash7::HashMap<UK, UV>& map, TypeSerializer* keySer, TypeSerializer* valSer, DataOutputSerializer& out)
    {
        out.writeInt(static_cast<int>(map.size()));
        int idx = 0;
        for (const auto& pair : map) {
            // Serialize key
            if constexpr (std::is_same_v<UK, Object*>) {
                if (pair.first == nullptr) {
                    INFO_RELEASE("Error:serializeEmhashMap: WARNING null Object* key at index=" << idx);
                }
                keySer->serialize(const_cast<Object*>(pair.first), out);
            } else if constexpr (std::is_pointer_v<UK>) {
                keySer->serialize(const_cast<UK>(pair.first), out);
            } else {
                UK mk = pair.first;
                keySer->serialize(&mk, out);
            }
            // Serialize value with null marker (for pointer types)
            if constexpr (std::is_pointer_v<UV>) {
                if (pair.second == nullptr) {
                    out.writeBoolean(true);
                } else {
                    out.writeBoolean(false);
                    if constexpr (std::is_same_v<UV, Object*>) {
                        valSer->serialize(const_cast<Object*>(pair.second), out);
                    } else {
                        valSer->serialize(const_cast<UV>(pair.second), out);
                    }
                }
            } else {
                out.writeBoolean(false);
                UV mv = pair.second;
                valSer->serialize(&mv, out);
            }
            idx++;
        }
    }

    /**
     * Serializes a std::vector entry-by-entry using the ListSerializer's element serializer.
     * Format matches ListSerializer::serialize(Object*,...): [int size] [elem_1] [elem_2] ...
     */
    template <typename V>
    static void serializeVector(const std::vector<V>& vec, TypeSerializer* elemSer, DataOutputSerializer& out)
    {
        out.writeInt(static_cast<int>(vec.size()));
        for (const auto& elem : vec) {
            if constexpr (std::is_pointer_v<V>) {
                elemSer->serialize(const_cast<V>(elem), out);
            } else {
                V me = elem;
                elemSer->serialize(&me, out);
            }
        }
    }

    std::vector<int8_t> serializeValue(const S& state, TypeSerializer* stateSerializer)
    {
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer outputSerializer;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        if constexpr (IsEmhashMapPtr<S>::value) {
            // MAP state: bypass MapSerializer (whose void* path is NOT_IMPL)
            // and serialize the emhash7::HashMap directly using sub-serializers
            auto* mapSer = dynamic_cast<MapSerializer*>(stateSerializer);
            if (mapSer && state != nullptr) {
                serializeEmhashMap(*state, mapSer->getKeySerializer(), mapSer->getValueSerializer(), outputSerializer);
            }
        } else if constexpr (IsVectorPtr<S>::value) {
            // LIST state: bypass ListSerializer (whose void* path is NOT_IMPL)
            // and serialize the std::vector directly using the element serializer
            auto* listSer = dynamic_cast<ListSerializer*>(stateSerializer);
            if (listSer && state != nullptr) {
                serializeVector(*state, listSer->getElementSerializer(), outputSerializer);
            } else {
                // fallback: stateSerializer is not ListSerializer (e.g. SortedVectorLong for topN),
                // serialize via the generic void* path
                stateSerializer->serialize(const_cast<S>(state), outputSerializer);
            }
        } else if constexpr (std::is_pointer_v<S>) {
            stateSerializer->serialize(const_cast<S>(state), outputSerializer);
        } else {
            S mutableState = state;
            stateSerializer->serialize(&mutableState, outputSerializer);
        }

        auto* data = outputSerializer.getData();
        size_t len = outputSerializer.length();
        std::vector<int8_t> result(len);
        for (size_t i = 0; i < len; i++) {
            result[i] = static_cast<int8_t>(data[i]);
        }
        return result;
    }
};

#endif // OMNISTREAM_HEAPSINGLESTATEITERATOR_H

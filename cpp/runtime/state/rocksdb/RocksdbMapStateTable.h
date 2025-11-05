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

#ifndef OMNISTREAM_ROCKSDBMAPSTATETABLE_H
#define OMNISTREAM_ROCKSDBMAPSTATETABLE_H

#include <vector>
#include <type_traits>
#include <tuple>
#include <functional>  // for std::hash
#include "core/typeutils/TypeSerializer.h"
#include "../StateTransformationFunction.h"
#include "../internal/InternalKvState.h"
#include "../InternalKeyContext.h"
#include "../KeyGroupRange.h"
#include "../RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/binary/BinaryRowData.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/LongSerializer.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "core/include/emhash7.hpp"

#include "../../../core/utils/MathUtils.h"
#include "basictypes/java_util_Iterator.h"
#include "basictypes/java_util_Map_Entry.h"
#include "utils/VectorBatchDeserializationUtils.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "state/RocksDbKvStateInfo.h"

/* S is the value used in the State,
 * like RowData* for HeapValueState,
 * emhash7<RowData*, int>* for HeapMapState,
 * vector<int64_t>* for List State
 */
template<typename K, typename N, typename UK, typename UV>
class RocksdbMapStateTable {
public:
    RocksdbMapStateTable(InternalKeyContext<K> *keyContext, std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo,
                      TypeSerializer *keySerializer, TypeSerializer *userKeySerializer);  // 加入db initData

    ~RocksdbMapStateTable();

    bool isEmpty()
    {
        // NOT_IMPL_EXCEPTION
        return size == 0;
    };

    void createTable(ROCKSDB_NAMESPACE::DB *db, const std::string& cfName,
         std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation)
    {
        LOG("create MapState column family")
        this->rocksDb = db;
        ROCKSDB_NAMESPACE::ColumnFamilyOptions familyOptions;
        DefaultConfigurableOptionsFactory::createColumnOptions(familyOptions);

        ROCKSDB_NAMESPACE::Status s;
        auto it1 = kvStateInformation->find(cfName);
        if (it1 != kvStateInformation->end() && it1->second->columnFamilyHandle_) {
            table = it1->second->columnFamilyHandle_;
        } else {
            s = db->CreateColumnFamily(familyOptions, cfName, &table);
            if (it1 != kvStateInformation->end()) {
                it1->second->setColumnFamilyHandle(table);
            }
        }
        auto it2 = kvStateInformation->find(cfName + "vb");
        if (it2 != kvStateInformation->end() && it2->second->columnFamilyHandle_) {
            VBTable = it2->second->columnFamilyHandle_;
        } else {
            s = db->CreateColumnFamily(familyOptions, cfName + "vb", &VBTable);
            if (it2 != kvStateInformation->end()) {
                it2->second->setColumnFamilyHandle(VBTable);
            }
        }
    }

    UV get(const N &nameSpace, const UK &userKey)
    {
        // 和Rocksdb交互的时候要try catch
        LOG("RocksdbMapStateTable value get")

        // outputSerializer free need after Get called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() || valueInTable.length() == 0) {
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }

        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()),
                                             static_cast<int>(valueInTable.length()), 0);
        if constexpr (std::is_same_v<UV, Object*>) {
            auto stateSerializer = getStateSerializer();
            auto buffer = stateSerializer->GetBuffer();
            stateSerializer->deserialize(buffer, serializedData);
            return buffer;
        } else {
            void *resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<UV>) {
                return (UV)resPtr;
            } else {
                return *(UV *)resPtr;
            }
        }
    };

    void put(const N &nameSpace, const UK &userKey, const UV &state)
    {
        // 存入
        LOG("RocksDB put")
        // outputSerializer free need after Put called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey);

        // valueOutputSerializer free need after Put called
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, state);
        rocksDb->Put(writeOptions, table, sliceKey, sliceValue);
    };

    void remove(const N &nameSpace, const UK &userKey)
    {
        // 删除
        // outputSerializer free need after Delete called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey);
        rocksDb->Delete(writeOptions, table, sliceKey);
    };

    bool contains(const UK &userKey)
    {
        // 和Rocksdb交互的时候要try catch
        LOG("RocksdbMapStateTable value contains")
        // outputSerializer free need after Get called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() || valueInTable.length() == 0) {
            return false;
        }
        return true;
    }

    emhash7::HashMap<UK, UV>* entries(const N &nameSpace)
    {
        auto* resultMap = new emhash7::HashMap<UK, UV>();
        auto currentKey = keyContext->getCurrentKey();

        // 序列化key
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);

        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, keyOutputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, keyOutputSerializer);
        }

        ROCKSDB_NAMESPACE::Slice sliceKey(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                          keyOutputSerializer.length());
        ROCKSDB_NAMESPACE::Iterator* iterator = rocksDb->NewIterator(ROCKSDB_NAMESPACE::ReadOptions(), table);
        if (iterator == nullptr) {
            THROW_LOGIC_EXCEPTION("iterator from db is null")
        }
        iterator->Seek(sliceKey);
        // 遍历以指定前缀开头的键值对
        for (; iterator->Valid() && iterator->key().starts_with(sliceKey); iterator->Next()) {
            ROCKSDB_NAMESPACE::Slice key = iterator->key();
            key.remove_prefix(sliceKey.size());
            ROCKSDB_NAMESPACE::Slice value = iterator->value();
            UK entryKey;
            UV entryValue;

            DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(key.data()), key.size(), 0);
            if constexpr (std::is_same_v<UK, Object*>) {
                auto buffer = userKeySerializer->GetBuffer();
                userKeySerializer->deserialize(buffer, serializedData);
                entryKey = buffer;
            } else {
                void *resPtr = getUserKeySerializer()->deserialize(serializedData);
                if constexpr (std::is_pointer_v<UK>) {
                    entryKey = (UK)resPtr;
                } else {
                    entryKey = *(UK *)resPtr;
                }
            }

            serializedData = DataInputDeserializer(reinterpret_cast<const uint8_t *>(value.data()), value.size(), 0);

            if constexpr (std::is_same_v<UV, Object*>) {
                auto stateSerializer = getStateSerializer();
                auto buffer = stateSerializer->GetBuffer();
                stateSerializer->deserialize(buffer, serializedData);
                entryValue = buffer;
            } else {
                void *resPtr = getStateSerializer()->deserialize(serializedData);
                if constexpr (std::is_pointer_v<UV>) {
                    entryValue = (UV)resPtr;
                } else {
                    entryValue = *(UV *)resPtr;
                }
            }
            resultMap->emplace(entryKey, entryValue);
        }

        delete iterator;

        if (resultMap->size() == 0) {
            delete resultMap;
            return nullptr;
        } else {
            return resultMap;
        }
    }

    java_util_Iterator *iterator()
    {
        auto it = new RocksDBMapIterator(this);
        return it;
    }

    typename InternalKvState<K, N, UV>::StateIncrementalVisitor *getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords)
    {
        return nullptr;
    };

    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> getMetaInfo()
    {
        return std::move(metaInfo);
    }

    void setMetaInfo(std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> newMetaInfo)
    {
        metaInfo = std::move(newMetaInfo);
    }

    // Not implemented
    std::vector<K> *getKeys(const N &nameSpace);

    std::vector<std::tuple<K, N>> *getKeysAndNamespace();

    TypeSerializer *getKeySerializer()
    {
        return keySerializer;
    }

    TypeSerializer *getUserKeySerializer()
    {
        return userKeySerializer;
    }

    TypeSerializer *getStateSerializer()
    {
        return metaInfo->getStateSerializer();
    }

    TypeSerializer *getNamespaceSerializer()
    {
        return metaInfo->getNamespaceSerializer();
    }

    void addVectorBatch(omnistream::VectorBatch *vectorBatch)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&vectorBatchId, keyOutputSerializer);

        ROCKSDB_NAMESPACE::Slice key(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                     (int32_t) (keyOutputSerializer.getPosition()));
        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        auto *buffer = new uint8_t[batchSize];
        omnistream::SerializedBatchInfo serializedBatchInfo =
            omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ROCKSDB_NAMESPACE::Slice vbValue(reinterpret_cast<const char *>(serializedBatchInfo.buffer),
                                         serializedBatchInfo.size);

        auto res = rocksDb->Put(writeOptions, VBTable, key, vbValue);
        vectorBatchId += 1;
    }

    omnistream::VectorBatch *getVectorBatch(long batchId)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&batchId, keyOutputSerializer);

        ROCKSDB_NAMESPACE::Slice key(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                     (int32_t) (keyOutputSerializer.getPosition()));

        std::string valueInTable;
        auto s = rocksDb->Get(readOptions, VBTable, key, &valueInTable);
        if (!s.ok()) {
            return nullptr;
        } else {
            uint8_t* address = reinterpret_cast<uint8_t *>(valueInTable.data()) + sizeof(int8_t);
            auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
            return batch;
        }
    }

    long getVectorBatchesSize()
    {
        return vectorBatchId;
    }

    class RocksDBMapEntry : public MapEntry {
    public:
        explicit RocksDBMapEntry(RocksdbMapStateTable<K, N, UK, UV> *stateTable, int32_t userKeyOffset,
                                 const ROCKSDB_NAMESPACE::Slice &sliceKey, const ROCKSDB_NAMESPACE::Slice &sliceValue)
            : stateTable(stateTable), userKeyOffset(userKeyOffset)
        {
            this->key.resize(sliceKey.size());
            std::copy(sliceKey.data(), sliceKey.data() + sliceKey.size(), this->key.begin());
            this->value.resize(sliceValue.size());
            std::copy(sliceValue.data(), sliceValue.data() + sliceValue.size(), this->value.begin());
        }

        ~RocksDBMapEntry() override
        {
            if constexpr (std::is_same_v<UK, Object*>) {
                if (userKey != nullptr) {
                    static_cast<Object*>(userKey)->putRefCount();
                }
            }
            if constexpr (std::is_same_v<UV, Object*>) {
                if (userValue != nullptr) {
                    static_cast<Object*>(userValue)->putRefCount();
                }
            }
        }

        void remove()
        {
            deleted = true;
            ROCKSDB_NAMESPACE::Slice sliceKey(key.data(), key.size());
            stateTable->rocksDb->Delete(stateTable->writeOptions, stateTable->table, sliceKey);
        }

        UK getKey() override
        {
            if (userKey == nullptr) {
                ROCKSDB_NAMESPACE::Slice sliceKey(key.data(), key.size());
                sliceKey.remove_prefix(userKeyOffset);

                DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(sliceKey.data()), sliceKey.size(), 0);
                if constexpr (std::is_same_v<UK, Object*>) {
                    auto buffer = stateTable->userKeySerializer->GetBuffer();
                    stateTable->userKeySerializer->deserialize(buffer, serializedData);
                    userKey = buffer;
                } else {
                    void *resPtr = stateTable->userKeySerializer->deserialize(serializedData);
                    if constexpr (std::is_pointer_v<UK>) {
                        userKey = (UK)resPtr;
                    } else {
                        userKey = *(UK *)resPtr;
                    }
                }
            }
            return userKey;
        }

        UV getValue() override
        {
            if (deleted) {
                return nullptr;
            } else {
                if (userValue == nullptr) {
                    DataInputDeserializer serializedData = DataInputDeserializer(reinterpret_cast<const uint8_t *>(value.data()), value.size(), 0);

                    if constexpr (std::is_same_v<UV, Object*>) {
                        auto stateSerializer = stateTable->getStateSerializer();
                        auto buffer = stateSerializer->GetBuffer();
                        stateSerializer->deserialize(buffer, serializedData);
                        userValue = buffer;
                    } else {
                        void *resPtr = stateTable->getStateSerializer()->deserialize(serializedData);
                        if constexpr (std::is_pointer_v<UV>) {
                            userValue = (UV)resPtr;
                        } else {
                            userValue = *(UV *)resPtr;
                        }
                    }
                }
                return userValue;
            }
        }

        void setValue(Object* val) override
        {
            if (deleted) {
                THROW_LOGIC_EXCEPTION("setValue IllegalStateException: The value has already been deleted.")
            }

            userValue = val;
            val->getRefCount();
            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceValue = stateTable->serializerValue(valueOutputSerializer, userValue);
            ROCKSDB_NAMESPACE::Slice sliceKey(key.data(), key.size());
            stateTable->rocksDb->Put(stateTable->writeOptions, stateTable->table, sliceKey, sliceValue);
        }

    public:
        RocksdbMapStateTable<K, N, UK, UV> *stateTable;
        int32_t userKeyOffset;
        std::vector<char> key;
        std::vector<char> value;
        bool deleted = false;
        UK userKey = nullptr;
        UV userValue = nullptr;
    };

    class RocksDBMapIterator : public java_util_Iterator {
    public:
        explicit RocksDBMapIterator(RocksdbMapStateTable<K, N, UK, UV> *stateTable)
        {
            this->stateTable = stateTable;
        }

        ~RocksDBMapIterator() override
        {
            for (size_t i = 0; i < cacheEntries.size(); ++i) {
                delete cacheEntries[i];
            }
            cacheEntries.clear();
        }

        bool hasNext() override
        {
            loadCache();
            return (cacheIndex < cacheEntries.size());
        }

        Object *next() override
        {
            RocksDBMapEntry* entry = nextEntry();
            if (entry != nullptr) {
                entry->getRefCount();
            }
            return entry;
        }

        void remove() override
        {
            if (currentEntry == nullptr || currentEntry->deleted) {
                THROW_LOGIC_EXCEPTION("The remove operation must be called after a valid next operation.")
            }

            currentEntry->remove();
        }

        RocksDBMapEntry *nextEntry()
        {
            loadCache();
            if (cacheIndex == cacheEntries.size()) {
                if (!expired) {
                    THROW_LOGIC_EXCEPTION("nextEntry IllegalStateException")
                }

                return nullptr;
            }

            currentEntry = cacheEntries[cacheIndex];
            cacheIndex++;
            return currentEntry;
        }

        void loadCache()
        {
            if (cacheIndex > cacheEntries.size()) {
                THROW_LOGIC_EXCEPTION("loadCache IllegalStateException")
            }

            // Load cache entries only when the cache is empty and there still exist unread entries
            if (cacheIndex < cacheEntries.size() || expired) {
                return;
            }

            auto currentKey = stateTable->keyContext->getCurrentKey();

            // 序列化key
            DataOutputSerializer keyOutputSerializer;
            OutputBufferStatus outputBufferStatus;
            keyOutputSerializer.setBackendBuffer(&outputBufferStatus);

            if constexpr (std::is_pointer_v<K>) {
                stateTable->getKeySerializer()->serialize(currentKey, keyOutputSerializer);
            } else {
                stateTable->getKeySerializer()->serialize(&currentKey, keyOutputSerializer);
            }

            ROCKSDB_NAMESPACE::Slice sliceKey(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                              keyOutputSerializer.length());
            ROCKSDB_NAMESPACE::Iterator* iterator = stateTable->rocksDb->NewIterator(stateTable->readOptions, stateTable->table);
            if (iterator == nullptr) {
                expired = true;
                return;
            }
            iterator->Seek(sliceKey);

            // free memory
            for (size_t i = 0; i < cacheEntries.size(); ++i) {
                delete cacheEntries[i];
                cacheEntries[i] = nullptr;
            }
            cacheEntries.clear();
            cacheIndex = 0;
            /*
             * If the entry pointing to the current position is not removed, it will be the first entry in the
             * new iterating. Skip it to avoid redundant access in such cases.
             */
            if (currentEntry != nullptr && !currentEntry->deleted) {
                iterator->Next();
            }

            while (true) {
                if (!iterator->Valid() || !iterator->key().starts_with(sliceKey)) {
                    expired = true;
                    break;
                }

                if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
                    break;
                }

                size_t keyPrefixLen = sliceKey.size();
                ROCKSDB_NAMESPACE::Slice key = iterator->key();
                ROCKSDB_NAMESPACE::Slice value = iterator->value();
                auto *entry = new RocksDBMapEntry(stateTable, keyPrefixLen, key, value);
                cacheEntries.push_back(entry);
                iterator->Next();
            }
            delete iterator;
        }

    protected:
        size_t CACHE_SIZE_LIMIT = 128;
        RocksdbMapStateTable<K, N, UK, UV> *stateTable;
        bool expired = false;
        std::vector<RocksDBMapEntry*> cacheEntries;
        size_t cacheIndex = 0;
        RocksDBMapEntry *currentEntry = nullptr;
    };

    class StateEntryIterator : public InternalKvState<K, N, UV>::StateIncrementalVisitor {
    public:
        UV nextEntries() override;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords, RocksdbMapStateTable<K, N, UK, UV> *table);

        bool hasNext() override;

        void remove(const K &key, const N &nameSpace, const UV &state) override;

    private:
        int recommendedMaxNumberOfReturnedRecords;
        int keyGroupIndex;
        typename InternalKvState<K, N, UV>::StateIncrementalVisitor *stateIncrementalVisitor;
        RocksdbMapStateTable<K, N, UK, UV> *table;

        void init();
    };

protected:
    // Variables
    InternalKeyContext<K> *keyContext;
    TypeSerializer *keySerializer;
    TypeSerializer *userKeySerializer;
    // KeyGroupRange *keyGroupRange;
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* table; // 是不是编程columnsFamily
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* VBTable;
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo;
    int size = 0;
    long vectorBatchId = 0;
    ROCKSDB_NAMESPACE::DB* rocksDb;
    ROCKSDB_NAMESPACE::ReadOptions readOptions;
    ROCKSDB_NAMESPACE::WriteOptions writeOptions;

    ROCKSDB_NAMESPACE::Slice serializerKeyAndUserKey(DataOutputSerializer &outputSerializer, UK userKey)
    {
        auto currentKey = keyContext->getCurrentKey();

        // 序列化key, userKey

        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, outputSerializer);
        }

        if constexpr (std::is_pointer_v<UK>) {
            getUserKeySerializer()->serialize(userKey, outputSerializer);
        } else {
            getUserKeySerializer()->serialize(&userKey, outputSerializer);
        }
        return ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(outputSerializer.getData()),
                                        outputSerializer.length());
    }

    ROCKSDB_NAMESPACE::Slice serializerValue(DataOutputSerializer &valueOutputSerializer, UV userValue)
    {
        // value序列化
        TypeSerializer *vSerializer = getStateSerializer();

        if constexpr (std::is_pointer_v<UV>) {
            vSerializer->serialize(userValue, valueOutputSerializer);
        } else {
            vSerializer->serialize(&userValue, valueOutputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
    }
};

template<typename K, typename N, typename UK, typename UV>
RocksdbMapStateTable<K, N, UK, UV>::RocksdbMapStateTable(InternalKeyContext<K> *keyContext,
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo,
    TypeSerializer *keySerializer, TypeSerializer *userKeySerializer)
{
    this->keyContext = keyContext;
    this->metaInfo = std::move(metaInfo);
    this->keySerializer = keySerializer;
    this->userKeySerializer = userKeySerializer;
    writeOptions.disableWAL = true;
}

template<typename K, typename N, typename UK, typename UV>
RocksdbMapStateTable<K, N, UK, UV>::~RocksdbMapStateTable()
{
    delete table;
    delete VBTable;
}

template<typename K, typename N, typename UK, typename UV>
std::vector<K> *RocksdbMapStateTable<K, N, UK, UV>::getKeys(const N &nameSpace)
{
    return nullptr;
}

template<typename K, typename N, typename UK, typename UV>
std::vector<std::tuple<K, N>> *RocksdbMapStateTable<K, N, UK, UV>::getKeysAndNamespace()
{
    return nullptr;
}

#endif // OMNISTREAM_ROCKSDBMAPSTATETABLE_H

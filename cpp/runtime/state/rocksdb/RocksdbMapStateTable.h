/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by c00572813 on 2025/4/17.
//

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
#include "core/io/DataOutputSerializer.h"
#include "core/io/DataInputDeserializer.h"
#include "core/typeutils/LongSerializer.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "core/include/emhash7.hpp"

#include "../../../util/MathUtils.h"
/* S is the value used in the State,
 * like RowData* for HeapValueState,
 * emhash7<RowData*, int>* for HeapMapState,
 * vector<int64_t>* for List State
 */
template<typename K, typename N, typename UK, typename UV>
class RocksdbMapStateTable {
public:
    RocksdbMapStateTable(InternalKeyContext<K> *keyContext, RegisteredKeyValueStateBackendMetaInfo *metaInfo,
                      TypeSerializer *keySerializer, TypeSerializer *userKeySerializer);  // 加入db initData

    ~RocksdbMapStateTable();

    bool isEmpty()
    {
        // NOT_IMPL_EXCEPTION
        return size == 0;
    };

    void createTable(ROCKSDB_NAMESPACE::DB *db, std::string cfName)
    {
        LOG("create MapState column family")
        this->rocksDb = db;
        ROCKSDB_NAMESPACE::Status s = db->CreateColumnFamily(ROCKSDB_NAMESPACE::ColumnFamilyOptions(), cfName, &table);
        s = db->CreateColumnFamily(ROCKSDB_NAMESPACE::ColumnFamilyOptions(), cfName + "vb", &VBTable);
    }

    UV get(const N &nameSpace, const UK &userKey)
    {
        // 和Rocksdb交互的时候要try catch
        LOG("RocksdbMapStateTable value get")

        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(userKey);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), table, sliceKey, &valueInTable);
        if (!s.ok() || valueInTable.length() == 0) {
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }

        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()),
                                             valueInTable.length(), 0);
        void *resPtr = getStateSerializer()->deserialize(serializedData);
        if constexpr (std::is_pointer_v<UV>) {
            return (UV)resPtr;
        } else {
            return *(UV *)resPtr;
        }
    };

    void put(const N &nameSpace, const UK &userKey, const UV &state)
    {
        // 存入
        LOG("RocksDB put")
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(userKey);
        ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(state);
        rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), table, sliceKey, sliceValue);
    };

     void remove(const N &nameSpace, const UK &userKey)
     {
         // 删除
         ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(userKey);
         rocksDb->Delete(ROCKSDB_NAMESPACE::WriteOptions(), table, sliceKey);
     };

    emhash7::HashMap<UK, UV>* entries(const N &nameSpace)
    {
        emhash7::HashMap<UK, UV>* resultMap = new emhash7::HashMap<UK, UV>();
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
        iterator->Seek(sliceKey);
        // 遍历以指定前缀开头的键值对
        for (; iterator->Valid() && iterator->key().starts_with(sliceKey); iterator->Next()) {
            ROCKSDB_NAMESPACE::Slice key = iterator->key();
            key.remove_prefix(sliceKey.size());
            ROCKSDB_NAMESPACE::Slice value = iterator->value();
            UK entryKey;
            UV entryValue;

            DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(key.data()), key.size(), 0);
            void *resPtr = getUserKeySerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<UV>) {
                entryKey = (UK)resPtr;
            } else {
                entryKey = *(UK *)resPtr;
            }

            serializedData = DataInputDeserializer(reinterpret_cast<const uint8_t *>(value.data()), value.size(), 0);
            resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<UV>) {
                entryValue = (UV)resPtr;
            } else {
                entryValue = *(UV *)resPtr;
            }
            resultMap->emplace(entryKey, entryValue);
        }
        if (resultMap->size() == 0) {
            return nullptr;
        } else {
            return resultMap;
        }
    }

    typename InternalKvState<K, N, UV>::StateIncrementalVisitor *getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords);

    RegisteredKeyValueStateBackendMetaInfo *getMetaInfo()
    {
        return metaInfo;
    }

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo *newMetaInfo)
    {
        metaInfo = newMetaInfo;
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
        uint8_t *buffer = new uint8_t[batchSize];
        omnistream::SerializedBatchInfo serializedBatchInfo =
            omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ROCKSDB_NAMESPACE::Slice vbValue(reinterpret_cast<const char *>(serializedBatchInfo.buffer),
                                         serializedBatchInfo.size);

        auto res = rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), VBTable, key, vbValue);
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
        auto s = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), VBTable, key, &valueInTable);
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
    RegisteredKeyValueStateBackendMetaInfo *metaInfo;
    int size = 0;
    long vectorBatchId = 0;
    ROCKSDB_NAMESPACE::DB* rocksDb;

    ROCKSDB_NAMESPACE::Slice serializerKeyAndUserKey(UK userKey)
    {
        auto currentKey = keyContext->getCurrentKey();

        // 序列化key, userKey
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

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

    ROCKSDB_NAMESPACE::Slice serializerValue(UV userValue)
    {
        // value序列化
        TypeSerializer *vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

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
    RegisteredKeyValueStateBackendMetaInfo *metaInfo, TypeSerializer *keySerializer, TypeSerializer *userKeySerializer)
{
    this->keyContext = keyContext;
    this->metaInfo = metaInfo;
    this->keySerializer = keySerializer;
    this->userKeySerializer = userKeySerializer;
}

template<typename K, typename N, typename UK, typename UV>
RocksdbMapStateTable<K, N, UK, UV>::~RocksdbMapStateTable()
{
    rocksDb->Close();
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

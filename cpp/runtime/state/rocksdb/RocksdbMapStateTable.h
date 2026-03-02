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
#include <unordered_set>
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
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"

#include "../../../core/utils/MathUtils.h"
#include "basictypes/java_util_Iterator.h"
#include "basictypes/java_util_Map_Entry.h"
#include "utils/VectorBatchDeserializationUtils.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "state/RocksDbKvStateInfo.h"
#include "runtime/state/DefaultConfigurableOptionsFactory.h"
#include "common.h"
#include <sstream>

const int FALCON_PREFIX_PARAM = 13;

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

        // [FALCON]-----------------------------------------------------------------------------------------------
        // familyOptions.memtable_factory.reset(ROCKSDB_NAMESPACE::NewHashLinkListRepFactory());
        familyOptions.prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(FALCON_PREFIX_PARAM));
        // familyOptions.compression = ROCKSDB_NAMESPACE::CompressionType::kZlibCompression;
        INFO_RELEASE("[FALCON] enable prefix for mapState.")
        // [FALCON]-----------------------------------------------------------------------------------------------

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

        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);

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
        bool isNull = serializedData.readBoolean();
        if(isNull){
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }
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
                UV value = *(UV*)resPtr;
                delete (UV*)resPtr;
                return value;
            }
        }
    };

    std::shared_ptr<std::string> getRawBytes(const N &nameSpace,UK& uk)
    {
        // outputSerializer free need after Get called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, uk, nameSpace);

        ROCKSDB_NAMESPACE::PinnableSlice pinSlice;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &pinSlice);
        if (!s.ok() || pinSlice.size() == 0) {
            return nullptr;
        }
        return std::make_shared<std::string>(pinSlice.data(), pinSlice.size());
    };

    void GetByBatch(const N &nameSpace, std::unordered_map<K,std::unordered_set<XXH128_hash_t>> &dataToGet,std::unordered_map<std::pair<K,XXH128_hash_t>,UV> &result)
    {

        std::vector<std::pair<K,XXH128_hash_t>> kvpairs;
        std::vector<std::string> key_strings;
        for (auto &item : dataToGet) {
            K currentKey = item.first;
            keyContext->setCurrentKey(currentKey);

            for (auto &userKey : item.second) {
                // outputSerializer free need after Delete called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);
                key_strings.emplace_back(sliceKey.data(),sliceKey.size());
                kvpairs.push_back({currentKey,userKey} );
            }
        }

        std::vector<ROCKSDB_NAMESPACE::Slice> key_slices;
        for (const auto &key_string : key_strings) {
            key_slices.emplace_back(key_string.data(),key_string.size());
        }

        std::vector<rocksdb::Status> statuses(key_slices.size());
        std::vector<rocksdb::PinnableSlice> resultsValue(key_slices.size());
        rocksdb::ReadOptions ro;
        ro.async_io = true;
        rocksDb->MultiGet(ro, table,
             (int)key_slices.size(), key_slices.data(),
             resultsValue.data(), statuses.data(),false);

        for (size_t i = 0; i < kvpairs.size(); ++i)
        {
            const auto& logicalPair = kvpairs[i];
            const rocksdb::Status& st = statuses[i];

            if (st.ok())
            {
                // Found in RocksDB
                const rocksdb::PinnableSlice& val = resultsValue[i];
                UV value = decodeValueFromPinnable(val);
                result.emplace(logicalPair,value);

            }
        }
    }


    UV decodeValueFromPinnable(const rocksdb::PinnableSlice& val) {
        // Build a read-only view into the pinned bytes:
        DataInputDeserializer in(
            reinterpret_cast<const uint8_t*>(val.data()),
            static_cast<int>(val.size()),
            /*position=*/0
        );

        // Your serializer:
        auto* stateSerializer = getStateSerializer();
        bool isNull = in.readBoolean();
        if(isNull){
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }
        if constexpr (std::is_same_v<UV, Object*>) {
            // Special path that deserializes into an existing "buffer" the serializer owns.
            auto buffer = stateSerializer->GetBuffer();
            stateSerializer->deserialize(buffer, in);
            // Ownership of 'buffer' follows your serializer contract.
            return buffer;
        } else {
            // Generic path that returns a heap pointer to the decoded object.
            void* resPtr = stateSerializer->deserialize(in);

            if constexpr (std::is_pointer_v<UV>) {
                // Return the pointer as-is. Caller must know who frees it.
                return static_cast<UV>(resPtr);
            } else {
                // Copy out the object and free the temporary heap allocation.
                UV value = *static_cast<UV*>(resPtr);
                delete static_cast<UV*>(resPtr);
                return value;
            }
        }
    }



    void put(const N &nameSpace, const UK &userKey, const UV &state)
    {
        // 存入
        LOG("RocksDB put")
        // outputSerializer free need after Put called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);

        // valueOutputSerializer free need after Put called
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, state);
        rocksDb->Put(writeOptions, table, sliceKey, sliceValue);
    };

    void putByBatch(const N &nameSpace, const K &key,const std::unordered_map<UK,UV> &dataToAdd)
    {
        keyContext->setCurrentKey(key);
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto &item : dataToAdd) {
            UK userKey = item.first;
            auto value = item.second;
                // outputSerializer free need after Put called
            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);

            // valueOutputSerializer free need after Put called
            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            ROCKSDB_NAMESPACE::Slice  sliceValue = serializerValue(valueOutputSerializer, (UV)value);
            putBatch.Put(table,sliceKey,sliceValue);
        }
        auto ret = rocksDb->Write(writeOptions, &putBatch);

    }

    void putByBatch(const N &nameSpace, std::unordered_map<K, std::unordered_map<UK,UV>> &dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto &item : dataToAdd)
        {
            K key = item.first;
            auto userKeyValues = item.second;
            keyContext->setCurrentKey(key);
            for (auto &item : userKeyValues) {
                UK userKey = item.first;
                auto value = item.second;
                // outputSerializer free need after Put called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);

                // valueOutputSerializer free need after Put called
                DataOutputSerializer valueOutputSerializer;
                OutputBufferStatus valueOutputBufferStatus;
                valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
                ROCKSDB_NAMESPACE::Slice  sliceValue = serializerValue(valueOutputSerializer, (UV)value);
                putBatch.Put(table,sliceKey,sliceValue);
            }
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void putByBatch(const N &nameSpace, std::vector<std::shared_ptr<std::tuple<K,UK,std::shared_ptr<std::string>>>>& dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto& item : dataToAdd) {
            K key = std::get<0>(*item);
            keyContext->setCurrentKey(key);
            UK ukey = std::get<1>(*item);
            std::shared_ptr<std::string> strPtr = std::get<2>(*item);
            // outputSerializer free need after Put called
            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, ukey, nameSpace);

            // valueOutputSerializer free need after Put called
            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceValue(strPtr->data(), strPtr->size());
            putBatch.Put(table, sliceKey, sliceValue);
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void remove(const N &nameSpace, const UK &userKey)
    {
        // 删除
        // outputSerializer free need after Delete called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);
        rocksDb->Delete(writeOptions, table, sliceKey);
    };

    void removeByBatch(const N &nameSpace, std::unordered_map<K,std::unordered_set<UK>> &dataToRemove)
    {
        ROCKSDB_NAMESPACE::WriteBatch deleteBatch;
        for (auto &item : dataToRemove) {
            K currentKey = item.first;
            keyContext->setCurrentKey(currentKey);

            for (auto &userKey : item.second) {
                // outputSerializer free need after Delete called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);
                deleteBatch.Delete(table,sliceKey);
            }
        }
         auto ret = rocksDb->Write(writeOptions, &deleteBatch);
    }

    void addByBatch(const N &nameSpace, std::unordered_map<K,std::unordered_set<RowData*>> &dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto &item : dataToAdd) {
            K& currentKey = item.first;
            keyContext->setCurrentKey(currentKey);

            for (auto &userKey : item.second) {
                // outputSerializer free need after Put called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);

                // valueOutputSerializer free need after Put called
                DataOutputSerializer valueOutputSerializer;
                OutputBufferStatus valueOutputBufferStatus;
                valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
                ROCKSDB_NAMESPACE::Slice  sliceValue = serializerValue(valueOutputSerializer, (UV)userKey);
                putBatch.Put(table,sliceKey,sliceValue);
            }
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    bool contains(const N &nameSpace, const UK &userKey)
    {
        // 和Rocksdb交互的时候要try catch
        LOG("RocksdbMapStateTable value contains")
        // outputSerializer free need after Get called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() || valueInTable.length() == 0) {
            return false;
        }
        return true;
    }

    std::unordered_map<K, emhash7::HashMap<UK, UV>> entriesCache;
    emhash7::HashMap<UK, UV>* entries(const N &nameSpace)
    {
        // auto* resultMap = new emhash7::HashMap<UK, UV>();
        auto currentKey = keyContext->getCurrentKey();
        if (auto it = entriesCache.find(currentKey); it != entriesCache.end()) {
            return it->second.size() >0 ? & it->second : nullptr;
        }

        emhash7::HashMap<UK, UV> resultMap;

        // 序列化key
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeByte(static_cast<uint32_t>(keyContext->getCurrentKeyGroupIndex()));
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, keyOutputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, keyOutputSerializer);
        }
        // serializer namespace
        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, keyOutputSerializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, keyOutputSerializer);
        }

        ROCKSDB_NAMESPACE::Slice sliceKey(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                          keyOutputSerializer.length());

        // [FALCON]------------------------------------------------------------------------------------------------
        ROCKSDB_NAMESPACE::ReadOptions readOption;
        if (sliceKey.size() < FALCON_PREFIX_PARAM) {
            readOption.total_order_seek = true;
        } else {
            readOption.total_order_seek = false;
        }
        // INFO_RELEASE("[FALCON] performing prefix length check.") // avoid too much log print

        ROCKSDB_NAMESPACE::Iterator* iterator = rocksDb->NewIterator(readOption, table);
        // [FALCON]------------------------------------------------------------------------------------------------

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
                    UK temp = (UK)resPtr;
                    BinaryRowData* rd = dynamic_cast<BinaryRowData*>(temp);
                    if (rd!=nullptr)
                    {
                        entryKey = (UK)(rd->copy());
                    }else
                    {
                        entryKey = (UK)resPtr;
                    }
                } else {
                    entryKey = *(UK *)resPtr;
                    delete resPtr;
                }
            }

            serializedData = DataInputDeserializer(reinterpret_cast<const uint8_t *>(value.data()), value.size(), 0);
            bool isNull = serializedData.readBoolean();
            if(isNull){
                if constexpr (std::is_pointer_v<UV>) {
                    entryValue = nullptr;
                } else {
                    entryValue = std::numeric_limits<UV>::max();
                }
            } else {
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
                        delete resPtr;
                    }
                }
            }
            resultMap.emplace(std::move(entryKey), std::move(entryValue));
        }

        delete iterator;

        auto [it, inserted] = entriesCache.emplace(currentKey, std::move(resultMap));
        return it->second.size() >0 ? &it->second: nullptr;
    }

    java_util_Iterator *iterator(const N &nameSpace)
    {
        auto it = new RocksDBMapIterator(this, nameSpace);
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
        std::vector<uint8_t> buf(batchSize);
        auto *buffer= buf.data();
        omnistream::SerializedBatchInfo serializedBatchInfo =
            omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ROCKSDB_NAMESPACE::Slice vbValue(reinterpret_cast<const char *>(serializedBatchInfo.buffer),
                                         serializedBatchInfo.size);

        auto res = rocksDb->Put(writeOptions, VBTable, key, vbValue);
        // delete [] buffer;
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
                bool isNull = serializedData.readBoolean();
                if(isNull){
                    if constexpr (std::is_pointer_v<UV>) {
                        return nullptr;
                    } else {
                        return std::numeric_limits<UV>::max();
                    }
                }
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
        explicit RocksDBMapIterator(RocksdbMapStateTable<K, N, UK, UV> *stateTable, const N &nameSpace)
        {
            this->stateTable = stateTable;
            this->nameSpace = nameSpace;
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
            keyOutputSerializer.writeByte(static_cast<uint32_t>(stateTable->keyContext->getCurrentKeyGroupIndex()));
            if constexpr (std::is_pointer_v<K>) {
                stateTable->getKeySerializer()->serialize(currentKey, keyOutputSerializer);
            } else {
                stateTable->getKeySerializer()->serialize(&currentKey, keyOutputSerializer);
            }
            // serializer namespace
            if constexpr (std::is_pointer_v<N>) {
                stateTable->getNamespaceSerializer()->serialize(nameSpace, keyOutputSerializer);
            } else {
                stateTable->getNamespaceSerializer()->serialize(&nameSpace, keyOutputSerializer);
            }
            ROCKSDB_NAMESPACE::Slice sliceKey(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                              keyOutputSerializer.length());

            // [FALCON]------------------------------------------------------------------------------------------------
            ROCKSDB_NAMESPACE::ReadOptions readOption = stateTable->readOptions;
            if (sliceKey.size() < FALCON_PREFIX_PARAM || currentEntry != nullptr) {
                readOption.total_order_seek = true;
            } else {
                readOption.total_order_seek = false;
            }
            // INFO_RELEASE("[FALCON] loadCache performing prefix length check.") // too much log print

            ROCKSDB_NAMESPACE::Iterator* iterator = stateTable->rocksDb->NewIterator(readOption, stateTable->table);
            // [FALCON]------------------------------------------------------------------------------------------------

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
        const N &nameSpace;
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

    ROCKSDB_NAMESPACE::Slice serializerKeyAndUserKey(DataOutputSerializer &outputSerializer, UK userKey, const N &nameSpace)
    {
        auto currentKey = keyContext->getCurrentKey();

        // 序列化key, userKey
        outputSerializer.writeByte(static_cast<uint32_t>(keyContext->getCurrentKeyGroupIndex()));
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, outputSerializer);
        }
        // serializer namespace
        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, outputSerializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, outputSerializer);
        }

        if constexpr (std::is_pointer_v<UK>) {
            getUserKeySerializer()->serialize(userKey, outputSerializer);
        } else {
            getUserKeySerializer()->serialize(&userKey, outputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(outputSerializer.getData()),
                                        outputSerializer.length());
    }

    ROCKSDB_NAMESPACE::Slice serializerKey(DataOutputSerializer &outputSerializer)
    {
        auto currentKey = keyContext->getCurrentKey();
        
        // 序列化key, userKey
        outputSerializer.writeByte(static_cast<uint32_t>(keyContext->getCurrentKeyGroupIndex()));
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, outputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(outputSerializer.getData()),
                                outputSerializer.length());
    }

    ROCKSDB_NAMESPACE::Slice serializerValue(DataOutputSerializer &valueOutputSerializer, UV userValue)
    {
        // value序列化
        TypeSerializer *vSerializer = getStateSerializer();

        if constexpr (std::is_pointer_v<UV>) {
            valueOutputSerializer.writeBoolean(userValue == nullptr);
            vSerializer->serialize(userValue, valueOutputSerializer);
        } else {
            valueOutputSerializer.writeBoolean(&userValue == nullptr);
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

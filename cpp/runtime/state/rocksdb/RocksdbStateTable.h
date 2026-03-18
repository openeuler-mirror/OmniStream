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

#ifndef OMNISTREAM_ROCKSDBSTATETABLE_H
#define OMNISTREAM_ROCKSDBSTATETABLE_H

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
#include "runtime/state/DefaultConfigurableOptionsFactory.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "state/rocksdb/RocksDbStringAppendOperator.h"

#include "table/utils/VectorBatchSerializationUtils.h"
#include "table/utils/VectorBatchDeserializationUtils.h"

#include "../../../core/utils/MathUtils.h"
#include "state/RocksDbKvStateInfo.h"

const int FALCON_HASH_PARAM = 13;

/* S is the value used in the State,
 * like RowData* for HeapValueState,
 * emhash7<RowData*, int>* for HeapMapState,
 * vector<int64_t>* for List State
 */
template<typename K, typename N, typename S>
class RocksdbStateTable {
public:
    RocksdbStateTable(InternalKeyContext<K> *keyContext,
                      std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo,
                      TypeSerializer *keySerializer);  // 加入db initData

    virtual ~RocksdbStateTable();

    bool isEmpty()
    {
        // NOT_IMPL_EXCEPTION
        return size == 0;
    };

    void createTable(ROCKSDB_NAMESPACE::DB *db, std::string cfName,
                     std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation)
    {
        this->rocksDb = db;
        rocksdb::Options options;
        options.create_if_missing = true;
        // set merge method, listState need
        options.merge_operator.reset(new RocksDbStringAppendOperator(','));
        ROCKSDB_NAMESPACE::ColumnFamilyOptions familyOptions(options);

        // [FALCON] -----------------------------------------------------------------------------------------------
        if (metaInfo->getStateType() == StateDescriptor::Type::VALUE) {
			// modify columnFamily option and read option for current columnFamily
            familyOptions.memtable_factory.reset(ROCKSDB_NAMESPACE::NewHashLinkListRepFactory());
            familyOptions.prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(FALCON_HASH_PARAM));
			readOptions.total_order_seek = true;
            INFO_RELEASE("[FALCON] enable hash memTable for valueState.")
        }
        // [FALCON] -----------------------------------------------------------------------------------------------

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

    int getHashCode(K key)
    {
        if constexpr (std::is_same_v<K, RowData *>) {
            // std::hash<RowData*> uses a simpler hasher. But here we need to keep it same as java
            int hashCode = key->hashCode();
            return MathUtils::murmurHash(hashCode);
        } else {
            int hashCode = std::hash<K>{}(key);
            return MathUtils::murmurHash(hashCode);
        }
    }

    ROCKSDB_NAMESPACE::Slice GetKeyNameSpaceSlice(DataOutputSerializer &outputSerializer, N &nameSpace)
    {
        auto currentKey = keyContext->getCurrentKey();

        outputSerializer.writeByte(static_cast<uint32_t>(keyContext->getCurrentKeyGroupIndex()));
        if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, int32_t>) {
            LongSerializer::INSTANCE->serialize(&currentKey, outputSerializer);
        } else if constexpr (std::is_pointer_v<K>) {
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
        return ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(outputSerializer.getData()),
            outputSerializer.length());
    }

    S get(N &nameSpace)
    {
        // 和Rocksdb交互的时候要try catch
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() && valueInTable.length() != 0) {
            THROW_RUNTIME_ERROR("rocksdb state table get failed, status is << " << s.ToString());
        }
        if (!s.ok() || valueInTable.length() == 0) {
            if constexpr (std::is_pointer_v<S>) {
                return nullptr;
            } else {
                return std::numeric_limits<S>::max();
            }
        }

        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()),
                                             valueInTable.length(), 0);
        if constexpr (std::is_same_v<S, Object*>) {
            auto stateSerializer = getStateSerializer();
            auto buffer = stateSerializer->GetBuffer();
            stateSerializer->deserialize(buffer, serializedData);
            return buffer;
        } else {
            void *resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<S>) {
                return (S) resPtr;
            } else {
                return resPtr == nullptr ? std::numeric_limits<S>::max() : *(S *) resPtr;
            }
        }
    };

    template<typename UV>
    UV getRes(void *res)
    {
        if constexpr (std::is_pointer_v<UV>) {
            return (UV) res;
        } else {
            return *(UV) res;
        }
    }

    void putByBatch(N &nameSpace, std::unordered_map<K, S>& pendingUpdates)
    {
        // 存入
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto& entry : pendingUpdates) {
            RowData* key = entry.first;
            S state = entry.second;

            keyContext->setCurrentKey(key);
            LOG("RocksDB put");
            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

            // value序列化
            TypeSerializer *vSerializer = getStateSerializer();
            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

            S tmpS = state;

            if constexpr (std::is_pointer_v<S>) {
                vSerializer->serialize(tmpS, valueOutputSerializer);
            } else {
                vSerializer->serialize(&tmpS, valueOutputSerializer);
            }

            ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                                valueOutputSerializer.length());
            putBatch.Put(table,sliceKey,sliceValue);
        }
        auto s3 = rocksDb->Write(writeOptions, &putBatch);

        if (!s3.ok()) {
            THROW_RUNTIME_ERROR("rocksdb state table put by batch failed, status is << " << s3.ToString());
        }
    };

    void put(N &nameSpace, const S &state)
    {
        // 存入
        LOG("RocksDB put");
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        // value序列化
        TypeSerializer *vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        S tmpS = state;

        if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }

        ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                            valueOutputSerializer.length());
        auto s3 = rocksDb->Put(writeOptions, table, sliceKey, sliceValue);
        if (!s3.ok()) {
            THROW_RUNTIME_ERROR("rocksdb state table put failed, status is << " << s3.ToString());
        }
    };

    void clear(N &nameSpace)
    {
        // 存入
        LOG("RocksDB value state clear")
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);
        rocksDb->Delete(writeOptions, table, sliceKey);
    }

    // for list state
    void add(N &nameSpace, const S &value)
    {
        // 存入
        LOG("RocksDB list value state add")
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        // value序列化
        TypeSerializer *vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        S tmpS = value;

        if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }

        ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                            valueOutputSerializer.length());

        const rocksdb::Status &status = rocksDb->Merge(writeOptions, table, sliceKey, sliceValue);
        if (!status.ok()) {
            std::cout << "Status not ok!!" << std::endl;
        }
    }

    std::vector<S>* getList(N &nameSpace)
    {
        LOG("RocksDB list value state get")
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() || valueInTable.length() == 0) {
            return nullptr;
        }
        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()),
                                             valueInTable.length(), 0);
        return deserializeList(serializedData);
    }

    void addAll(N &nameSpace, const vector<S> &values)
    {
        // 存入
        LOG("RocksDB list value state addAll")
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        const rocksdb::Slice &slice = serializeList(values);
        rocksDb->Merge(writeOptions, table, sliceKey, slice);
    }

    typename InternalKvState<K, N, S>::StateIncrementalVisitor *getStateIncrementalVisitor(
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

    // [FALCON] function which will be called in RocksdbValueState
    K getCurrentKey() { return keyContext->getCurrentKey(); }
    void setCurrentKey(K newKey) { keyContext->setCurrentKey(newKey); }

    class StateEntryIterator : public InternalKvState<K, N, S>::StateIncrementalVisitor {
    public:
        S nextEntries() override;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords, RocksdbStateTable<K, N, S> *table);

        bool hasNext() override;

    private:
        int recommendedMaxNumberOfReturnedRecords;
        int keyGroupIndex;
        typename InternalKvState<K, N, S>::StateIncrementalVisitor *stateIncrementalVisitor;
        RocksdbStateTable<K, N, S> *table;

        void init();
    };

protected:
    // Variables
    InternalKeyContext<K> *keyContext;
    TypeSerializer *keySerializer;
    // KeyGroupRange *keyGroupRange;
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *table; // 是不是编程columnsFamily
    ROCKSDB_NAMESPACE::ColumnFamilyHandle *VBTable;
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo;
    int size = 0;
    long vectorBatchId = 0;
    ROCKSDB_NAMESPACE::DB *rocksDb;
    byte DELIMITER = (byte)',';
    ROCKSDB_NAMESPACE::ReadOptions readOptions;
    ROCKSDB_NAMESPACE::WriteOptions writeOptions;

    ROCKSDB_NAMESPACE::Slice serializeList(const std::vector<S> &values)
    {
        // value序列化
        TypeSerializer *vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        bool first = true;
        for (const auto &item: values) {
            if (first) {
                first = false;
            } else {
                valueOutputSerializer.write((uint32_t)DELIMITER);
            }

            if constexpr (std::is_pointer_v<S>) {
                if (item == nullptr) {
                    THROW_RUNTIME_ERROR("You cannot add null to a value list.")
                }
                vSerializer->serialize(item, valueOutputSerializer);
            } else {
                vSerializer->serialize((void *) &item, valueOutputSerializer);
            }
        }
        return ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
    }

    std::vector<S>* deserializeList(DataInputDeserializer serializedData)
    {
        auto* result = new std::vector<S>();
        while (serializedData.Available() > 0) {
            void *resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<S>) {
                result->push_back((S) resPtr);
            } else {
                result->push_back(*(S *)resPtr);
            }
            if (serializedData.Available() > 0) {
                serializedData.readByte();
            }
        }
        return result;
    }
};

template<typename K, typename N, typename S>
RocksdbStateTable<K, N, S>::RocksdbStateTable(InternalKeyContext<K> *keyContext,
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo, TypeSerializer *keySerializer)
{
    this->keyContext = keyContext;
    this->metaInfo = std::move(metaInfo);
    this->keySerializer = keySerializer;
    writeOptions.disableWAL = true;
}

template<typename K, typename N, typename S>
RocksdbStateTable<K, N, S>::~RocksdbStateTable()
{}

template<typename K, typename N, typename S>
std::vector<K> *RocksdbStateTable<K, N, S>::getKeys(const N &nameSpace)
{
    return nullptr;
}

template<typename K, typename N, typename S>
std::vector<std::tuple<K, N>> *RocksdbStateTable<K, N, S>::getKeysAndNamespace()
{
    return nullptr;
}

#endif // OMNISTREAM_ROCKSDBSTATETABLE_H

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

#ifndef OMNISTREAM_BSSSTATETABLE_H
#define OMNISTREAM_BSSSTATETABLE_H
#ifdef WITH_OMNISTATESTORE

#include "typeutils/LongSerializer.h"
#include "memory/DataInputDeserializer.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "utils/VectorBatchDeserializationUtils.h"
#include "boost_state_table.h"
#include "boost_state_db.h"
#include "table_description.h"
#include "state/HashCode.h"

template<typename K, typename N, typename S>
class BssStateTable {
public:
    BssStateTable(InternalKeyContext<K> *keyContext, RegisteredKeyValueStateBackendMetaInfo *metaInfo,
                  TypeSerializer *keySerializer) : keyContext(keyContext), metaInfo(metaInfo),
        keySerializer(keySerializer) {};

    bool isEmpty()
    {
        return size == 0;
    }

    void createTable(ock::bss::BoostStateDBPtr &_dbPtr)
    {
        this->dbPtr = _dbPtr;
        auto tblDesc = std::make_shared<ock::bss::TableDescription>(
                ock::bss::StateType::VALUE, "dbTable", -1, ock::bss::TableSerializer{}, dbPtr->GetConfig());
        dbTable = std::dynamic_pointer_cast<ock::bss::KVTable>(_dbPtr->GetTableOrCreate(tblDesc));
    };

    S get(N &nameSpace)
    {
        LOG("bss state table get");
        uint32_t keyHashCode;

        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        ock::bss::BinaryData readValue;
        auto res = dbTable->Get(keyHashCode, priKey, readValue);
        if (res != ock::bss::BSS_OK || readValue.Length() == 0) {
            if constexpr (std::is_pointer_v<S>) {
                return nullptr;
            } else {
                return std::numeric_limits<S>::max();
            }
        }
        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(readValue.Data()),
            readValue.Length(), 0);
        void *resPtr = getStateSerializer()->deserialize(serializedData);
        if constexpr (std::is_pointer_v<S>) {
            return (S) resPtr;
        } else {
            return resPtr == nullptr ? std::numeric_limits<S>::max() : *(S *) resPtr;
        }
    }

    void put(N &nameSpace, const S &state)
    {
        LOG("BSS state table put")
        uint32_t keyHashCode;
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        TypeSerializer *vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        S tmpS = state;
        if constexpr (std::is_same_v<S, int64_t> || std::is_same_v<S, int32_t>) {
            LongSerializer::INSTANCE->serialize(&tmpS, valueOutputSerializer);
        } else if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }
        ock::bss::BinaryData priValue(valueOutputSerializer.getData(),
            static_cast<int32_t>(valueOutputSerializer.getPosition()));
        auto res = dbTable->Put(keyHashCode, priKey, priValue);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: put failed")
        }
    }

    void clear(N &nameSpace)
    {
        LOG("BSS state table clear")
        uint32_t keyHashCode;
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        auto res = dbTable->Remove(keyHashCode, priKey);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: clear failed")
        }
    }

    void add(N &nameSpace, const S &value)
    {
        LOG("BSS state table add")
        uint32_t keyHashCode;
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        S tmpS = value;
        TypeSerializer *vSerializer = getStateSerializer();
        if constexpr (std::is_same_v<S, int64_t> || std::is_same_v<S, int32_t>) {
            LongSerializer::INSTANCE->serialize(&tmpS, valueOutputSerializer);
        } else if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }
        ock::bss::BinaryData priVal(valueOutputSerializer.getData(),
            static_cast<int32_t>(valueOutputSerializer.getPosition()));
        auto res = dbTable->Add(keyHashCode, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: add failed")
        }
    }

    ock::bss::BinaryData GetPriBinaryData(N &nameSpace, uint32_t &keyHashCode, DataOutputSerializer &serializer)
    {
        auto currentKey = keyContext->getCurrentKey();
        // serialize key
        if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, int32_t>) {
            LongSerializer::INSTANCE->serialize(&currentKey, serializer);
        } else if constexpr (std::is_pointer_v<K>) {
            keySerializer->serialize(currentKey, serializer);
        } else {
            keySerializer->serialize(&currentKey, serializer);
        }
        // serialize
        if constexpr (std::is_same_v<N, int64_t> || std::is_same_v<N, int32_t>) {
            LongSerializer::INSTANCE->serialize(&nameSpace, serializer);
        } else if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, serializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, serializer);
        }
        ock::bss::BinaryData priBinaryData(serializer.getData(), static_cast<int32_t>(serializer.getPosition()));
        keyHashCode = HashCode::Hash(serializer.getData(), static_cast<int32_t>(serializer.getPosition()));
        return priBinaryData;
    }

    long getVectorBatchesSize()
    {
        return vectorBatchId;
    }

    void addVectorBatch(omnistream::VectorBatch *vectorBatch)
    {
        LOG("Bss state table addVectorBatch")
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&vectorBatchId, keyOutputSerializer);
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(),
            static_cast<int32_t>(keyOutputSerializer.getPosition()));
        uint32_t keyHashCode = HashCode::Hash(keyOutputSerializer.getData(),
            static_cast<int32_t>(keyOutputSerializer.getPosition()));
        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        uint8_t *buffer = new uint8_t[batchSize];
        omnistream::SerializedBatchInfo serializedBatchInfo =
                omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ock::bss::BinaryData priVal(serializedBatchInfo.buffer, serializedBatchInfo.size);
        auto res = dbTable->Put(keyHashCode, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: addVectorBatch failed")
        }
        vectorBatchId++;
    }

    omnistream::VectorBatch *getVectorBatch(long batchId)
    {
        LOG("Bss state table getVectorBatch")
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&batchId, keyOutputSerializer);
        uint32_t keyHashCode = HashCode::Hash(keyOutputSerializer.getData(),
            static_cast<int32_t>(keyOutputSerializer.getPosition()));
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(),
            static_cast<uint32_t>(keyOutputSerializer.getPosition()));

        ock::bss::BinaryData priVal;
        auto res = dbTable->Get(keyHashCode, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: getVectorBatch failed")
        }
        uint8_t* address = const_cast<uint8_t *>(priVal.Data() + sizeof(int8_t));
        auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
        return batch;
    }

    TypeSerializer *getNamespaceSerializer()
    {
        return metaInfo->getNamespaceSerializer();
    }

    TypeSerializer *getStateSerializer()
    {
        return metaInfo->getStateSerializer();
    }

    RegisteredKeyValueStateBackendMetaInfo *getMetaInfo()
    {
        return metaInfo;
    }

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo *newMetaInfo)
    {
        metaInfo = newMetaInfo;
    }
protected:
    InternalKeyContext<K> *keyContext;
    RegisteredKeyValueStateBackendMetaInfo *metaInfo;
    TypeSerializer *keySerializer;
    ock::bss::BoostStateDBPtr dbPtr;
    ock::bss::ConfigRef config;
    ock::bss::KVTableRef dbTable;
    int size = 0;
    long vectorBatchId = 0;
};

#endif // OMNISTREAM_BSSSTATETABLE_H
#endif
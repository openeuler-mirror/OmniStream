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

#ifndef OMNISTREAM_BSSLISTSTATETABLE_H
#define OMNISTREAM_BSSLISTSTATETABLE_H
#ifdef WITH_OMNISTATESTORE

#include "config.h"
#include "boost_state_table.h"
#include "state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "state/InternalKeyContext.h"
#include "boost_state_db.h"

template<typename K, typename N, typename UV>
class BssListStateTable {
public:
    BssListStateTable(InternalKeyContext<K> *keyContext, RegisteredKeyValueStateBackendMetaInfo *metaInfo,
        TypeSerializer *keySerializer) : keyContext(keyContext), metaInfo(metaInfo),
        keySerializer(keySerializer) {
    }

    bool isEmpty()
    {
        return true;
    }

    void createTable(ock::bss::BoostStateDBPtr &_dbPtr)
    {
        this->dbPtr = _dbPtr;
        auto tblDesc = std::make_shared<ock::bss::TableDescription>(
                ock::bss::StateType::LIST, "dbTable", -1, ock::bss::TableSerializer{}, _dbPtr->GetConfig());
        dbTable = std::dynamic_pointer_cast<ock::bss::KListTable>(_dbPtr->GetTableOrCreate(tblDesc));
    };

    std::vector<UV>* get(N &nameSpace)
    {
        LOG("BSS ListState table Get")
        if constexpr (is_same_v<N, long>) {
            LOG("namespace is " << nameSpace)
        }

        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        ock::bss::ListResult getResult = dbTable->Get(keyHashCode, priKey);
        std::vector<UV>* result = new std::vector<UV>();
        for (std::size_t i = 0; i < getResult.addresses.size(); ++i) {
            DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(getResult.addresses.at(i)),
                getResult.lengths.at(i), 0);
            void *resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<UV>) {
                result->push_back(static_cast<UV>(resPtr));
            } else {
                result->push_back(resPtr ? *static_cast<UV*>(resPtr) : std::numeric_limits<UV>::max());
            }
        }
        dbTable->CleanResource(getResult.resId);
        return result;
    }

    // overwrite data in bss list table
    void put(N &nameSpace, const UV &state)
    {
        LOG("BSS ListState table put")
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        TypeSerializer *vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        UV tmpS = state;
        if constexpr (std::is_same_v<UV, int64_t> || std::is_same_v<UV, int32_t>) {
            LongSerializer::INSTANCE->serialize(&tmpS, valueOutputSerializer);
        } else if constexpr (std::is_pointer_v<UV>) {
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

    // append data in bss list table
    void add(N &nameSpace, const UV &value)
    {
        LOG("BSS ListState table add")
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        UV tmpS = value;
        TypeSerializer *vSerializer = getStateSerializer();
        if constexpr (std::is_same_v<UV, int64_t> || std::is_same_v<UV, int32_t>) {
            LongSerializer::INSTANCE->serialize(&tmpS, valueOutputSerializer);
        } else if constexpr (std::is_pointer_v<UV>) {
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

    void addAll(N &nameSpace, const vector<UV> &values)
    {
        LOG("BSS ListState table addAll")
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
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
            if constexpr (std::is_pointer_v<UV>) {
                if (item == nullptr) {
                    THROW_RUNTIME_ERROR("You cannot add null to a value list.")
                }
                vSerializer->serialize(item, valueOutputSerializer);
            } else {
                vSerializer->serialize((void *) &item, valueOutputSerializer);
            }
        }
        ock::bss::BinaryData priVal(valueOutputSerializer.getData(),
                                    static_cast<int32_t>(valueOutputSerializer.getPosition()));
        auto res = dbTable->Add(keyHashCode, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: add failed")
        }
    }

    void clear(N &nameSpace)
    {
        LOG("BSS ListState table clear")
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        dbTable->Remove(keyHashCode, priKey);
    }

    void addVectorBatch(omnistream::VectorBatch *vectorBatch)
    {
        LOG("BSS ListState table addVectorBatch")
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&vectorBatchId, keyOutputSerializer);
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
        uint32_t keyHashCode = HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length()));

        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        uint8_t *buffer = new uint8_t[batchSize];
        omnistream::SerializedBatchInfo serializedBatchInfo =
                omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ock::bss::BinaryData priBatchVal(serializedBatchInfo.buffer, serializedBatchInfo.size);
        auto res = dbTable->Put(keyHashCode, priKey, priBatchVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: addVectorBatch failed")
        }
        vectorBatchId++;
    }

    omnistream::VectorBatch *getVectorBatch(long batchId)
    {
        LOG("BSS ListState table getVectorBatch")
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&batchId, keyOutputSerializer);
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
        uint32_t keyHashCode = HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length()));
        auto getResult = dbTable->Get(keyHashCode, priKey);
        auto address = reinterpret_cast<uint8_t *>(reinterpret_cast<uint8_t *>(getResult.addresses.at(0)) +
            sizeof(int8_t));
        dbTable->CleanResource(getResult.resId);
        auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
        return batch;
    }

    long getVectorBatchesSize()
    {
        return vectorBatchId;
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

    ock::bss::BinaryData GetPriBinaryData(N &nameSpace, uint32_t &keyHashCode, DataOutputSerializer &serializer)
    {
        auto currentKey = keyContext->getCurrentKey();
        // serialize key

        if constexpr (is_same_v<long, K>) {
            LOG("get current key is " << currentKey)
        }
        if constexpr (is_same_v<N, long>) {
            LOG("namespace is " << nameSpace)
        }
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
        ock::bss::BinaryData priBinaryData(serializer.getData(),
            static_cast<int32_t>(serializer.getPosition()));
        keyHashCode = HashCode::Hash(serializer.getData(), static_cast<int32_t>(serializer.getPosition()));
        return priBinaryData;
    }

private:
    InternalKeyContext<K> *keyContext;
    RegisteredKeyValueStateBackendMetaInfo *metaInfo;
    TypeSerializer *keySerializer;
    ock::bss::BoostStateDBPtr dbPtr{};
    ock::bss::ConfigRef config;
    ock::bss::KListTableRef dbTable;
    int size = 0;
    long vectorBatchId = 0;
    byte DELIMITER = (byte)',';
};

#endif // OMNISTREAM_BSSLISTSTATETABLE_H
#endif
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

#ifndef OMNISTREAM_BSSMAPSTATETABLE_H
#define OMNISTREAM_BSSMAPSTATETABLE_H
#ifdef WITH_OMNISTATESTORE

#include "state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "state/InternalKeyContext.h"
#include "boost_state_table.h"
#include "config.h"
#include "boost_state_db.h"
#include "typeutils/LongSerializer.h"
#include "memory/DataInputDeserializer.h"
#include "emhash7.hpp"
#include "table/data/vectorbatch/VectorBatch.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "utils/VectorBatchDeserializationUtils.h"

template<typename K, typename N, typename UK, typename UV>
class BssMapStateTable {
public:
    BssMapStateTable(InternalKeyContext<K> *keyContext, TypeSerializer *keySerializer,
        TypeSerializer *userKeySerializer, RegisteredKeyValueStateBackendMetaInfo *metaInfo) : keyContext(keyContext),
        keySerializer(keySerializer), userKeySerializer(userKeySerializer), metaInfo(metaInfo) {};

    bool isEmpty()
    {
        return true;
    }

    void createTable(ock::bss::BoostStateDBPtr &_dbPtr)
    {
        auto tblDesc = std::make_shared<ock::bss::TableDescription>(
                ock::bss::StateType::MAP, "dbTable", -1, ock::bss::TableSerializer{}, _dbPtr->GetConfig());
        dbTable = std::dynamic_pointer_cast<ock::bss::KMapTable>(_dbPtr->GetTableOrCreate(tblDesc));
    };

    UV get(N &nameSpace, const UK &userKey)
    {
        LOG("bss MapState table get");
        // hashcode is used to determine the position of map, not the specified element
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        uint32_t hashCode;
        ock::bss::BinaryData priNamespace = GetNamespaceBinaryData(nameSpace, serializer, hashCode);
        OutputBufferStatus outputBufferStatus1;
        serializer.setBackendBuffer(&outputBufferStatus1);
        ock::bss::BinaryData priUserKey = GetUserKeyBinaryData(userKey, serializer);
        ock::bss::BinaryData readValue;
        // namespace is the key of Kmap, userKey is the key of value of the Kmap
        auto res = dbTable->Get(hashCode, priNamespace, priUserKey, readValue);
        if (res != ock::bss::BSS_OK || readValue.Length() == 0) {
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }
        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(readValue.Data()),
                                             readValue.Length(), 0);
        void *resPtr = getStateSerializer()->deserialize(serializedData);
        if constexpr (std::is_pointer_v<UV>) {
            return (UV) resPtr;
        } else {
            return resPtr == nullptr ? std::numeric_limits<UV>::max() : *(UV *) resPtr;
        }
    }

    void put(N &nameSpace, const UK &userKey, const UV &state)
    {
        LOG("BSS Map State table put")
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priUserKey = GetUserKeyBinaryData(userKey, serializer);
        TypeSerializer *vSerializer = getStateSerializer();
        OutputBufferStatus valueOutputBufferStatus;
        serializer.setBackendBuffer(&valueOutputBufferStatus);
        UV tmpS = state;

        if constexpr (std::is_pointer_v<UV>) {
            vSerializer->serialize(tmpS, serializer);
        } else {
            vSerializer->serialize(&tmpS, serializer);
        }
        ock::bss::BinaryData priValue(serializer.getData(),
                                      static_cast<uint32_t>(serializer.getPosition()));
        OutputBufferStatus outputBufferStatus1;
        serializer.setBackendBuffer(&outputBufferStatus1);
        uint32_t hashCode;
        ock::bss::BinaryData priNamespaceKey = GetNamespaceBinaryData(nameSpace, serializer, hashCode);
        auto res = dbTable->Put(hashCode, priNamespaceKey, priUserKey, priValue);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: put failed")
        }
    }

    void remove(N &nameSpace, const UK &userKey)
    {
        LOG("BSS MapState table remove")
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        uint32_t keyHashCode;
        ock::bss::BinaryData priNameSpace = GetNamespaceBinaryData(nameSpace, serializer, keyHashCode);
        OutputBufferStatus outputBufferStatus1;
        serializer.setBackendBuffer(&outputBufferStatus1);
        ock::bss::BinaryData priUserKey = GetUserKeyBinaryData(userKey, serializer);
        auto res = dbTable->Remove(keyHashCode, priNameSpace, priUserKey);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: clear failed")
        }
    }

    emhash7::HashMap<UK, UV>* entries(N &nameSpace)
    {
        LOG("BSS MapState table entries")
        auto* resultMap = new emhash7::HashMap<UK, UV>();
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        uint32_t keyHashCode;
        ock::bss::BinaryData priBinaryData = GetNamespaceBinaryData(nameSpace, serializer, keyHashCode);
        auto pMapIteratorWrraper = dbTable->EntryIteratorWrraper(keyHashCode, priBinaryData);
        while (pMapIteratorWrraper->HasNext()) {
            LOG("get element from wrapper")
            auto pairs = pMapIteratorWrraper->Next();
            if (pairs.size() != 2) {
                LOG("ERROR: get the element from mapState is wrong size")
                THROW_LOGIC_EXCEPTION("ERROR: get the element from mapState is wrong size")
            }
            auto keyData = pairs.at(0);
            DataInputDeserializer keySerializedData(reinterpret_cast<const uint8_t *>(keyData.Data()),
                                                    static_cast<int>(keyData.Length()), 0);
            void *keyResPtr = getStateSerializer()->deserialize(keySerializedData);
            UK userKey;
            if constexpr (std::is_pointer_v<UK>) {
                userKey = (UK) keyResPtr;
            } else {
                userKey = (keyResPtr == nullptr ? std::numeric_limits<UK>::max() : *(UK *) keyResPtr);
            }

            auto valData = pairs.at(1);
            DataInputDeserializer valSerializedData(reinterpret_cast<const uint8_t *>(valData.Data()),
                                                    static_cast<int>(valData.Length()), 0);
            void *valResPtr = getStateSerializer()->deserialize(valSerializedData);
            UV userVal;
            if constexpr (std::is_pointer_v<UV>) {
                userVal = (UV) valResPtr;
            } else {
                userVal = (valResPtr == nullptr ? std::numeric_limits<UV>::max() : *(UV *) valResPtr);
            }
            resultMap->emplace(userKey, userVal);
        }
        delete pMapIteratorWrraper;
        LOG("get entries size is " << resultMap->size())
        if (resultMap->size() == 0) {
            delete resultMap;
            return nullptr;
        } else {
            return resultMap;
        }
    }

    void addVectorBatch(omnistream::VectorBatch *vectorBatch)
    {
        LOG("BSS MapState table addVectorBatch")
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&vectorBatchId, keyOutputSerializer);
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(), static_cast<uint32_t>(keyOutputSerializer.getPosition()));
        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        uint8_t *buffer = new uint8_t[batchSize];
        omnistream::SerializedBatchInfo serializedBatchInfo =
                omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ock::bss::BinaryData priVal(serializedBatchInfo.buffer, static_cast<uint32_t>(serializedBatchInfo.size));

        OutputBufferStatus outputBufferStatus1;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus1);
        uint32_t keyHashCode;
        auto vectorBatchKey = GetVectorBatchNameSpaceKey(keyOutputSerializer, keyHashCode);
        auto res = dbTable->Put(keyHashCode, vectorBatchKey, priKey, priVal);
        LOG("add result " << res)
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: addAll failed")
        }
        vectorBatchId++;
    }

    omnistream::VectorBatch *getVectorBatch(long batchId)
    {
        LOG("BSS MapState table getVectorBatch")
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        longSerializer.serialize(&batchId, keyOutputSerializer);
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(),
                                    static_cast<uint32_t>(keyOutputSerializer.getPosition()));
        ock::bss::BinaryData priVal;
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus1;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus1);
        auto vectorBatchKey = GetVectorBatchNameSpaceKey(keyOutputSerializer, keyHashCode);
        auto res = dbTable->Get(keyHashCode, vectorBatchKey, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: getVectorBatch failed")
        }
        uint8_t* address = const_cast<uint8_t *>(priVal.Data() + sizeof(int8_t));
        auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
        return batch;
    }

    ock::bss::BinaryData GetNamespaceBinaryData(N &nameSpace, DataOutputSerializer &serializer, uint32_t &hashCode)
    {
        auto currentKey = keyContext->getCurrentKey();
        // serialize key

        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, serializer);
        } else {
            getKeySerializer()->serialize(&currentKey, serializer);
        }
        // serialize namespace
        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, serializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, serializer);
        }
        ock::bss::BinaryData priBinaryData(serializer.getData(),
                                           static_cast<uint32_t>(serializer.getPosition()));
        hashCode = HashCode::Hash(priBinaryData.Data(), priBinaryData.Length());
        return priBinaryData;
    }

    ock::bss::BinaryData GetUserKeyBinaryData(UK userKey, DataOutputSerializer &serializer)
    {
        // serialize user key
        if constexpr (std::is_pointer_v<UK>) {
            getUserKeySerializer()->serialize(userKey, serializer);
        } else {
            getUserKeySerializer()->serialize(&userKey, serializer);
        }
        ock::bss::BinaryData priBinaryData(serializer.getData(), static_cast<uint32_t>(serializer.getPosition()));
        return priBinaryData;
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

    TypeSerializer *getKeySerializer()
    {
        return keySerializer;
    }

    TypeSerializer *getUserKeySerializer()
    {
        return userKeySerializer;
    }

    ock::bss::BinaryData GetVectorBatchNameSpaceKey(DataOutputSerializer &serializer, uint32_t &keyHashCode)
    {
        LongSerializer::INSTANCE->serialize(&vectorBatchNamespaceKey, serializer);
        ock::bss::BinaryData priBinaryData(serializer.getData(), static_cast<int32_t>(serializer.getPosition()));
        keyHashCode = HashCode::Hash(priBinaryData.Data(), static_cast<int32_t>(priBinaryData.Length()));
        return priBinaryData;
    }

    long GetVectorBatchesSize()
    {
        return vectorBatchId;
    }
private:
    InternalKeyContext<K> *keyContext;
    TypeSerializer *keySerializer;
    TypeSerializer *userKeySerializer;
    RegisteredKeyValueStateBackendMetaInfo *metaInfo;
    ock::bss::ConfigRef config;
    ock::bss::KMapTableRef dbTable;
    int size = 0;
    long vectorBatchId = 0;
    long vectorBatchNamespaceKey = 1;
};
#endif // OMNISTREAM_BSSMAPSTATETABLE_H
#endif
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

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "state/InternalKeyContext.h"
#include "state/HashCode.h"
#include "state/bss/BssKeyGroupUtils.h"
#include "boost_state_table.h"
#include "config.h"
#include "boost_state_db.h"
#include "typeutils/LongSerializer.h"
#include "memory/DataInputDeserializer.h"
#include "emhash7.hpp"
#include "table/data/vectorbatch/VectorBatch.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "utils/VectorBatchDeserializationUtils.h"
#include "state/bss/BssExceptionUtils.h"
#include "data/util/SequenceNumberHelper.h"

template <typename K, typename N, typename UK, typename UV>
class BssMapStateTable {
public:
    BssMapStateTable(
        InternalKeyContext<K>* keyContext,
        TypeSerializer* keySerializer,
        TypeSerializer* userKeySerializer,
        RegisteredKeyValueStateBackendMetaInfo* metaInfo)
        : keyContext(keyContext),
          keySerializer(keySerializer),
          userKeySerializer(userKeySerializer),
          metaInfo(metaInfo),
          sequenceNumberHelper(keyContext->getNumberOfKeyGroups()) {};

    ~BssMapStateTable()
    {
        delete metaInfo;
    }

    bool isEmpty()
    {
        return true;
    }

    void createTable(ock::bss::BoostStateDBPtr& _dbPtr)
    {
        if (_dbPtr == nullptr) {
            throw std::invalid_argument("OmniStateStore database must not be null");
        }
        auto tblDesc = std::make_shared<ock::bss::TableDescription>(
            ock::bss::StateType::MAP, metaInfo->getName(), -1, ock::bss::TableSerializer{}, _dbPtr->GetConfig());
        dbTable = bss_adapter::CheckTable(
            std::dynamic_pointer_cast<ock::bss::KMapTable>(_dbPtr->GetTableOrCreate(tblDesc)), metaInfo->getName());
    };

    UV get(N& nameSpace, const UK& userKey)
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
        if (bss_adapter::IsNotFound(res) || (res == ock::bss::BSS_OK && readValue.Length() == 0)) {
            bss_adapter::LogStateOperationSuccess("MAP", metaInfo->getName(), "get", hashCode, "found=false");
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }
        bss_adapter::CheckResult(res, "KMapTable::Get(" + metaInfo->getName() + ")");
        bss_adapter::LogStateOperationSuccess(
            "MAP", metaInfo->getName(), "get", hashCode, "found=true, valueBytes=" +
                std::to_string(readValue.Length()));
        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t*>(readValue.Data()), readValue.Length(), 0);
        void* resPtr = getStateSerializer()->deserialize(serializedData);
        if constexpr (std::is_pointer_v<UV>) {
            return (UV)resPtr;
        } else {
            return resPtr == nullptr ? std::numeric_limits<UV>::max() : *(UV*)resPtr;
        }
    }

    void put(N& nameSpace, const UK& userKey, const UV& state)
    {
        LOG("BSS Map State table put");
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priUserKey = GetUserKeyBinaryData(userKey, serializer);
        TypeSerializer* vSerializer = getStateSerializer();
        OutputBufferStatus valueOutputBufferStatus;
        serializer.setBackendBuffer(&valueOutputBufferStatus);
        UV tmpS = state;

        if constexpr (std::is_pointer_v<UV>) {
            vSerializer->serialize(tmpS, serializer);
        } else {
            vSerializer->serialize(&tmpS, serializer);
        }
        ock::bss::BinaryData priValue(serializer.getData(), static_cast<uint32_t>(serializer.getPosition()));
        OutputBufferStatus outputBufferStatus1;
        serializer.setBackendBuffer(&outputBufferStatus1);
        uint32_t hashCode;
        ock::bss::BinaryData priNamespaceKey = GetNamespaceBinaryData(nameSpace, serializer, hashCode);
        auto res = dbTable->Put(hashCode, priNamespaceKey, priUserKey, priValue);
        bss_adapter::CheckResult(res, "KMapTable::Put(" + metaInfo->getName() + ")");
        bss_adapter::LogStateOperationSuccess(
            "MAP", metaInfo->getName(), "put", hashCode, "valueBytes=" + std::to_string(priValue.Length()));
    }

    void remove(N& nameSpace, const UK& userKey)
    {
        LOG("BSS MapState table remove");
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        uint32_t keyHashCode;
        ock::bss::BinaryData priNameSpace = GetNamespaceBinaryData(nameSpace, serializer, keyHashCode);
        OutputBufferStatus outputBufferStatus1;
        serializer.setBackendBuffer(&outputBufferStatus1);
        ock::bss::BinaryData priUserKey = GetUserKeyBinaryData(userKey, serializer);
        auto res = dbTable->Remove(keyHashCode, priNameSpace, priUserKey);
        if (!bss_adapter::IsNotFound(res)) {
            bss_adapter::CheckResult(res, "KMapTable::RemoveEntry(" + metaInfo->getName() + ")");
        }
        bss_adapter::LogStateOperationSuccess(
            "MAP", metaInfo->getName(), "remove", keyHashCode,
            bss_adapter::IsNotFound(res) ? "removed=false" : "removed=true");
    }

    bool contains(N& nameSpace, const UK& userKey)
    {
        DataOutputSerializer serializer;
        OutputBufferStatus namespaceBuffer;
        serializer.setBackendBuffer(&namespaceBuffer);
        uint32_t hashCode;
        auto namespaceData = GetNamespaceBinaryData(nameSpace, serializer, hashCode);
        OutputBufferStatus userKeyBuffer;
        serializer.setBackendBuffer(&userKeyBuffer);
        auto userKeyData = GetUserKeyBinaryData(userKey, serializer);
        bool found = dbTable->Contain(hashCode, namespaceData, userKeyData);
        bss_adapter::LogStateOperationSuccess(
            "MAP", metaInfo->getName(), "contains", hashCode, found ? "found=true" : "found=false");
        return found;
    }

    void clear(N& nameSpace)
    {
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        uint32_t hashCode;
        auto namespaceData = GetNamespaceBinaryData(nameSpace, serializer, hashCode);
        auto res = dbTable->Remove(hashCode, namespaceData);
        if (!bss_adapter::IsNotFound(res)) {
            bss_adapter::CheckResult(res, "KMapTable::RemoveNamespace(" + metaInfo->getName() + ")");
        }
        bss_adapter::LogStateOperationSuccess(
            "MAP", metaInfo->getName(), "clear", hashCode,
            bss_adapter::IsNotFound(res) ? "removed=false" : "removed=true");
    }

    emhash7::HashMap<UK, UV>* entries(N& nameSpace)
    {
        LOG("BSS MapState table entries");
        auto* resultMap = new emhash7::HashMap<UK, UV>();
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        uint32_t keyHashCode;
        ock::bss::BinaryData priBinaryData = GetNamespaceBinaryData(nameSpace, serializer, keyHashCode);
        auto iterator = std::unique_ptr<ock::bss::MapIteratorWrraper>(
            dbTable->EntryIteratorWrraper(keyHashCode, priBinaryData));
        if (iterator == nullptr) {
            delete resultMap;
            throw std::runtime_error("OmniStateStore failed to create map iterator for state '" + metaInfo->getName() + "'");
        }
        while (iterator->HasNext()) {
            LOG("get element from wrapper");
            auto pairs = iterator->Next();
            if (pairs.size() != 2) {
                LOG("ERROR: get the element from mapState is wrong size");
                THROW_LOGIC_EXCEPTION("ERROR: get the element from mapState is wrong size");
            }
            auto keyData = pairs.at(0);
            DataInputDeserializer keySerializedData(
                reinterpret_cast<const uint8_t*>(keyData.Data()), static_cast<int>(keyData.Length()), 0);
            void* keyResPtr = getUserKeySerializer()->deserialize(keySerializedData);
            UK userKey;
            if constexpr (std::is_pointer_v<UK>) {
                userKey = (UK)keyResPtr;
            } else {
                userKey = (keyResPtr == nullptr ? std::numeric_limits<UK>::max() : *(UK*)keyResPtr);
            }

            auto valData = pairs.at(1);
            DataInputDeserializer valSerializedData(
                reinterpret_cast<const uint8_t*>(valData.Data()), static_cast<int>(valData.Length()), 0);
            void* valResPtr = getStateSerializer()->deserialize(valSerializedData);
            UV userVal;
            if constexpr (std::is_pointer_v<UV>) {
                userVal = (UV)valResPtr;
            } else {
                userVal = (valResPtr == nullptr ? std::numeric_limits<UV>::max() : *(UV*)valResPtr);
            }
            resultMap->emplace(userKey, userVal);
        }
        LOG("get entries size is " << resultMap->size());
        bss_adapter::LogStateOperationSuccess(
            "MAP", metaInfo->getName(), "entries", keyHashCode,
            "elementCount=" + std::to_string(resultMap->size()));
        if (resultMap->size() == 0) {
            delete resultMap;
            return nullptr;
        } else {
            return resultMap;
        }
    }

    uint32_t getNextSequenceNumber(int32_t keyGroup)
    {
        return sequenceNumberHelper.getNextSequenceNumber(keyGroup);
    }

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
    {
        auto sequenceNumber = sequenceNumberHelper.getNextSequenceNumber(keyGroup);
        LOG("BSS MapState table addVectorBatch");
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        ock::bss::BinaryData priKey(
            keyOutputSerializer.getData(), static_cast<uint32_t>(keyOutputSerializer.getPosition()));
        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        std::vector<uint8_t> bufferStorage(batchSize);
        auto* serializationBuffer = bufferStorage.data();
        omnistream::SerializedBatchInfo serializedBatchInfo =
            omnistream::VectorBatchSerializationUtils::serializeVectorBatch(
                vectorBatch, batchSize, serializationBuffer);
        ock::bss::BinaryData priVal(serializedBatchInfo.buffer, static_cast<uint32_t>(serializedBatchInfo.size));

        DataOutputSerializer namespaceSerializer;
        OutputBufferStatus namespaceOutputBufferStatus;
        namespaceSerializer.setBackendBuffer(&namespaceOutputBufferStatus);
        uint32_t keyHashCode;
        auto vectorBatchKey = GetVectorBatchNameSpaceKey(namespaceSerializer, keyHashCode, keyGroup);
        auto res = dbTable->Put(keyHashCode, vectorBatchKey, priKey, priVal);
        LOG("add result " << res);
        bss_adapter::CheckResult(res, "KMapTable::PutVectorBatch(" + metaInfo->getName() + ")");
        sequenceNumberHelper.addNextSequenceNumber(keyGroup);
    }

    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        for (const auto& [keyGroup, vectorBatch] : vectorBatchByKeyGroup) {
            keyOutputSerializer.clear();
            auto sequenceNumber = sequenceNumberHelper.getNextSequenceNumber(keyGroup);
            keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
            long sequenceNumberForSerializer = sequenceNumber;
            longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
            ock::bss::BinaryData priKey(
                keyOutputSerializer.getData(), static_cast<uint32_t>(keyOutputSerializer.getPosition()));
            auto batchSize =
                omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
            std::vector<uint8_t> bufferStorage(batchSize);
            // serializeVectorBatch 的 buffer 形参是 uint8_t*&（内部当游标前移），
            // 必须传具名左值；返回的 SerializedBatchInfo.buffer 仍指向起始位置
            auto* buffer = bufferStorage.data();
            auto serializedBatchInfo =
                omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
            ock::bss::BinaryData priVal(serializedBatchInfo.buffer, static_cast<uint32_t>(serializedBatchInfo.size));
            DataOutputSerializer namespaceSerializer;
            OutputBufferStatus namespaceOutputBufferStatus;
            namespaceSerializer.setBackendBuffer(&namespaceOutputBufferStatus);
            uint32_t keyHashCode;
            auto vectorBatchKey = GetVectorBatchNameSpaceKey(namespaceSerializer, keyHashCode, keyGroup);
            if (dbTable->Put(keyHashCode, vectorBatchKey, priKey, priVal) != ock::bss::BSS_OK) {
                THROW_RUNTIME_ERROR("Failed to add VectorBatch to BSS for keyGroup " << keyGroup);
            }
            sequenceNumberHelper.addNextSequenceNumber(keyGroup);
        }
    }

    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
    {
        LOG("BSS MapState table getVectorBatch");
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        ock::bss::BinaryData priKey(
            keyOutputSerializer.getData(), static_cast<uint32_t>(keyOutputSerializer.getPosition()));
        ock::bss::BinaryData priVal;
        uint32_t keyHashCode;
        DataOutputSerializer namespaceSerializer;
        OutputBufferStatus namespaceOutputBufferStatus;
        namespaceSerializer.setBackendBuffer(&namespaceOutputBufferStatus);
        auto vectorBatchKey = GetVectorBatchNameSpaceKey(namespaceSerializer, keyHashCode, keyGroup);
        auto res = dbTable->Get(keyHashCode, vectorBatchKey, priKey, priVal);
        if (bss_adapter::IsNotFound(res)) {
            return nullptr;
        }
        bss_adapter::CheckResult(res, "KMapTable::GetVectorBatch(" + metaInfo->getName() + ")");
        if (priVal.Length() <= sizeof(int8_t)) {
            throw std::runtime_error("OmniStateStore returned an invalid VectorBatch map payload");
        }
        uint8_t* address = const_cast<uint8_t*>(priVal.Data() + sizeof(int8_t));
        auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
        return batch;
    }

    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup)
    {
        NOT_IMPL_EXCEPTION;
    }

    void clearVectorBatches(int64_t currentTimestamp)
    {
        auto* keyGroupRange = keyContext->getKeyGroupRange();
        for (int32_t keyGroup = keyGroupRange->getStartKeyGroup(); keyGroup <= keyGroupRange->getEndKeyGroup();
             ++keyGroup) {
            auto nextSequenceNumber = getNextSequenceNumber(keyGroup);
            for (uint32_t sequenceNumber = 0; sequenceNumber < nextSequenceNumber; ++sequenceNumber) {
                auto* vectorBatch = getVectorBatch(keyGroup, sequenceNumber);
                if (vectorBatch != nullptr && vectorBatch->isEmpty(currentTimestamp)) {
                    removeVectorBatch(keyGroup, sequenceNumber);
                }
                delete vectorBatch;
            }
        }
    }

    void clearVectorBatches(int32_t keyGroup, const std::vector<uint32_t>& sequenceNumbersToDelete)
    {
        for (auto sequenceNumber : sequenceNumbersToDelete) {
            removeVectorBatch(keyGroup, sequenceNumber);
        }
    }

    ock::bss::BinaryData GetNamespaceBinaryData(N& nameSpace, DataOutputSerializer& serializer, uint32_t& hashCode)
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
        ock::bss::BinaryData priBinaryData(serializer.getData(), static_cast<uint32_t>(serializer.getPosition()));
        hashCode = BssKeyGroupUtils::ForceKeyGroup(
            HashCode::Hash(priBinaryData.Data(), priBinaryData.Length()),
            static_cast<uint32_t>(keyContext->getCurrentKeyGroupIndex()),
            static_cast<uint32_t>(keyContext->getNumberOfKeyGroups()));
        return priBinaryData;
    }

    ock::bss::BinaryData GetUserKeyBinaryData(UK userKey, DataOutputSerializer& serializer)
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
    TypeSerializer* getNamespaceSerializer()
    {
        return metaInfo->getNamespaceSerializer();
    }

    TypeSerializer* getStateSerializer()
    {
        return metaInfo->getStateSerializer();
    }

    RegisteredKeyValueStateBackendMetaInfo* getMetaInfo()
    {
        return metaInfo;
    }

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo* newMetaInfo)
    {
        metaInfo = newMetaInfo;
    }

    TypeSerializer* getKeySerializer()
    {
        return keySerializer;
    }

    TypeSerializer* getUserKeySerializer()
    {
        return userKeySerializer;
    }

    ock::bss::BinaryData GetVectorBatchNameSpaceKey(
        DataOutputSerializer& serializer, uint32_t& keyHashCode, int32_t keyGroup)
    {
        LongSerializer::INSTANCE->serialize(&vectorBatchNamespaceKey, serializer);
        ock::bss::BinaryData priBinaryData(serializer.getData(), static_cast<int32_t>(serializer.getPosition()));
        keyHashCode = BssKeyGroupUtils::ForceKeyGroup(
            HashCode::Hash(priBinaryData.Data(), static_cast<int32_t>(priBinaryData.Length())),
            static_cast<uint32_t>(keyGroup),
            static_cast<uint32_t>(keyContext->getNumberOfKeyGroups()));
        return priBinaryData;
    }

private:
    void removeVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        ock::bss::BinaryData priKey(
            keyOutputSerializer.getData(), static_cast<uint32_t>(keyOutputSerializer.getPosition()));
        DataOutputSerializer namespaceSerializer;
        OutputBufferStatus namespaceOutputBufferStatus;
        namespaceSerializer.setBackendBuffer(&namespaceOutputBufferStatus);
        uint32_t keyHashCode;
        auto vectorBatchKey = GetVectorBatchNameSpaceKey(namespaceSerializer, keyHashCode, keyGroup);
        dbTable->Remove(keyHashCode, vectorBatchKey, priKey);
    }

    InternalKeyContext<K>* keyContext;
    TypeSerializer* keySerializer;
    TypeSerializer* userKeySerializer;
    RegisteredKeyValueStateBackendMetaInfo* metaInfo;
    ock::bss::ConfigRef config;
    ock::bss::KMapTableRef dbTable;
    int size = 0;
    omnistream::SequenceNumberHelper sequenceNumberHelper{}; // only used for VectorBatch storage
    long vectorBatchNamespaceKey = 1;
};
#endif // OMNISTREAM_BSSMAPSTATETABLE_H
#endif

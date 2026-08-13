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

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "boost_state_table.h"
#include "memory/DataInputDeserializer.h"
#include "state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "state/InternalKeyContext.h"
#include "state/HashCode.h"
#include "state/bss/BssKeyGroupUtils.h"
#include "boost_state_db.h"
#include "state/bss/BssExceptionUtils.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table_description.h"
#include "typeutils/LongSerializer.h"
#include "utils/VectorBatchDeserializationUtils.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "data/util/SequenceNumberHelper.h"

template <typename K, typename N, typename UV>
class BssListStateTable {
public:
    BssListStateTable(
        InternalKeyContext<K>* keyContext,
        RegisteredKeyValueStateBackendMetaInfo* metaInfo,
        TypeSerializer* keySerializer)
        : keyContext(keyContext),
          metaInfo(metaInfo),
          keySerializer(keySerializer),
          sequenceNumberHelper(keyContext->getNumberOfKeyGroups())
    {
    }

    ~BssListStateTable()
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
            bss_adapter::ThrowWithLog<std::invalid_argument>(
                "OmniStateStore database must not be null");
        }
        this->dbPtr = _dbPtr;
        auto tblDesc = std::make_shared<ock::bss::TableDescription>(
            ock::bss::StateType::LIST, metaInfo->getName(), -1, ock::bss::TableSerializer{}, _dbPtr->GetConfig());
        dbTable = std::dynamic_pointer_cast<ock::bss::KListTable>(_dbPtr->GetTableOrCreate(tblDesc));
        bss_adapter::CheckTable(dbTable, metaInfo->getName());
    };

    std::vector<UV>* get(N& nameSpace)
    {
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        ock::bss::ListResult getResult = dbTable->Get(keyHashCode, priKey);
        std::vector<UV>* result = new std::vector<UV>();
        for (std::size_t i = 0; i < getResult.addresses.size(); ++i) {
            DataInputDeserializer serializedData(
                reinterpret_cast<const uint8_t*>(getResult.addresses.at(i)), getResult.lengths.at(i), 0);
            void* resPtr = getStateSerializer()->deserialize(serializedData);
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
    void put(N& nameSpace, const UV& state)
    {
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        TypeSerializer* vSerializer = getStateSerializer();
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
        ock::bss::BinaryData priValue(
            valueOutputSerializer.getData(), static_cast<int32_t>(valueOutputSerializer.getPosition()));
        auto res = dbTable->Put(keyHashCode, priKey, priValue);
        bss_adapter::CheckResult(res, "KListTable::Put(" + metaInfo->getName() + ")");
    }

    // append data in bss list table
    void add(N& nameSpace, const UV& value)
    {
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        UV tmpS = value;
        TypeSerializer* vSerializer = getStateSerializer();
        if constexpr (std::is_same_v<UV, int64_t> || std::is_same_v<UV, int32_t>) {
            LongSerializer::INSTANCE->serialize(&tmpS, valueOutputSerializer);
        } else if constexpr (std::is_pointer_v<UV>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }
        ock::bss::BinaryData priVal(
            valueOutputSerializer.getData(), static_cast<int32_t>(valueOutputSerializer.getPosition()));
        auto res = dbTable->Add(keyHashCode, priKey, priVal);
        bss_adapter::CheckResult(res, "KListTable::Add(" + metaInfo->getName() + ")");
    }

    void addAll(N& nameSpace, const std::vector<UV>& values)
    {
        for (const auto& item : values) {
            if constexpr (std::is_pointer_v<UV>) {
                if (item == nullptr) {
                    bss_adapter::ThrowWithLog<std::runtime_error>("You cannot add null to a value list.");
                }
            }
            add(nameSpace, item);
        }
    }

    void clear(N& nameSpace)
    {
        uint32_t keyHashCode;
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer serializer;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        auto res = dbTable->Remove(keyHashCode, priKey);
        if (!bss_adapter::IsNotFound(res)) {
            bss_adapter::CheckResult(res, "KListTable::Remove(" + metaInfo->getName() + ")");
        }
    }

    uint32_t getNextSequenceNumber(int32_t keyGroup)
    {
        return sequenceNumberHelper.getNextSequenceNumber(keyGroup);
    }

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
    {
        auto sequenceNumber = sequenceNumberHelper.getNextSequenceNumber(keyGroup);
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
        uint32_t keyHashCode = BssKeyGroupUtils::ForceKeyGroup(
            HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length())),
            static_cast<uint32_t>(keyGroup),
            static_cast<uint32_t>(keyContext->getNumberOfKeyGroups()));

        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        std::vector<uint8_t> bufferStorage(batchSize);
        auto* serializationBuffer = bufferStorage.data();
        omnistream::SerializedBatchInfo serializedBatchInfo =
            omnistream::VectorBatchSerializationUtils::serializeVectorBatch(
                vectorBatch, batchSize, serializationBuffer);
        ock::bss::BinaryData priBatchVal(serializedBatchInfo.buffer, serializedBatchInfo.size);
        auto res = dbTable->Put(keyHashCode, priKey, priBatchVal);
        bss_adapter::CheckResult(res, "KListTable::PutVectorBatch(" + metaInfo->getName() + ")");
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
            ock::bss::BinaryData priKey(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
            auto keyHashCode = BssKeyGroupUtils::ForceKeyGroup(
                HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length())),
                static_cast<uint32_t>(keyGroup),
                static_cast<uint32_t>(keyContext->getNumberOfKeyGroups()));
            auto batchSize =
                omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
            std::vector<uint8_t> bufferStorage(batchSize);
            // serializeVectorBatch 的 buffer 形参是 uint8_t*&（内部当游标前移），
            // 必须传具名左值；返回的 SerializedBatchInfo.buffer 仍指向起始位置
            auto* buffer = bufferStorage.data();
            auto serializedBatchInfo =
                omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
            ock::bss::BinaryData priBatchVal(serializedBatchInfo.buffer, serializedBatchInfo.size);
            if (dbTable->Put(keyHashCode, priKey, priBatchVal) != ock::bss::BSS_OK) {
                bss_adapter::ThrowWithLog<std::runtime_error>(
                    "Failed to add VectorBatch to BSS for keyGroup " + std::to_string(keyGroup));
            }
            sequenceNumberHelper.addNextSequenceNumber(keyGroup);
        }
    }

    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
        uint32_t keyHashCode = BssKeyGroupUtils::ForceKeyGroup(
            HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length())),
            static_cast<uint32_t>(keyGroup),
            static_cast<uint32_t>(keyContext->getNumberOfKeyGroups()));
        auto getResult = dbTable->Get(keyHashCode, priKey);
        if (getResult.addresses.empty()) {
            dbTable->CleanResource(getResult.resId);
            return nullptr;
        }
        if (getResult.lengths.empty() || getResult.lengths.at(0) <= sizeof(int8_t)) {
            dbTable->CleanResource(getResult.resId);
            bss_adapter::ThrowWithLog<std::runtime_error>(
                "OmniStateStore returned an invalid VectorBatch list payload");
        }
        auto* begin = reinterpret_cast<const uint8_t*>(getResult.addresses.at(0));
        std::vector<uint8_t> payload(begin, begin + getResult.lengths.at(0));
        dbTable->CleanResource(getResult.resId);
        auto* deserializationBuffer = payload.data() + sizeof(int8_t);
        auto batch =
            omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(deserializationBuffer);
        return batch;
    }

    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup)
    {
        bss_adapter::ThrowWithLog<std::logic_error>(
            "BssListStateTable::getVectorBatches is not implemented for this state type");
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

    ock::bss::BinaryData GetPriBinaryData(N& nameSpace, uint32_t& keyHashCode, DataOutputSerializer& serializer)
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
        keyHashCode = BssKeyGroupUtils::ForceKeyGroup(
            HashCode::Hash(serializer.getData(), static_cast<int32_t>(serializer.getPosition())),
            static_cast<uint32_t>(keyContext->getCurrentKeyGroupIndex()),
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
        ock::bss::BinaryData priKey(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
        auto keyHashCode = BssKeyGroupUtils::ForceKeyGroup(
            HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length())),
            static_cast<uint32_t>(keyGroup),
            static_cast<uint32_t>(keyContext->getNumberOfKeyGroups()));
        dbTable->Remove(keyHashCode, priKey);
    }

    InternalKeyContext<K>* keyContext;
    RegisteredKeyValueStateBackendMetaInfo* metaInfo;
    TypeSerializer* keySerializer;
    ock::bss::BoostStateDBPtr dbPtr{};
    ock::bss::ConfigRef config;
    ock::bss::KListTableRef dbTable;
    int size = 0;
    omnistream::SequenceNumberHelper sequenceNumberHelper{}; // only used for VectorBatch storage
    byte DELIMITER = (byte)',';
};

#endif // OMNISTREAM_BSSLISTSTATETABLE_H
#endif

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <tuple>
#include <memory>
#include <vector>

#include <emhash7.hpp>
#include <xxhash.h>

#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/utils/key_type_traits.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "table/data/RowData.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"

namespace omnistream {

template <typename K>
class HeapRestoreBackendDelegate;

template <typename K>
class HeapRestoreKVState : public virtual RestoreKVState {
public:
    HeapRestoreKVState(
        HeapRestoreBackendDelegate<K>& delegate, typename HeapRestoreBackendDelegate<K>::RestoreStateInfo& stateInfo)
        : delegate_(delegate),
          stateInfo_(stateInfo),
          keyGroupId_(0)
    {
    }

    void setKeyGroupId(int keyGroupId) override
    {
        keyGroupId_ = keyGroupId;
    }

    void flush() override
    {
        // Heap writes are immediate; nothing to flush.
    }

    void discard() override
    {
        // Heap writes are immediate; nothing to discard.
    }

protected:
    void writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t value) override;
    void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value) override;

    std::tuple<void*, void*> deserializeKey(const std::vector<int8_t>& keyBytes);

    void writeValueEntry(const std::vector<int8_t>& keyBytes, ByteView value);

    void writeMapEntry(const std::vector<int8_t>& keyBytes, ByteView value);
    void writeListEntry(const std::vector<int8_t>& keyBytes, ByteView value);

    struct DeserializedKeyGuard {
        void* rawKey;
        void* rawNs;
        DeserializedKeyGuard(void* k, void* n) : rawKey(k), rawNs(n)
        {
        }
        ~DeserializedKeyGuard()
        {
            if constexpr (std::is_same_v<K, Object*>) {
                // Release the ref-count acquired by GetBuffer(); the table holds its own reference after put().
                (*static_cast<Object**>(rawKey))->putRefCount();
            }
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
        }
        DeserializedKeyGuard(const DeserializedKeyGuard&) = delete;
        DeserializedKeyGuard& operator=(const DeserializedKeyGuard&) = delete;
    };

    HeapRestoreBackendDelegate<K>& delegate_;
    typename HeapRestoreBackendDelegate<K>::RestoreStateInfo& stateInfo_;
    int keyGroupId_;
};

// ============================================================================
// 实现
// ============================================================================

template <typename K>
void HeapRestoreKVState<K>::writeLongEntry(const std::vector<int8_t>& /*keyBytes*/, int64_t /*value*/)
{
    INFO_RELEASE("HeapRestoreKVState: Error: writeLongEntry is not supported for '" << stateInfo_.stateName << "'");
    throw std::runtime_error(
        "HeapRestoreKVState: writeLongEntry should not be called; use writeBytesEntry for VALUE/MAP states");
}

template <typename K>
void HeapRestoreKVState<K>::writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    if (stateInfo_.stateType == StateDescriptor::Type::MAP) {
        writeMapEntry(keyBytes, value);
    } else if (stateInfo_.stateType == StateDescriptor::Type::LIST) {
        writeListEntry(keyBytes, value);
    } else {
        writeValueEntry(keyBytes, value);
    }
}

template <typename K>
std::tuple<void*, void*> HeapRestoreKVState<K>::deserializeKey(const std::vector<int8_t>& keyBytes)
{
    DataInputDeserializer keyInput(
        reinterpret_cast<const uint8_t*>(keyBytes.data()),
        static_cast<int>(keyBytes.size()),
        delegate_.getKeyGroupPrefixBytes());

    void* rawKey = nullptr;
    if constexpr (std::is_same_v<K, Object*>) {
        auto* keyObj = delegate_.getKeySerializer()->GetBuffer();
        delegate_.getKeySerializer()->deserialize(keyObj, keyInput);
        rawKey = new Object*(keyObj);
    } else if constexpr (std::is_pointer_v<K>) {
        rawKey = new K(static_cast<K>(delegate_.getKeySerializer()->deserialize(keyInput)));
    } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
        using KeyBaseType = unwrap_shared_ptr_t<K>;
        auto* keyBuffer = static_cast<KeyBaseType*>(delegate_.getKeySerializer()->deserialize(keyInput));
        if (keyBuffer == nullptr) {
            INFO_RELEASE(
                "HeapRestoreKVState: Error: deserialized a null shared row key for '" << stateInfo_.stateName << "'");
            throw std::runtime_error("HeapRestoreKVState: deserialized a null shared row key");
        }
        rawKey = new K(std::shared_ptr<KeyBaseType>(static_cast<KeyBaseType*>(keyBuffer->copy())));
    } else if constexpr (is_shared_ptr_v<K>) {
        INFO_RELEASE("HeapRestoreKVState: Error: shared_ptr key not implemented for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: shared_ptr key not implemented");
    } else {
        rawKey = delegate_.getKeySerializer()->deserialize(keyInput);
    }

    void* rawNs = stateInfo_.namespaceSerializer->deserialize(keyInput);
    return {rawKey, rawNs};
}

template <typename K>
void HeapRestoreKVState<K>::writeValueEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    if (stateInfo_.mainStateDesc == nullptr) {
        INFO_RELEASE("HeapRestoreKVState: Error: mainStateDesc is null for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: mainStateDesc is null for '" + stateInfo_.stateName + "'");
    }

    if (stateInfo_.mainTablePtr == 0) {
        stateInfo_.mainTablePtr = delegate_.getBackend()->getStateTablePtr(stateInfo_.mainStateDesc->getName());
    }
    if (stateInfo_.mainTablePtr == 0) {
        INFO_RELEASE("HeapRestoreKVState: Error: main table not found for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: main table not found for '" + stateInfo_.stateName + "'");
    }

    auto [rawKey, rawNs] = deserializeKey(keyBytes);
    DeserializedKeyGuard keyGuard(rawKey, rawNs);

    BackendDataType valueBackendType =
        stateInfo_.valueSerializer ? stateInfo_.valueSerializer->getBackendId() : BackendDataType::BIGINT_BK;

    switch (valueBackendType) {
        case BackendDataType::BIGINT_BK: {
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            std::unique_ptr<int64_t> rawVal(static_cast<int64_t*>(stateInfo_.valueSerializer->deserialize(valInput)));
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), *rawVal);
            break;
        }
        case BackendDataType::INT_BK: {
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            std::unique_ptr<int> rawVal(static_cast<int*>(stateInfo_.valueSerializer->deserialize(valInput)));
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), *rawVal);
            break;
        }
        case BackendDataType::ROW_BK: {
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData*>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            void* rawVal = stateInfo_.valueSerializer->deserialize(valInput);
            std::unique_ptr<RowData> copied(static_cast<RowData*>(rawVal)->copy());
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), copied.release());
            break;
        }
        case BackendDataType::OBJECT_BK:
        case BackendDataType::POJO_BK:
        case BackendDataType::TUPLE_OBJ_OBJ_BK: {
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, Object*>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            Object* valObj = stateInfo_.valueSerializer->GetBuffer();
            stateInfo_.valueSerializer->deserialize(valObj, valInput);
            Object* stateVal = valObj->clone();
            valObj->putRefCount();
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), stateVal);
            stateVal->putRefCount();
            break;
        }
        case BackendDataType::SET_LONG: {
            auto* table =
                reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<long>*>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            auto* vecPtr = static_cast<std::vector<long>*>(stateInfo_.valueSerializer->deserialize(valInput));
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), vecPtr);
            break;
        }
        default:
            INFO_RELEASE(
                "HeapRestoreKVState: Error: unsupported value backend type "
                << static_cast<int>(valueBackendType) << " for VALUE state '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unsupported value backend type " +
                std::to_string(static_cast<int>(valueBackendType)) + " for VALUE state '" + stateInfo_.stateName + "'");
    }

    stateInfo_.mainEntryCount++;
}

template <typename K>
void HeapRestoreKVState<K>::writeMapEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    if (stateInfo_.mainStateDesc == nullptr) {
        INFO_RELEASE("HeapRestoreKVState: Error: mainStateDesc is null for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: mainStateDesc is null for '" + stateInfo_.stateName + "'");
    }

    if (stateInfo_.mainTablePtr == 0) {
        stateInfo_.mainTablePtr = delegate_.getBackend()->getStateTablePtr(stateInfo_.mainStateDesc->getName());
    }
    if (stateInfo_.mainTablePtr == 0) {
        INFO_RELEASE("HeapRestoreKVState: Error: main table not found for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: main table not found for '" + stateInfo_.stateName + "'");
    }

    auto [rawKey, rawNs] = deserializeKey(keyBytes);
    DeserializedKeyGuard keyGuard(rawKey, rawNs);

    auto* mapKeySer = stateInfo_.mapKeySerializer;
    auto* mapValSer = stateInfo_.mapValueSerializer;
    if (mapKeySer == nullptr || mapValSer == nullptr) {
        INFO_RELEASE(
            "HeapRestoreKVState: Error: MAP state missing key/value serializers for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: MAP state missing key/value serializers");
    }

    BackendDataType mapKeyId = mapKeySer->getBackendId();
    BackendDataType mapValId = mapValSer->getBackendId();

    DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));

    if (mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT64) {
        using UK = XXH128_hash_t;
        using UV = std::tuple<int32_t, int64_t>;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(valInput)));
        UK uk = *ukPtr;
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT32_INT64) {
        using UK = XXH128_hash_t;
        using UV = std::tuple<int32_t, int32_t, int64_t>;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(valInput)));
        UK uk = *ukPtr;
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::INT_BK && mapValId == BackendDataType::INT_BK) {
        using UK = int;
        using UV = int;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(valInput)));
        UK uk = *ukPtr;
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::BIGINT_BK && mapValId == BackendDataType::BIGINT_BK) {
        using UK = int64_t;
        using UV = int64_t;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(valInput)));
        UK uk = *ukPtr;
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::VARCHAR_BK && mapValId == BackendDataType::INT_BK) {
        using UK = std::string;
        using UV = int;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        auto* rawUk = static_cast<std::string*>(mapKeySer->deserialize(valInput));
        UK uk = *rawUk;
        delete rawUk;
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[std::move(uk)] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[std::move(uk)] = uv;
        }
    } else if (
        (mapKeyId == BackendDataType::OBJECT_BK || mapKeyId == BackendDataType::POJO_BK) &&
        (mapValId == BackendDataType::OBJECT_BK || mapValId == BackendDataType::POJO_BK)) {
        using UK = Object*;
        using UV = Object*;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        Object* keyObj = mapKeySer->GetBuffer();
        mapKeySer->deserialize(keyObj, valInput);
        UK uk = keyObj->clone();
        keyObj->putRefCount();

        Object* valObj = mapValSer->GetBuffer();
        mapValSer->deserialize(valObj, valInput);
        UV uv = valObj->clone();
        valObj->putRefCount();

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
            uk->putRefCount();
            uv->putRefCount();
        } else {
            (*kvMap)[uk] = uv;
            uk->putRefCount();
            uv->putRefCount();
        }
    } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::INT_BK) {
        using UK = RowData*;
        using UV = int32_t;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        void* rawUk = mapKeySer->deserialize(valInput);
        UK uk = static_cast<RowData*>(rawUk)->copy();
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_BK) {
        using UK = RowData*;
        using UV = RowData*;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        void* rawUk = mapKeySer->deserialize(valInput);
        UK uk = static_cast<RowData*>(rawUk)->copy();
        void* rawUv = mapValSer->deserialize(valInput);
        UV uv = static_cast<RowData*>(rawUv)->copy();

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::TIME_WINDOW_BK && mapValId == BackendDataType::TIME_WINDOW_BK) {
        using UK = TimeWindow;
        using UV = TimeWindow;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(valInput)));
        UK uk = *ukPtr;
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_LIST_BK) {
        using UK = RowData*;
        using UV = std::vector<RowData*>*;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        void* rawUk = mapKeySer->deserialize(valInput);
        UK uk = static_cast<RowData*>(rawUk)->copy();
        int listSize = valInput.readInt();
        auto* vec = new std::vector<RowData*>();
        vec->reserve(listSize);
        for (int i = 0; i < listSize; i++) {
            void* rawElem = mapValSer->deserialize(valInput);
            vec->push_back(static_cast<RowData*>(rawElem)->copy());
        }

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = vec;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = vec;
        }
    } else {
        INFO_RELEASE(
            "HeapRestoreKVState: Error: unsupported MAP type combination (keyId="
            << static_cast<int>(mapKeyId) << ", valId=" << static_cast<int>(mapValId) << ") for state '"
            << stateInfo_.stateName << "'");
        throw std::runtime_error(
            "HeapRestoreKVState: unsupported MAP type combination (keyId=" +
            std::to_string(static_cast<int>(mapKeyId)) + ", valId=" + std::to_string(static_cast<int>(mapValId)) +
            ") for state '" + stateInfo_.stateName + "'");
    }

    stateInfo_.mainEntryCount++;
}

template <typename K>
void HeapRestoreKVState<K>::writeListEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    if (stateInfo_.mainStateDesc == nullptr) {
        INFO_RELEASE("HeapRestoreKVState: Error: mainStateDesc is null for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: mainStateDesc is null for '" + stateInfo_.stateName + "'");
    }

    if (stateInfo_.mainTablePtr == 0) {
        stateInfo_.mainTablePtr = delegate_.getBackend()->getStateTablePtr(stateInfo_.mainStateDesc->getName());
    }
    if (stateInfo_.mainTablePtr == 0) {
        INFO_RELEASE("HeapRestoreKVState: Error: main table not found for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: main table not found for '" + stateInfo_.stateName + "'");
    }

    auto [rawKey, rawNs] = deserializeKey(keyBytes);
    DeserializedKeyGuard keyGuard(rawKey, rawNs);

    auto* listSer = dynamic_cast<ListSerializer*>(stateInfo_.valueSerializer);
    TypeSerializer* elemSer = listSer ? listSer->getElementSerializer() : nullptr;
    if (elemSer == nullptr) {
        INFO_RELEASE(
            "HeapRestoreKVState: Error: LIST state serializer is not ListSerializer for '" << stateInfo_.stateName
                                                                                           << "'");
        throw std::runtime_error(
            "HeapRestoreKVState: LIST state serializer is not ListSerializer for '" + stateInfo_.stateName + "'");
    }

    BackendDataType elemId = elemSer->getBackendId();
    DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));

    if (elemId == BackendDataType::BIGINT_BK) {
        int size = valInput.readInt();
        std::unique_ptr<std::vector<int64_t>> vec(new std::vector<int64_t>());
        vec->reserve(size);
        for (int i = 0; i < size; i++) {
            std::unique_ptr<int64_t> elemPtr(static_cast<int64_t*>(elemSer->deserialize(valInput)));
            vec->push_back(*elemPtr);
        }

        auto* table =
            reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<int64_t>*>*>(stateInfo_.mainTablePtr);
        table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), vec.release());
    } else {
        INFO_RELEASE(
            "HeapRestoreKVState: Error: unsupported LIST element type " << static_cast<int>(elemId) << " for state '"
                                                                        << stateInfo_.stateName << "'");
        throw std::runtime_error(
            "HeapRestoreKVState: unsupported LIST element type " + std::to_string(static_cast<int>(elemId)) +
            " for state '" + stateInfo_.stateName + "'");
    }

    stateInfo_.mainEntryCount++;
}

} // namespace omnistream

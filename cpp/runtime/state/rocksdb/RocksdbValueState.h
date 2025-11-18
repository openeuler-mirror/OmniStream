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

#ifndef OMNISTREAM_ROCKSDBVALUESTATE_H
#define OMNISTREAM_ROCKSDBVALUESTATE_H

#include "core/typeutils/TypeSerializer.h"
#include "../internal/InternalKvState.h"
#include "core/api/common/state/ValueState.h"
#include "../AbstractKeyedStateBackend.h"
#include "RocksdbStateTable.h"

#include "rocksdb/db.h"
#include "state/RocksDbKvStateInfo.h"

template <typename K, typename N, typename V>
class RocksdbValueState : public ValueState<V>, public InternalKvState<K, N, V> {
public:
    TypeSerializer *getKeySerializer()
    {
        return keySerializer;
    };
    TypeSerializer *getNamespaceSerializer()
    {
        return namespaceSerializer;
    };
    TypeSerializer *getValueSerializer()
    {
        return valueSerializer;
    };
    void setNamespaceSerializer(TypeSerializer *serializer)
    {
        namespaceSerializer = serializer;
    };
    void setValueSerializer(TypeSerializer *serializer)
    {
        valueSerializer = serializer;
    };
    void setCurrentNamespace(N nameSpace) override
    {
        currentNamespace = nameSpace;
    };
    V value() override;
    void update(const V &value, bool copyKey = false) override;

    void setDefaultValue(V value)
    {
        defaultValue = value;
    };

    static RocksdbValueState<K, N, V> *create(
            StateDescriptor *stateDesc, RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer);

    static RocksdbValueState<K, N, V> *update(
            StateDescriptor *stateDesc, RocksdbStateTable<K, N, V> *stateTable,
            RocksdbValueState<K, N, V> *existingState);

    RocksdbValueState(RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer,
                      TypeSerializer *valueSerializer,
                      TypeSerializer *namespaceSerializer, V defaultValue);

    ~RocksdbValueState() = default;

    void createTable(ROCKSDB_NAMESPACE::DB* db, std::string cfName,
                    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation);

    void clearVectors(int64_t currentTimestamp) {}
    void clear() override;
    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override;
    omnistream::VectorBatch *getVectorBatch(int batchId) override;
    long getVectorBatchesSize() override;

private:
    RocksdbStateTable<K, N, V> *stateTable;
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    V defaultValue;
    N currentNamespace;
};

template <typename K, typename N, typename V>
RocksdbValueState<K, N, V> *RocksdbValueState<K, N, V>::create(StateDescriptor *stateDesc,
    RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer)
{
    return new RocksdbValueState<K, N, V>(
            stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer(), V());
}

template<typename K, typename N, typename V>
RocksdbValueState<K, N, V>::RocksdbValueState(RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer,
                                              TypeSerializer *valueSerializer, TypeSerializer *namespaceSerializer,
                                              V defaultValue)
    : stateTable(stateTable), keySerializer(keySerializer), valueSerializer(valueSerializer),
    namespaceSerializer(namespaceSerializer), defaultValue(defaultValue) {}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::createTable(ROCKSDB_NAMESPACE::DB* db, std::string cfName,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation)
{
    stateTable->createTable(db, cfName, kvStateInformation);
}

template <typename K, typename N, typename V>
V RocksdbValueState<K, N, V>::value()
{
    auto result = stateTable->get(currentNamespace);
    // For BinaryRowData, the underlying implementation uses a shared BinaryRowData
    // instance to deserialize data from RocksDB. Therefore, we need to copy it
    // to avoid it being overwritten or deleted by the next get operation.
    if constexpr (std::is_pointer<V>::value)
    {
        if (result == nullptr) {
            return defaultValue;
        }
        auto *br = dynamic_cast<BinaryRowData*>(result);
        if (br == nullptr) {
            return result;
        }
        auto *copied = br->copy();
        if constexpr (std::is_convertible_v<decltype(copied), V>) {
            return static_cast<V>(copied);
        } else {
            return result;
        }
    } else {
        return result == std::numeric_limits<V>::max() ? defaultValue : result;
    }
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::update(const V &value, bool copyKey)
{
    if constexpr (std::is_pointer_v<V>) {
        if (value == nullptr) {
            clear();
            return;
        }
    }

    // V进来序列化一下，key也要序列化一下
    // copy key
    stateTable->put(currentNamespace, value);
}

template <typename K, typename N, typename V>
RocksdbValueState<K, N, V> *RocksdbValueState<K, N, V>::update(StateDescriptor *stateDesc,
    RocksdbStateTable<K, N, V> *stateTable, RocksdbValueState<K, N, V> *existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::addVectorBatch(omnistream::VectorBatch *vectorBatch)
{
    stateTable->addVectorBatch(vectorBatch);
};

template <typename K, typename N, typename V>
omnistream::VectorBatch *RocksdbValueState<K, N, V>::getVectorBatch(int batchId)
{
    return stateTable->getVectorBatch(batchId);
};

template <typename K, typename N, typename V>
long RocksdbValueState<K, N, V>::getVectorBatchesSize()
{
    return stateTable->getVectorBatchesSize();
};

template<typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::clear()
{
    stateTable->clear(currentNamespace);
}

#endif // OMNISTREAM_ROCKSDBVALUESTATE_H

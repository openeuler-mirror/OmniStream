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

#ifndef OMNISTREAM_BSSVALUESTATE_H
#define OMNISTREAM_BSSVALUESTATE_H
#ifdef WITH_OMNISTATESTORE

#include "runtime/state/internal/InternalKvState.h"
#include "api/common/state/ValueState.h"
#include "BssStateTable.h"
#include "api/common/state/StateDescriptor.h"

template <typename K, typename N, typename V>
class BssValueState : public ValueState<V>, public InternalKvState<K, N, V> {
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
    V value() override
    {
        auto result = stateTable->get(currentNamespace);
        if constexpr (std::is_pointer<V>::value) {
            return result == nullptr ? defaultValue : result;
        } else {
            return result == std::numeric_limits<V>::max() ? defaultValue : result;
        }
    };
    void update(const V &value, bool copyKey = false) override
    {
        stateTable->put(currentNamespace, value);
    };

    void setDefaultValue(V value)
    {
        defaultValue = value;
    };
    static BssValueState<K, N, V> *create(
            StateDescriptor *stateDesc, BssStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer)
    {
        return new BssValueState<K, N, V>(stateTable, keySerializer, stateTable->getStateSerializer(),
            stateTable->getNamespaceSerializer(), V());
    };

    static BssValueState<K, N, V> *updateState(StateDescriptor *stateDesc, BssStateTable<K, N, V> *stateTable,
        BssValueState<K, N, V> *existingState)
    {
        existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
        existingState->setValueSerializer(stateTable->getStateSerializer());
        return existingState;
    }

    BssValueState(BssStateTable<K, N, V> *stateTable,
                  TypeSerializer *keySerializer,
                  TypeSerializer *valueSerializer,
                  TypeSerializer *namespaceSerializer,
                  V defaultValue): stateTable(stateTable), keySerializer(keySerializer),
        valueSerializer(valueSerializer), namespaceSerializer(namespaceSerializer), defaultValue(defaultValue) {};

    ~BssValueState()
    {
        delete stateTable;
    };

    void CreateTable(ock::bss::BoostStateDBPtr &_dbPtr)
    {
        stateTable->createTable(_dbPtr);
    }

    void clearVectors(int64_t currentTimestamp) {}

    void clear() override
    {
        stateTable->clear(currentNamespace);
    };

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override
    {
        stateTable->addVectorBatch(vectorBatch);
    };

    omnistream::VectorBatch *getVectorBatch(int batchId) override
    {
        return stateTable->getVectorBatch(batchId);
    };

    long getVectorBatchesSize() override
    {
        return stateTable->getVectorBatchesSize();
    };

private:
    BssStateTable<K, N, V>* stateTable;
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    V defaultValue;
    N currentNamespace;
};

#endif // OMNISTREAM_BSSVALUESTATE_H
#endif

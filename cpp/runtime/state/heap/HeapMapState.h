/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_HEAPMAPSTATE_H
#define FLINK_TNEL_HEAPMAPSTATE_H

#include <emhash7.hpp>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/MapState.h"
#include "../VoidNamespace.h"
#include "core/api/common/state/StateDescriptor.h"
#include "StateTable.h"
#include "table/data/binary/BinaryRowData.h"
#include "vectorbatch/VectorBatch.h"

// The state is a map. in the InternalKvState, the state is stored as a pointer to emhash7
template<typename K, typename N, typename UK, typename UV>
class HeapMapState : public MapState<UK, UV>, public InternalKvState<K, N, emhash7::HashMap<UK, UV> *> {
public:
    HeapMapState(StateTable<K, N, emhash7::HashMap<UK, UV>* > *stateTable, TypeSerializer *keySerializer,
                 TypeSerializer *valueSerializer, TypeSerializer *namespaceSerializer);

    ~HeapMapState() {
        stateTable->deleteMaps();
    };
    
    TypeSerializer *getKeySerializer() const { return keySerializer; };

    TypeSerializer *getNamespaceSerializer() const { return namespaceSerializer; };

    TypeSerializer *getValueSerializer() const { return valueSerializer; };

    void setNamespaceSerializer(TypeSerializer *serializer) { namespaceSerializer = serializer; };

    void setValueSerializer(TypeSerializer *serializer) { valueSerializer = serializer; };

    std::optional<UV> get(const UK &userKey) override;

    void put(const UK &userKey, const UV &userValue) override;

    void updateOrCreate(const UK &key, UV defaultValue,
                        std::function<std::optional<UV>(UV &)> transformFunc) override;

    void remove(const UK &userKey) override;

    bool contains(const UK &userKey) override;

    void update(const UK &key, const UV &value) override;

    // void updateOrCreate(const UK &key, const UV defaultValue, typename HeapMapState::ValueTransformFuncPtr transformFunc) override;

    void setCurrentNamespace(N nameSpace) override;

    void clear() override { stateTable->remove(currentNamespace); };

    static HeapMapState<K, N, UK, UV> *
    create(StateDescriptor *stateDesc, StateTable<K, N, emhash7::HashMap<UK, UV>* > *stateTable,
           TypeSerializer *keySerializer);

    static HeapMapState<K, N, UK, UV> *
    update(StateDescriptor *stateDesc, StateTable<K, N, emhash7::HashMap<UK, UV>* > *stateTable,
           HeapMapState<K, N, UK, UV> *existingState);

    // This gets the pointer to the actual map (a value for state), like Join's emhash<RowData*, int> with currentNamespace and currentKey
    typename emhash7::HashMap<UK, UV>* entries() override;

    void addVectorBatch(omnistream::VectorBatch* vectorBatch);

    const std::vector<omnistream::VectorBatch*> &getVectorBatches() const;
private:
    StateTable<K, N, emhash7::HashMap<UK, UV>* > *stateTable;
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    N currentNamespace;
};

template<typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::updateOrCreate(const UK &key, UV defaultValue,
                                                std::function<std::optional<UV>(UV &)> transformFunc)
{
    emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        // First-time initialization for namespace
        userMap = new emhash7::HashMap<UK, UV>(1);
        stateTable->put(currentNamespace, userMap);
        LOG_PRINTF("created userMap at %p\n", userMap);
    }
    userMap->updateOrCreate(key, defaultValue, transformFunc);
}

template<typename K, typename N, typename UK, typename UV>
emhash7::HashMap<UK, UV> *HeapMapState<K, N, UK, UV>::entries() {
    LOG_PRINTF("----HeapMapState::entries\\n\\t\\t this=%p, stateTable=%p\n", this, stateTable);
    typename emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    LOG_PRINTF("----HeapMapState::entries\\n\\t\\t this=%p foundUserMap=%p\n", this, userMap);
    return userMap;
}

template<typename K, typename N, typename UK, typename UV>
std::optional<UV> HeapMapState<K, N, UK, UV>::get(const UK &userKey) {
    if (stateTable == nullptr) {
        throw std::runtime_error("StateTable is not initialized.");
    }

    emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        return std::nullopt;
    }
    auto it = userMap->find(userKey);
    if (it == userMap->end()) {
        return std::nullopt;
    }
    return (it->second);
}

template<typename K, typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV>::HeapMapState(StateTable<K, N, emhash7::HashMap<UK, UV>* > *stateTable,
                                         TypeSerializer *keySerializer, TypeSerializer *valueSerializer,
                                         TypeSerializer *namespaceSerializer) {
    this->keySerializer = keySerializer;
    this->namespaceSerializer = namespaceSerializer;
    this->valueSerializer = valueSerializer;
    this->stateTable = stateTable;
    currentNamespace = N();
}

template<typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::put(const UK &userKey, const UV &userValue) {
    LOG_PRINTF("HeapMapState::put\\n\\t\\t this=%p, stateTable=%p\n", this, stateTable);

    /*
    if constexpr(std::is_same_v<UK, RowData*>) {
        if (auto temp = dynamic_cast<RowData *>(userKey); temp != nullptr) {
            std::cout << "userKey=";
            temp->printRow();
        }
    }
    */
    emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    LOG_PRINTF("stateTable %p found userMap? %p\n", stateTable, userMap);
    if (userMap == nullptr) {
        userMap = new emhash7::HashMap<UK, UV>();
        LOG_PRINTF("created userMap? %p\n", userMap);

        (*userMap)[userKey] = userValue;
        // Save this Map to stateTable
        stateTable->put(currentNamespace, userMap);
        LOG_PRINTF("userMap? %p put to stateTable %p\n", userMap, stateTable);
    } else {
        (*userMap)[userKey] = userValue;
    }
}

template<typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::remove(const UK &userKey) {
    emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        return;
    }

    userMap->erase(userKey);

    if (userMap->empty()) {
        clear();
    }
}

template<typename K, typename N, typename UK, typename UV>
bool HeapMapState<K, N, UK, UV>::contains(const UK &userKey) {
    emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    return userMap != nullptr && (userMap->find(userKey) != userMap->end());
}

template <typename K, typename N, typename UK, typename UV>
inline void HeapMapState<K, N, UK, UV>::update(const UK &userKey, const UV &userValue)
{
    emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    auto it = userMap->find(userKey);
    it->second = userValue;
}

/*
template<typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::updateOrCreate(const UK &key, const UV defaultValue,
                                                typename HeapMapState::ValueTransformFuncPtr transformFunc) {
    emhash7::HashMap<UK, UV> *userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        // This is a new key, need to copy it for statebackend
        stateTable->copyCurrentKey();
        userMap = new emhash7::HashMap<UK, UV>(1);
        stateTable->put(currentNamespace, userMap);
        LOG_PRINTF("created userMap? %p\n", userMap);
        //TODO: make it general here?
        // Only copy when it is a new RowData* as key
    }
    if constexpr(std::is_same_v<UV, int>) {
        userMap->updateOrCreate(key, defaultValue);
    } else {
        NOT_IMPL_EXCEPTION
    }
}
 */
template<typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::setCurrentNamespace(N nameSpace) {
    currentNamespace = nameSpace;
}

template<typename K, typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV> *
HeapMapState<K, N, UK, UV>::create(StateDescriptor *stateDesc, StateTable<K, N, emhash7::HashMap<UK, UV>* > *stateTable,
                                   TypeSerializer *keySerializer) {
    return new HeapMapState<K, N, UK, UV>(stateTable,
                                          keySerializer,
                                          stateTable->getStateSerializer(),
                                          stateTable->getNamespaceSerializer());
}

template<typename K, typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV> *
HeapMapState<K, N, UK, UV>::update(StateDescriptor *stateDesc, StateTable<K, N, emhash7::HashMap<UK, UV>* > *stateTable,
                                   HeapMapState<K, N, UK, UV> *existingState) {
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template<typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::addVectorBatch(omnistream::VectorBatch *vectorBatch) {
    this->vectorBatches.push_back(vectorBatch);
}

template<typename K, typename N, typename UK, typename UV>
const std::vector<omnistream::VectorBatch*> &HeapMapState<K, N, UK, UV>::getVectorBatches() const {
    return this->vectorBatches;
}

#endif // FLINK_TNEL_HEAPMAPSTATE_H

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STATETABLE_H
#define FLINK_TNEL_STATETABLE_H

#include <vector>
#include <type_traits>
#include <tuple>
#include <functional> // for std::hash
#include "StateMap.h"
#include "core/typeutils/TypeSerializer.h"
#include "../StateTransformationFunction.h"
#include "../internal/InternalKvState.h"
#include "../InternalKeyContext.h"
#include "../KeyGroupRange.h"
#include "../RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/binary/BinaryRowData.h"
/* S is the value used in the State,
 * like RowData* for HeapValueState,
 * emhash7<RowData*, int>* for HeapMapState,
 * vector<int64_t>* for List State
 */
template<typename K, typename N, typename S>
class StateTable {
public:
    StateTable(InternalKeyContext<K> *keyContext, RegisteredKeyValueStateBackendMetaInfo *metaInfo,
               TypeSerializer *keySerializer);

    virtual ~StateTable();

    bool isEmpty() {
        return size() == 0;
    };

    int size();

    S get(const N &nameSpace) {
        return get(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    };

    bool containsKey(const N &nameSpace) {
        return containsKey(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    };

    void put(const N &nameSpace, const S &state) {
        put(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace, state);
    };

    void put(const K &key, int keyGroup, const N &nameSpace, const S &state) {
        getMapForKeyGroup(keyGroup)->put(key, nameSpace, state);
    }

    K putAndRemoveDuplicateKey(const N &nameSpace, const S &state) {
        return getMapForKeyGroup(keyContext->getCurrentKeyGroupIndex())->putAndRemoveDuplicateKey(
                keyContext->getCurrentKey(), nameSpace, state);
    }

    void remove(const N &nameSpace) {
        remove(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    };

    S removeAndGetOld(const N &nameSpace) {
        return removeAndGetOld(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    };

    template<typename T>
    void transform(const N &nameSpace, T value, StateTransformationFunction<S, T> transformation);

    S get(const K &key, const N &nameSpace);

    typename InternalKvState<K, N, S>::StateIncrementalVisitor *
    getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords);

    RegisteredKeyValueStateBackendMetaInfo *getMetaInfo() {
        return metaInfo;
    }

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo *newMetaInfo) {
        metaInfo = newMetaInfo;
    }

    void copyCurrentKey() {
        if constexpr (std::is_same_v<K, RowData *>) {
            auto newKey = static_cast<BinaryRowData *>(keyContext->getCurrentKey())->copy();
            keyContext->setCurrentKey(newKey);
        } else if constexpr (std::is_pointer_v<K>) {
            NOT_IMPL_EXCEPTION;
        } else {
            return;
        }
    }

    // Not implemented
    std::vector<K> *getKeys(const N &nameSpace);

    std::vector<std::tuple<K, N>> *getKeysAndNamespace();

    // Access to maps
    std::vector<StateMap<K, N, S> *> *getState() {
        return &keyGroupedStateMaps;
    }

    int getKeyGroupOffset() {
        return keyGroupRange->getStartKeyGroup();
    }

    StateMap<K, N, S> *getMapForKeyGroup(int keyGroupIndex);

    TypeSerializer *getKeySerializer() {
        return keySerializer;
    }

    TypeSerializer *getStateSerializer() {
        return metaInfo->getStateSerializer();
    }

    TypeSerializer *getNamespaceSerializer() {
        return metaInfo->getNamespaceSerializer();
    }

    void deleteMaps()
    {
        for (int index = keyGroupRange->getStartKeyGroup(); index <= keyGroupRange->getEndKeyGroup(); index++)
        {
            delete keyGroupedStateMaps[index];
            keyGroupedStateMaps[index] = nullptr;
        }
    };

    class StateEntryIterator : public InternalKvState<K, N, S>::StateIncrementalVisitor {
    public:
        S nextEntries() override;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords, StateTable<K, N, S> *table);

        bool hasNext() override;

        // void remove(const K &key, const N &nameSpace, const S &state) override;

        // void update(const K &key, const N &nameSpace, const S &state, const S &newState) override;
    private:
        int recommendedMaxNumberOfReturnedRecords;
        int keyGroupIndex;
        typename InternalKvState<K, N, S>::StateIncrementalVisitor* stateIncrementalVisitor;
        StateTable<K, N, S> *table;

        void init();
    };
protected:
    // Variables
    InternalKeyContext<K> *keyContext;
    TypeSerializer *keySerializer;
    KeyGroupRange *keyGroupRange;
    std::vector<StateMap<K, N, S> *> keyGroupedStateMaps;
    RegisteredKeyValueStateBackendMetaInfo *metaInfo;

    // Abstract Functions
    virtual StateMap<K, N, S> *createStateMap() = 0;

    // Internal interactions with cowMap
    S get(const K &key, int keyGroupIndex, const N &nameSpace) {
        return getMapForKeyGroup(keyGroupIndex)->get(key, nameSpace);
    };

    bool containsKey(const K &key, int keyGroupIndex, const N &nameSpace) {
        return getMapForKeyGroup(keyGroupIndex)->containsKey(key, nameSpace);
    };

    void remove(const K &key, int keyGroupIndex, const N &nameSpace) {
        getMapForKeyGroup(keyGroupIndex)->remove(key, nameSpace);
    };

    S removeAndGetOld(const K &key, int keyGroupIndex, const N &nameSpace) {
        getMapForKeyGroup(keyGroupIndex)->removeAndGetOld(key, nameSpace);
    };

    // Access to maps
    int indexToOffset(int index) {
        return index - getKeyGroupOffset();
    };

    virtual void initialize() = 0;

};

template<typename K, typename N, typename S>
StateTable<K, N, S>::StateTable(InternalKeyContext<K> *keyContext, RegisteredKeyValueStateBackendMetaInfo *metaInfo,
                                TypeSerializer *keySerializer) {
    this->keyContext = keyContext;
    this->metaInfo = metaInfo;
    this->keySerializer = keySerializer;
    this->keyGroupRange = keyContext->getKeyGroupRange();
    keyGroupedStateMaps = {};
}

template<typename K, typename N, typename S>
StateTable<K, N, S>::~StateTable() {
    for (auto stateMapPtr: keyGroupedStateMaps) {
        delete stateMapPtr;
    }
}

template<typename K, typename N, typename S>
int StateTable<K, N, S>::size() {
    int count = 0;
    for (int i = 0; i < keyGroupedStateMaps.size(); i++) {
        count += keyGroupedStateMaps[i]->size();
    }
    return count;
}

template<typename K, typename N, typename S>
S StateTable<K, N, S>::get(const K &key, const N &nameSpace) {
    // Key class must have a hash function
    std::hash<K> keyHash;
    int keyGroup = keyHash(key) % keyContext->getNumberOfKeyGroups();
    return get(key, keyGroup, nameSpace);
}

template<typename K, typename N, typename S>
std::vector<K> *StateTable<K, N, S>::getKeys(const N &nameSpace) {
    return nullptr;
}

template<typename K, typename N, typename S>
std::vector<std::tuple<K, N>> *StateTable<K, N, S>::getKeysAndNamespace() {
    return nullptr;
}

template<typename K, typename N, typename S>
typename InternalKvState<K, N, S>::StateIncrementalVisitor *
StateTable<K, N, S>::getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
    return new typename StateTable<K, N, S>::StateEntryIterator(recommendedMaxNumberOfReturnedRecords, this);
}

template<typename K, typename N, typename S>
StateMap<K, N, S> *StateTable<K, N, S>::getMapForKeyGroup(int keyGroupIndex) {
    const int pos = indexToOffset(keyGroupIndex);
    if (pos >= 0 && pos < keyContext->getKeyGroupRange()->getNumberOfKeyGroups()) {
        return keyGroupedStateMaps[pos];
    } else {
        throw std::logic_error("Key group is not in key group range");
    }
}

template<typename K, typename N, typename S>
StateTable<K, N, S>::StateEntryIterator::StateEntryIterator(int recommendedMaxNumberOfReturnedRecords,
                                                            StateTable<K, N, S> *table) {
    this->table = table;
    this->recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
    this->keyGroupIndex = 0;
    init();
}

// This function move the stateIncremental visitor to the first keyGroup that has value in it.
template<typename K, typename N, typename S>
void StateTable<K, N, S>::StateEntryIterator::init() {
    while (keyGroupIndex < table->keyContext->getKeyGroupRange()->getNumberOfKeyGroups()) {
        StateMap<K, N, S> *stateMap = table->keyGroupedStateMaps[keyGroupIndex++];
        auto visitor = stateMap->getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
        if (visitor->hasNext()) {
            stateIncrementalVisitor = visitor;
            return;
        }
    }
}
// This function checks if next keyGroup exist and go to it
template<typename K, typename N, typename S>
bool StateTable<K, N, S>::StateEntryIterator::hasNext() {
    while (stateIncrementalVisitor == nullptr || !stateIncrementalVisitor->hasNext()) {
        if (keyGroupIndex == table->keyContext->getKeyGroupRange()->getNumberOfKeyGroups()) {
            return false;
        }
        auto visitor = table->keyGroupedStateMaps[keyGroupIndex++]->getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
        if (visitor->hasNext()) {
            stateIncrementalVisitor = visitor;
            break;
        }
    }
    return true;
}

template<typename K, typename N, typename S>
S StateTable<K, N, S>::StateEntryIterator::nextEntries() {
    if (!hasNext()) {
        if constexpr (std::is_pointer_v<S>) {
            return nullptr;
        } else {
            return std::numeric_limits<S>::max();
        }
    }
    return stateIncrementalVisitor->nextEntries();
}
/*
    template<typename K, typename N, typename S>
    void StateTable<K, N, S>::StateEntryIterator::remove(const K &key, const N &nameSpace, const S &state) {
        StateMap<K, N, S> *current = table->keyGroupedStateMaps[keyGroupIndex - 1];
        current->remove(key, nameSpace);
    }

    template<typename K, typename N, typename S>
    void StateTable<K, N, S>::StateEntryIterator::update(const K &key, const N &nameSpace, const S &state,
                                                         const S &newState) {
        StateMap<K, N, S> *current = table->keyGroupedStateMaps[keyGroupIndex - 1];
        current->put(key, nameSpace, newState);
    }
*/
template<typename K, typename N, typename S>
template<typename T>
void StateTable<K, N, S>::transform(const N &nameSpace, T value, StateTransformationFunction<S, T> transformation) {
    K key = keyContext->getCurrentKey();
    int keyGroup = keyContext->getCurrentKeyGroupIndex();
    getMapForKeyGroup(keyGroup).transform(key, nameSpace, value, transformation);
}

#endif // FLINK_TNEL_STATETABLE_H

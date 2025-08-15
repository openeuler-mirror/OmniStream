/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_COPYONWRITESTATETABLE_H
#define FLINK_TNEL_COPYONWRITESTATETABLE_H

#include "StateTable.h"
#include "CopyOnWriteStateMap.h"

// No snapshotting features implemented
template <typename K, typename N, typename S>
class CopyOnWriteStateTable : public StateTable<K, N, S>
{
public:
    CopyOnWriteStateTable(InternalKeyContext<K> *keyContext, RegisteredKeyValueStateBackendMetaInfo *metaInfo, TypeSerializer *keySerializer);

    virtual ~CopyOnWriteStateTable();

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo *metaInfo);
    int computeKeyGroupForKeyHash(K key);
protected:
    StateMap<K, N, S> *createStateMap() override;
    void initialize() override;
};

template <typename K, typename N, typename S>
inline StateMap<K, N, S> *CopyOnWriteStateTable<K, N, S>::createStateMap()
{
    return new omnistream::CopyOnWriteStateMap<K, N, S>(this->keySerializer);
}

template <typename K, typename N, typename S>
inline CopyOnWriteStateTable<K, N, S>::CopyOnWriteStateTable(InternalKeyContext<K> *keyContext, RegisteredKeyValueStateBackendMetaInfo *metaInfo, TypeSerializer *keySerializer) : StateTable<K, N, S>(keyContext, metaInfo, keySerializer)
{
    initialize();
}

template <typename K, typename N, typename S>
CopyOnWriteStateTable<K, N, S>::~CopyOnWriteStateTable() {
}

template <typename K, typename N, typename S>
inline void CopyOnWriteStateTable<K, N, S>::initialize()
{
    for (int i = 0; i < this->keyGroupRange->getNumberOfKeyGroups(); i++)
    {
        this->keyGroupedStateMaps.push_back(createStateMap());
    }
}

template <typename K, typename N, typename S>
inline void CopyOnWriteStateTable<K, N, S>::setMetaInfo(RegisteredKeyValueStateBackendMetaInfo *metaInfo)
{
}

template <typename K, typename N, typename S>
inline int CopyOnWriteStateTable<K, N, S>::computeKeyGroupForKeyHash(K key)
{
    std::hash<K> keyHash;
    int group = keyHash(key) % StateTable<K, N, S>::keyContext->getNumberOfKeyGroups();
    return group;
}

#endif // FLINK_TNEL_COPYONWRITESTATETABLE_H

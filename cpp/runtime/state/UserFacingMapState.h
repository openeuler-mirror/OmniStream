/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_USERFACINGMAPSTATE_H
#define FLINK_TNEL_USERFACINGMAPSTATE_H
#include "core/api/MapState.h"

template <typename K, typename V>
class UserFacingMapState : public MapState<K, V>
{
public:
    void clear() override {};
    UserFacingMapState(MapState<K, V> *originalState) : originalState(originalState) {};
    std::optional<V> get(const K &key) override { return originalState->get(key); };
    void remove(const K &key) override {};
    void put(const K &key, const V &value) override { originalState->put(key, value); };
    bool contains(const K &key) override { return originalState->contains(key); };

    typename emhash7::HashMap<K, V>::iterator *iterator() override { return originalState->iterator(); };
private:
    MapState<K, V> *originalState;
};

#endif // FLINK_TNEL_USERFACINGMAPSTATE_H

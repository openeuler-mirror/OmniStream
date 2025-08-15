/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_MAPSTATE_H
#define FLINK_TNEL_MAPSTATE_H
#include <optional>
#include "common/state/State.h"
#include "emhash7.hpp"

template <typename UK, typename UV>
class MapState : virtual public State
{
public:
    using ValueTransformFuncPtr = UV (*) (UV& oldValue);
    virtual ~MapState() = default;
    // virtual void updateOrCreate(const UK &key, const UV defaultValue, MapState<UK, UV>::ValueTransformFuncPtr transformFunc) = 0;
    virtual std::optional<UV> get(const UK &key) = 0;
    virtual void put(const UK &key, const UV &value) = 0;
    virtual void updateOrCreate(const UK &key, UV defaultValue,
            std::function<std::optional<UV>(UV &)> transformFunc)
    {
        // Default fallback: emulate with get, update, remove
        auto record = this->get(key);
        if (record.has_value()) {
            auto maybeUpdated = transformFunc(*record);
            if (maybeUpdated) {
                this->update(key, *maybeUpdated);
            } else {
                this->remove(key);
            }
        } else if (transformFunc(defaultValue).has_value()) {
            this->put(key, defaultValue);
        }
    };
    virtual void remove(const UK &key) = 0;
    virtual bool contains(const UK &key) = 0;
    virtual void update(const UK &key, const UV &value) = 0;

    virtual typename emhash7::HashMap<UK, UV> *entries() = 0;
};

#endif // FLINK_TNEL_MAPSTATE_H

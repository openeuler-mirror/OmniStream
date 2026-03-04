/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by w00850971 on 2025/6/23.
//

#ifndef OMNISTREAM_FALCONVALUE_H
#define OMNISTREAM_FALCONVALUE_H

#include <utility>
#include <functional>
#include "basictypes/Object.h"
#include "table/data/binary/BinaryRowData.h"

/**
 * Falcon value of RocksDBValueState、RocksDBMapState and RocksDBListState, which is composed deserialized value and a
 * flag indicates whether the state should be flushed to memTable and RocksDB.
 * @param [V] Type of deserialized value
 */
template <typename V>
class FalconValue {
public:
    FalconValue(const V& value, bool isDirty)
    {
        this->value = value; // deepcopy value outside using TypeSerializer
        dirty = isDirty;
        // [refcount] when value is create and insert into cache, increase its refcount; when value is delete and remove
        // from cache, decrease its refcount. In this way, value's refcount is controlled by falcon itself.
        if constexpr (std::is_same_v<V, Object*>) {
            reinterpret_cast<Object*>(this->value)->getRefCount();
        }
    }
    FalconValue() // used for LinkedHashMap to initialize link head and link tail
    {
        this->value = V();
        dirty = false;
        if constexpr (std::is_same_v<V, Object*>) {
            reinterpret_cast<Object*>(this->value)->getRefCount();
        }
    }
    ~FalconValue()
    {
        if constexpr (std::is_pointer_v<V>) {
            // [refcount] when falconValue is removed from cache, value's ref count should -1 to avoid memory leak.
            if constexpr (std::is_same_v<V, Object*>) {
                reinterpret_cast<Object*>(value)->putRefCount();
            } else {
                delete value;
            }
            value = nullptr;
        }
    }

    const V& getValue() const { return value; }
    [[nodiscard]] bool isDirty() const { return dirty; }
    void markAsDirty() { dirty = true; }
    void markAsClean() { dirty = false; }

private:
    V value;
    bool dirty; // isDirty=true means the value should be flushed to memTable and RocksDB
};

#endif // OMNISTREAM_FALCONVALUE_H
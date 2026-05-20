/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by w00850971 on 2025/6/23.
//

#ifndef OMNISTREAM_VALUESTATEFALCONKEY_H
#define OMNISTREAM_VALUESTATEFALCONKEY_H

#include <utility>
#include <functional>
#include "basictypes/Object.h"
#include "table/data/RowData.h"

/**
 * Falcon key of RocksDBValueState, which is composed deserialized key, namespace and columnFamily.
 * @param [K] Type of deserialized key
 * @param [N] Type of deserialized namespace
 */
template <typename K, typename N>
class ValueStateFalconKey {
public:
    ValueStateFalconKey(const K& key, const N& ns)
    {
        // K type can be Object*, RowData*(BinaryRowData*, GenericRowData* or JoinedRowData*), int32_t or int64_t.
        if constexpr (std::is_same_v<K, Object*>) {
            this->key = key;
            // [refcount] when key is create and insert into cache, increase its refcount; when key is delete and remove
            // from cache, decrease its refcount. In this way, key's refcount is controlled by falcon itself.
            if (this->key != nullptr) {
                reinterpret_cast<Object*>(this->key)->getRefCount();
            }
        } else if constexpr (std::is_same_v<K, RowData*>) {
            // deep copy key. todo: GenericRowData* and JoinedRowData* do not implement copy method
            this->key = reinterpret_cast<RowData*>(key)->copy();
        } else {
            this->key = key;  // int32_t and int64_t type do not need deepcopy
        }
        this->ns = ns;  // N type can be int64_t, TimeWindow ot VoidNamespace, do not need deepcopy.
    }
    ValueStateFalconKey()
    {
        // K type can be Object*, RowData*(BinaryRowData*, GenericRowData* or JoinedRowData*), int32_t or int64_t.
        if constexpr (std::is_same_v<K, Object*>) {
            this->key = K();
            // [refcount] when key is create and insert into cache, increase its refcount; when key is delete and remove
            // from cache, decrease its refcount. In this way, key's refcount is controlled by falcon itself.
            if (this->key != nullptr) {
                reinterpret_cast<Object*>(this->key)->getRefCount();
            }
        } else if constexpr (std::is_same_v<K, RowData*>) {
            // deep copy key. todo: GenericRowData* and JoinedRowData* do not implement copy method
            this->key = reinterpret_cast<RowData*>(K())->copy();
        } else {
            this->key = K();  // int32_t and int64_t type do not need deepcopy
        }
        this->ns = N();  // N type can be int64_t, TimeWindow ot VoidNamespace, do not need deepcopy.
    }
    ~ValueStateFalconKey()
    {
        if constexpr (std::is_same_v<K, Object*>) { // if K is Object*, putRefCount
            // [refcount] when falconKey is removed from cache, key's ref count should -1 to avoid memory leak.
            if (key != nullptr) {
                reinterpret_cast<Object*>(key)->putRefCount();
                key = nullptr;
            }
        } else if constexpr (std::is_same_v<K, RowData*>) { // if K is RowData*, delete if needed
            if (key != nullptr) {
                delete key;
                key = nullptr;
            }
        } else {
            // if K is int type, do nothing
        }
    }

    const K& getKey() const { return key; }
    const N& getNamespace() const { return ns; }

    // equals function for ValueStateFalconKey
    bool operator==(const ValueStateFalconKey& other) const
    {
        if constexpr (std::is_same_v<K, Object*>) {  // if K is Object*, use equals to compare
            if (key == nullptr && other.key == nullptr) {
                return ns == other.ns;
            } else if (key != nullptr && other.key != nullptr) {
                return reinterpret_cast<Object*>(key)->equals(other.key) && ns == other.ns;
            } else {
                return false;
            }
        } else { // if K is RowData* or int type, use == to compare. todo: GenericRowData == is not implemented
            return key == other.key && ns == other.ns;
        }
    }

private:
    K key;
    N ns;
};

namespace std {
    // hash function for ValueStateFalconKey
    template <typename K, typename N>
    struct hash<ValueStateFalconKey<K, N>> {
        size_t operator()(const ValueStateFalconKey<K, N>& v) const
        {
            size_t keyHash;
            if constexpr (std::is_same_v<K, Object*>) {
                keyHash = static_cast<size_t>(reinterpret_cast<Object*>(v.getKey())->hashCode());
            } else if constexpr (std::is_same_v<K, RowData*>) {
                // todo: GenericRowData* hash method is not implemented
                keyHash = static_cast<size_t>(reinterpret_cast<RowData*>(v.getKey())->hashCode());
            } else {
                keyHash = std::hash<K>()(v.getKey());
            }
            size_t nsHash = std::hash<N>()(v.getNamespace()) << 1;
            return keyHash ^ nsHash;
        }
    };

    // hash function for ValueStateFalconKey*
    template <typename K, typename N>
    struct hash<ValueStateFalconKey<K, N>*> {
        size_t operator()(const ValueStateFalconKey<K, N>* v) const
        {
            if (v == nullptr) {
                return 0;
            } else {
                return hash<ValueStateFalconKey<K, N>>{}(*v);
            }
        }
    };

    // equals function for ValueStateFalconKey*
    template <typename K, typename N>
    struct equal_to<ValueStateFalconKey<K, N>*> {
        bool operator()(const ValueStateFalconKey<K, N>* lhs, const ValueStateFalconKey<K, N>* rhs) const
        {
            if (lhs == rhs) return true;
            if (lhs == nullptr || rhs == nullptr) return false;
            return *lhs == *rhs;
        }
    };
}

#endif // OMNISTREAM_VALUESTATEFALCONKEY_H
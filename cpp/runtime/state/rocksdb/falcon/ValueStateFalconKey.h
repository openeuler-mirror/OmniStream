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
#include "table/data/binary/BinaryRowData.h"

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
        // todo: check if we do not deep copy key here, whether hash code will be miscalculate
        this->key = key;
        this->ns = ns;
        // [refcount] when key is create and insert into cache, increase its refcount; when key is delete and remove
        // from cache, decrease its refcount. In this way, key's refcount is controlled by falcon itself.
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(this->key)->getRefCount();
        }
        if constexpr (std::is_same_v<N, Object*>) {
            reinterpret_cast<Object*>(this->ns)->getRefCount();
        }
    }

    // used for LinkedHashMap to initialize link head and link tail
    ValueStateFalconKey()
    {
        this->key = K();
        this->ns = N();
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(this->key)->getRefCount();
        }
        if constexpr (std::is_same_v<N, Object*>) {
            reinterpret_cast<Object*>(this->ns)->getRefCount();
        }
    }

    ~ValueStateFalconKey()
    {
        if constexpr (std::is_pointer_v<K>) {
            // [refcount] when falconKey is removed from cache, key's ref count should -1 to avoid memory leak.
            if constexpr (std::is_same_v<K, Object*>) {
                reinterpret_cast<Object*>(key)->putRefCount();
            } else {
                delete key;
            }
            key = nullptr;
        }
        if constexpr (std::is_pointer_v<N>) {
            // [refcount] when falconKey is removed from cache, key's ref count should -1 to avoid memory leak.
            if constexpr (std::is_same_v<N, Object*>) {
                reinterpret_cast<Object*>(ns)->putRefCount();
            } else {
                delete ns;
            }
            ns = nullptr;
        }
    }

    const K& getKey() const { return key; }
    const N& getNamespace() const { return ns; }

    // equals function for ValueStateFalconKey
    bool operator==(const ValueStateFalconKey& other) const
    {
        if constexpr (std::is_same_v<K, Object*>) {  // DataStream case
            if (key == nullptr && other.key == nullptr) {
                return ns == other.ns;
            } else if (key != nullptr && other.key != nullptr) {
                return reinterpret_cast<Object*>(key)->equals(other.key) && ns == other.ns;
            } else {
                return false;
            }
        } else {  // SQL case(BinaryRowData or int16_t) and other case, use == directly
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
            if constexpr (std::is_same_v<K, Object*>) {  // DataStream case
                keyHash = static_cast<size_t>(reinterpret_cast<Object*>(v.getKey())->hashCode());
            } else if constexpr (std::is_same_v<K, BinaryRowData*>) {  // SQL case1
                keyHash = static_cast<size_t>(reinterpret_cast<BinaryRowData*>(v.getKey())->hashCode());
            } else {  // SQL case2(int16_t) and other case
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
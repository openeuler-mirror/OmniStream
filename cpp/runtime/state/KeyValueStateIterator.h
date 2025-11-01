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
#ifndef FLINK_TNEL_KEYVALUESTATEITERATOR_H
#define FLINK_TNEL_KEYVALUESTATEITERATOR_H

#include <vector>
#include <cstdint>

class KeyValueStateIterator {
public:
    virtual ~KeyValueStateIterator() = default;

    /**
     * Advances the iterator. Should only be called if isValid() returned true. Valid flag
     * can only change after calling next().
     */
    virtual void next() = 0;

    /** Returns the key-group for the current key. */
    virtual int keyGroup() const = 0;

    virtual std::vector<uint8_t> key() const = 0;
    virtual std::vector<uint8_t> value() const = 0;

    /** Returns the Id of the K/V state to which the current key belongs. */
    virtual int kvStateId() const = 0;

    virtual bool isNewKeyValueState() const = 0;

    virtual bool isNewKeyGroup() const = 0;

    virtual bool isValid() const = 0;

    virtual void close() = 0;
};

#endif // FLINK_TNEL_KEYVALUESTATEITERATOR_H
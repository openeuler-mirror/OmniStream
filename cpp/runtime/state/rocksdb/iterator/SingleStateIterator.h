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
#ifndef OMNISTREAM_SINGLESTATEITERATOR_H
#define OMNISTREAM_SINGLESTATEITERATOR_H

#include <vector>
#include <cstdint>
#include <memory>

/**
 * An interface for iterating over a single state in a RocksDB state backend.
 */
class SingleStateIterator {
public:
    virtual ~SingleStateIterator() = default;

    virtual void next() = 0;
    virtual bool isValid() const = 0;
    virtual std::vector<uint8_t> key() const = 0;
    virtual std::vector<uint8_t> value() const = 0;
    virtual int getKvStateId() const = 0;
    virtual void close() = 0;
};

#endif // OMNISTREAM_SINGLESTATEITERATOR_H

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
#ifndef OMNISTREAM_ROCKSSINGLESTATEITERATOR_H
#define OMNISTREAM_ROCKSSINGLESTATEITERATOR_H
#include <vector>
#include <stdexcept>
#include "SingleStateIterator.h"
#include "runtime/state/RocksIteratorWrapper.h"
#include "common.h"
#include <iostream>
#include <string>

/**
 * Wraps a RocksDB iterator to cache its current key and assigns an id for the key/value state.
 * Used by RocksStatesPerKeyGroupMergeIterator.
 */
class RocksSingleStateIterator : public SingleStateIterator {
public:
    /**
     * @param iterator Underlying RocksIteratorWrapper (ownership transferred)
     * @param kvStateId Id of the K/V state to which this iterator belongs
     */
    RocksSingleStateIterator(
        std::unique_ptr<RocksIteratorWrapper> iterator,
        int kvStateId)
        : iterator_(std::move(iterator)),
        kvStateId_(kvStateId)
    {
        if (!iterator_) {
            throw std::invalid_argument("RocksIteratorWrapper cannot be null");
        }
        currentKey_.clear();
        for(auto& c:iterator_->key()){
            currentKey_.push_back(c);
        }
    }

    void next() override
    {
        iterator_->next();
        if (iterator_->isValid()) {
            currentKey_.clear();
            for(auto& c:iterator_->key()){
                currentKey_.push_back(c);
            }
        }
    }

    bool isValid() const override
    {
        return iterator_ && iterator_->isValid();
    }

    std::vector<int8_t> key() const override
    {
        return currentKey_;
    }

    std::vector<int8_t> value() const override
    {
        std::vector<int8_t> value;
        for(auto& c:iterator_->value()) {
            value.push_back(c);
        }
        return value;
    }

    int getKvStateId() const override
    {
        return kvStateId_;
    }

    void close() override
    {
        if (iterator_) {
            iterator_->close();
        }
    }

private:
    std::unique_ptr<RocksIteratorWrapper> iterator_;
    std::vector<int8_t> currentKey_;
    int kvStateId_;
};
#endif // OMNISTREAM_ROCKSSINGLESTATEITERATOR_H

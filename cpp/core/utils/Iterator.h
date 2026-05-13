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
#pragma once

#include "common.h"

#include <memory>

namespace omnistream::utils {

/**
 * Interface for iterators.
 * Provides methods to iterate over a collection of elements of type T.
 */
template <typename E>
class Iterator {
public:
    virtual ~Iterator() = default;
    virtual bool hasNext() = 0;
    virtual E next() = 0;
    virtual void remove() {};
};

/**
 * Interface for iterable objects.
 * Provides a way to iterate over a collection of elements of type T.
 */
template <typename T>
class Iterable {
public:
    virtual ~Iterable() = default;

    /**
     * Returns an iterator over the elements of type T.
     */
    virtual std::unique_ptr<Iterator<T>> iterator() = 0;
};



/**
 * VectorIterator class for iterating over vectors of shared_ptr.
 * This is a reusable iterator that can be used with any vector of shared_ptr<T>.
 */
template <typename T>
class VectorIterator : public Iterator<std::shared_ptr<T>> {
public:
    VectorIterator(const std::vector<std::shared_ptr<T>>& vector)
        : vector_(vector),
          current_(0) {}

    bool hasNext() override {
        return current_ < vector_.size();
    }

    std::shared_ptr<T> next() override {
        if (!hasNext()) {
            return nullptr;
        }
        return vector_[current_++];
    }

    void remove() override {
        // Remove is not supported for VectorIterator
    }

private:
    const std::vector<std::shared_ptr<T>>& vector_;
    size_t current_;
};

/**
 * Creates an empty Iterable that contains no elements.
 * @return An empty Iterable instance.
 */
template <typename T>
std::shared_ptr<Iterable<T>> emptyIterable() {
    class EmptyIterable : public Iterable<T> {
    public:
        std::unique_ptr<Iterator<T>> iterator() override {
            class EmptyIterator : public Iterator<T> {
            public:
                bool hasNext() override { return false; }
                T next() override { return T(); }
                void remove() override { /* Remove is not supported for EmptyIterator */ }
            };
            return std::make_unique<EmptyIterator>();
        }
    };
    return std::make_shared<EmptyIterable>();
}

} // namespace omnistream::utils


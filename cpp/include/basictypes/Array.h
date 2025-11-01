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

#ifndef FLINK_TNEL_ARRAY_H
#define FLINK_TNEL_ARRAY_H

#include <securec.h>
#include <stdexcept>
#include <utility>
#include <initializer_list>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Object.h"

class Array : public Object {
public:

    using T = Object*;
    using reference = T&;
    using const_reference = const T&;
    using pointer = T*;
    using const_pointer = const T*;
    using iterator = T*;
    using const_iterator = const T*;

    void append(Object *value);
    Array() : length(0), data_(nullptr), capacity_(0) {}

    explicit Array(int size)
        : length(size), capacity_(size)
    {
        data_ = new T[size];
        errno_t ret = memset_s((void *)data_, sizeof(T) * size, 0, sizeof(T) * size);
        if (ret != 0) {
            throw std::runtime_error("memset_s failed" + std::to_string(ret));
        }
    }

    Array(int size, const T& value) : Array(size)
    {
        for (int i = 0; i < length; ++i) {
            data_[i] = value;
        }
    }

    Array(std::initializer_list<T> init) : Array(init.size())
    {
        int i = 0;
        for (const auto& item : init) {
            data_[i++] = item;
        }
    }

    ~Array()
    {
        for (int i = 0; i < this->length; ++i) {
            if (data_ != nullptr && data_[i])
                ((Object *)data_[i])->putRefCount();
        }
        if (data_)
            delete data_;
    }

    Array(const Array& other) : Array(other.length)
    {
        for (int i = 0; i < length; ++i) {
            data_[i] = other.data_[i];
        }
    }

    Array(Array&& other) noexcept
        : length(other.length), data_(other.data_), capacity_(other.capacity_)
    {
        other.data_ = nullptr;
        other.length = other.capacity_ = 0;
    }

    Array& operator=(const Array& other)
    {
        if (this != &other) {
            Array temp(other);
            swap(temp);
        }
        return *this;
    }

    Array& operator=(Array&& other) noexcept
    {
        if (this != &other) {
            delete[] data_;
            data_ = other.data_;
            length = other.length;
            capacity_ = other.capacity_;
            other.data_ = nullptr;
            other.length = other.capacity_ = 0;
        }
        return *this;
    }

    reference operator[](int index)
    {
        return data_[index];
    }

    const_reference operator[](int index) const
    {
        return data_[index];
    }

    reference at(int index)
    {
        if (index >= length) {
            throw std::out_of_range("Index out of range");
        }
        return data_[index];
    }

    const_reference at(int index) const
    {
        if (index >= length) {
            throw std::out_of_range("Index out of range");
        }
        return data_[index];
    }

    reference front() { return data_[0]; }
    const_reference front() const { return data_[0]; }
    reference back() { return data_[length - 1]; }
    const_reference back() const { return data_[length - 1]; }

    pointer data() noexcept { return data_; }
    const_pointer data() const noexcept { return data_; }

    iterator begin() noexcept { return data_; }
    const_iterator begin() const noexcept { return data_; }
    const_iterator cbegin() const noexcept { return data_; }

    iterator end() noexcept { return data_ + length; }
    const_iterator end() const noexcept { return data_ + length; }
    const_iterator cend() const noexcept { return data_ + length; }

    bool empty() const noexcept { return length == 0; }
    int size() const noexcept { return length; }
    int capacity() const noexcept { return capacity_; }

    void reserve(int new_capacity)
    {
        if (new_capacity <= capacity_) return;

        T* new_data = new T[new_capacity];
        for (int i = 0; i < length && data_ != nullptr; ++i) {
            new_data[i] = std::move(data_[i]);
        }

        if (data_)
            delete[] data_;
        data_ = new_data;
        capacity_ = new_capacity;
    }

    void resize(int new_size)
    {
        if (new_size > capacity_) {
            reserve(new_size);
        }
        length = new_size;
    }

    void push_back(const T& value)
    {
        if (length >= capacity_) {
            reserve(capacity_ == 0 ? 1 : capacity_ * EXPAND_SIZE);
        }
        data_[length++] = value;
    }

    void push_back(T&& value)
    {
        if (length >= capacity_) {
            reserve(capacity_ == 0 ? 1 : capacity_ * EXPAND_SIZE);
        }
        data_[length++] = std::move(value);
    }

    template <typename... Args>
    reference emplace_back(Args&&... args)
    {
        if (length >= capacity_) {
            reserve(capacity_ == 0 ? 1 : capacity_ * EXPAND_SIZE);
        }
        new(data_ + length) T(std::forward<Args>(args)...);
        return data_[length++];
    }

    void pop_back()
    {
        if (length > 0) --length;
    }

    void swap(Array& other) noexcept
    {
        using std::swap;
        swap(data_, other.data_);
        swap(length, other.length);
        swap(capacity_, other.capacity_);
    }

    bool operator==(const Array& other) const
    {
        if (length != other.length) return false;
        for (int i = 0; i < length; ++i) {
            if (data_[i] != other.data_[i]) return false;
        }
        return true;
    }

    bool operator!=(const Array& other) const
    {
        return !(*this == other);
    }

    bool equals(Object *obj);

    Object* clone();

    void set(int index, T obj);

    T get(int index);

    int length;
private:
    static const int EXPAND_SIZE = 2;
    T* data_;
    int capacity_;
};

#endif // FLINK_TNEL_ARRAY_H

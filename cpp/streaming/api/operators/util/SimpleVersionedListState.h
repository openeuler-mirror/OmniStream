#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <stdexcept>
#include <iostream>

#include "core/api/common/state/ListState.h"
#include "core/io/SimpleVersionedSerializer.h"
#include "core/io/SimpleVersionedSerialization.h"

// SimpleVersionedListState类
template <typename T>
class SimpleVersionedListState : public ListState<T> {
private:
    // 成员变量
    std::shared_ptr<ListState<std::vector<uint8_t>>> rawState;
    std::shared_ptr<SimpleVersionedSerializer<T>> serializer;
    
    // 辅助方法
    std::vector<uint8_t> serialize(const T& value) const;
    std::vector<std::vector<uint8_t>>* serializeAll(const std::vector<T>& values) const;
    std::vector<T*>* deserializeAll(const std::vector<std::vector<uint8_t>>* rawValues) const;
    
public:
    /**
     * 创建一个新的SimpleVersionedListState，使用给定的序列化器从给定的原始ListState中读写字节。
     */
    SimpleVersionedListState(
            std::shared_ptr<ListState<std::vector<uint8_t>>> rawState,
            std::shared_ptr<SimpleVersionedSerializer<T>> serializer);
    
    virtual ~SimpleVersionedListState() = default;
    
    // ListState接口方法实现
    void add(const T& value) override;
    void update(const std::vector<T>& values) override;
    std::vector<T>* get() override;
    void merge(const std::vector<T>& other) override;
    void addAll(const std::vector<T>& values) override;
    void clear() override;
};

// SimpleVersionedListState构造函数实现
template <typename T>
SimpleVersionedListState<T>::SimpleVersionedListState(
        std::shared_ptr<ListState<std::vector<uint8_t>>> rawState,
        std::shared_ptr<SimpleVersionedSerializer<T>> serializer)
    : rawState(rawState), serializer(serializer) {
    if (!rawState) {
        throw std::invalid_argument("rawState cannot be null");
    }
    if (!serializer) {
        throw std::invalid_argument("serializer cannot be null");
    }
}

// serialize辅助方法实现
template <typename T>
std::vector<uint8_t> SimpleVersionedListState<T>::serialize(const T& value) const {
    INFO_RELEASE("h30082497 SimpleVersionedListState:serialize 1");
    return SimpleVersionedSerialization::writeVersionAndSerialize(*serializer, value);
}

// serializeAll辅助方法实现
template <typename T>
std::vector<std::vector<uint8_t>>* SimpleVersionedListState<T>::serializeAll(const std::vector<T>& values) const {
    auto rawValues = new std::vector<std::vector<uint8_t>>();
    rawValues->reserve(values.size());

    for (const auto& value : values) {
        rawValues->push_back(serialize(value));
    }
    INFO_RELEASE("h30082497 SimpleVersionedListState:serializeAll rawValues->size() " + std::to_string(rawValues->size()));
    return rawValues;
}

// deserializeAll辅助方法实现
template <typename T>
std::vector<T*>* SimpleVersionedListState<T>::deserializeAll(const std::vector<std::vector<uint8_t>>* rawValues) const {
    if (rawValues == nullptr) {
        return nullptr;
    }
    
    auto values = new std::vector<T*>();
    values->reserve(rawValues->size());
    
    for (auto& rawValue : const_cast<std::vector<std::vector<uint8_t>>&>(*rawValues)) {
        values->push_back(SimpleVersionedSerialization::readVersionAndDeSerialize(*serializer, rawValue));
    }
    
    return values;
}

// add方法实现
template <typename T>
void SimpleVersionedListState<T>::add(const T& value) {
    auto serialized = serialize(value);
    INFO_RELEASE("h30082497 SimpleVersionedListState:add  serialized:size " + std::to_string(serialized.size()));
    rawState->add(serialized);
}

// update方法实现
template <typename T>
void SimpleVersionedListState<T>::update(const std::vector<T>& values) {
    auto rawValues = serializeAll(values);
    rawState->update(*rawValues);
    delete rawValues;
    INFO_RELEASE("h30082497 SimpleVersionedListState:update  rawState:size " + std::to_string(rawState->get()->size()));
}

// get方法实现
template <typename T>
std::vector<T>* SimpleVersionedListState<T>::get() {
    auto rawValues = rawState->get();
    auto ptrValues = deserializeAll(rawValues);
    
    if (ptrValues == nullptr) {
        return nullptr;
    }
    
    auto values = new std::vector<T>();
    values->reserve(ptrValues->size());
    
    for (auto ptr : *ptrValues) {
        values->push_back(*ptr);
        delete ptr;
    }
    
    delete ptrValues;
    return values;
}

// merge方法实现
template <typename T>
void SimpleVersionedListState<T>::merge(const std::vector<T>& other) {
    auto rawValues = serializeAll(other);
    INFO_RELEASE("h30082497 SimpleVersionedListState:merge  other:size " + std::to_string(other.size()));
    INFO_RELEASE("h30082497 SimpleVersionedListState:merge  rawValues:size " + std::to_string(rawValues->size()));
    rawState->merge(*rawValues);
    delete rawValues;
}

// addAll方法实现
template <typename T>
void SimpleVersionedListState<T>::addAll(const std::vector<T>& values) {
    auto rawValues = serializeAll(values);
    INFO_RELEASE("h30082497 SimpleVersionedListState:addAll  values:size " + std::to_string(values.size()));
    INFO_RELEASE("h30082497 SimpleVersionedListState:addAll  rawValues:size " + std::to_string(rawValues->size()));
    rawState->addAll(*rawValues);
    delete rawValues;
}

// clear方法实现
template <typename T>
void SimpleVersionedListState<T>::clear() {
    rawState->clear();
}

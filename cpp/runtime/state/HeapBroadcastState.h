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

#ifndef OMNISTREAM_HEAPBROADCASTSTATE_H
#define OMNISTREAM_HEAPBROADCASTSTATE_H

#include <memory>
#include <map>
#include <vector>
#include <optional>

#include "core/typeutils/MapSerializer.h"

#include "BackendWritableBroadcastState.h"
#include "RegisteredBroadcastStateBackendMetaInfo.h"

template <typename K = STATE_MV, typename V = STATE_MV>
class HeapBroadcastState : public BackendWritableBroadcastState<K, V> {
public:
    HeapBroadcastState(const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo_,
                       const std::shared_ptr<std::map<K, V>>& internalMap_,
                       const std::shared_ptr<MapSerializer>& internalMapCopySerializer_)
        : stateMetaInfo(stateMetaInfo_),
          internalMap(internalMap_),
          internalMapCopySerializer(internalMapCopySerializer_) {}

    HeapBroadcastState(const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo_,
                       const std::shared_ptr<MapSerializer>& internalMapCopySerializer_)
        : stateMetaInfo(stateMetaInfo_),
          internalMapCopySerializer(internalMapCopySerializer_) {
        initInternalMap();
    }

    HeapBroadcastState(const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo_)
        : HeapBroadcastState(stateMetaInfo_,
                             std::make_shared<MapSerializer>(stateMetaInfo_->getKeySerializer(), stateMetaInfo_->getValueSerializer())) {}

    void setStateMetaInfo(const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo_) override  {
        internalMapCopySerializer = std::make_shared<MapSerializer>(
            stateMetaInfo_->getKeySerializer(),
            stateMetaInfo_->getValueSerializer());
        stateMetaInfo = stateMetaInfo_;
    }

    std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo> getStateMetaInfo() const override {
        return stateMetaInfo;
    }

    template<typename SK, typename SV>
    void setAndConvertInternalMap(const std::shared_ptr<std::map<SK, SV>>& internalMap_) {
        if (internalMap == nullptr) {
            initInternalMap();
        }
        for (auto& entry : *internalMap_) {
            if (std::holds_alternative<K>(entry.first) && std::holds_alternative<V>(entry.second)) {
                put(std::get<K>(entry.first), std::get<V>(entry.second));
            }
        }
    }

    void initInternalMap() {
        internalMap = std::shared_ptr<std::map<K, V>>();
    }

    void setInternalMap(const std::shared_ptr<std::map<K, V>>& internalMap_) {
        internalMap = internalMap_;
    }

    std::shared_ptr<std::map<K, V>> getInternalMap() {
        return internalMap;
    }

    std::shared_ptr<MapSerializer> getInternalMapCopySerializer() {
        return internalMapCopySerializer;
    }

    std::shared_ptr<BackendWritableBroadcastState<K, V>> deepCopy() override {
        return std::dynamic_pointer_cast<BackendWritableBroadcastState<K, V>>(copy());
    }

    std::shared_ptr<HeapBroadcastState<K, V>> copy() {
        return std::make_shared<HeapBroadcastState<K, V>>(std::make_shared<RegisteredBroadcastStateBackendMetaInfo>(*this->stateMetaInfo),
                                                          std::make_shared<std::map<K, V>>(*this->internalMap),
                                                          std::make_shared<MapSerializer>(*this->internalMapCopySerializer));
    }

    long write(DataOutputSerializer& out) override {
        INFO_RELEASE("h30082497 HeapBroadcastState::write 1");
        if (internalMap == nullptr) {
        INFO_RELEASE("h30082497 HeapBroadcastState::write 1 if 1");
            initInternalMap();
        INFO_RELEASE("h30082497 HeapBroadcastState::write 1 if end");
        }
        INFO_RELEASE("h30082497 HeapBroadcastState::write 2");
        long offset = out.getPosition();
        INFO_RELEASE("h30082497 HeapBroadcastState::write 3");

        out.writeInt(internalMap->size());
        INFO_RELEASE("h30082497 HeapBroadcastState::write 4");

        for (const auto& entry : *internalMap) {
        INFO_RELEASE("h30082497 HeapBroadcastState::write 4 for 1");
            getStateMetaInfo()->getKeySerializer()->serialize(variantToObject(entry.first), out);
        INFO_RELEASE("h30082497 HeapBroadcastState::write 4 for 2");
            getStateMetaInfo()->getValueSerializer()->serialize(variantToObject(entry.second), out);
        INFO_RELEASE("h30082497 HeapBroadcastState::write 4 for end");
        }
        INFO_RELEASE("h30082497 HeapBroadcastState::write end");

        return offset;

    }

    void put(const K& key_, const V& value_) override {
        (*internalMap)[key_] = value_;
    }

    void putAll(const std::map<K, V>& map_) override {
        for (const auto& entry : map_) {
            (*internalMap)[entry.first] = entry.second;
        }
    }

    void remove(const K& key_) override {
        internalMap->erase(key_);
    }

    std::optional<V> get(const K& key_) const override {
        auto iterator = internalMap->find(key_);
        if (iterator != internalMap->end()) {
            return iterator->second;
        }
        return std::optional<V>{};
    }

    bool contains(const K& key_) const override {
        return internalMap->find(key_) != internalMap->end();
    }

    std::vector<std::pair<K, V>> entries() override {
        std::vector<std::pair<K, V>> result;
        result.reserve(internalMap->size());
        for (auto& entry : *internalMap) {
            result.emplace_back(entry.first, entry.second);
        }
        return result;
    }

    std::vector<std::pair<K, V>> immutableEntries() const override {
        std::vector<std::pair<K, V>> result;
        result.reserve(internalMap->size());
        for (const auto& entry : *internalMap) {
            result.emplace_back(entry.first, entry.second);
        }
        return result;
    }

    void clear() override {
        internalMap->clear();
    }

private:
    std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo> stateMetaInfo;
    std::shared_ptr<std::map<K, V>> internalMap;
    std::shared_ptr<MapSerializer> internalMapCopySerializer;
};

#endif //OMNISTREAM_HEAPBROADCASTSTATE_H
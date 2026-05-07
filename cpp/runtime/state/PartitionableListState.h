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

#ifndef OMNISTREAM_PARTITIONABLELISTSTATE_H
#define OMNISTREAM_PARTITIONABLELISTSTATE_H

#include <memory>
#include <string>
#include <vector>
#include <set>
#include <typeinfo>

#include "core/memory/DataOutputSerializer.h"
#include "core/api/common/variants/CustomVariant.h"
#include "core/api/common/state/ListState.h"
#include "core/typeutils/ListSerializer.h"

#include "RegisteredOperatorStateBackendMetaInfo.h"

template<typename S>
class PartitionableListState : public ListState<S> {
public:
    PartitionableListState(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> stateMetaInfo_,
                           const std::shared_ptr<std::vector<S>> internalList_,
                           const std::shared_ptr<ListSerializer> internalListCopySerializer_)
        : stateMetaInfo(stateMetaInfo_),
          internalList(internalList_),
          internalListCopySerializer(internalListCopySerializer_) {}

    PartitionableListState(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> stateMetaInfo_,
                           const std::shared_ptr<ListSerializer> internalListCopySerializer_)
        : stateMetaInfo(stateMetaInfo_),
          internalListCopySerializer(internalListCopySerializer_) {
        initInternalList();
    }

    PartitionableListState(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> stateMetaInfo_)
        : PartitionableListState(stateMetaInfo_, std::make_shared<ListSerializer>(stateMetaInfo_->getStateSerializer())) {}

    void setStateMetaInfo(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> stateMetaInfo_) {
        initInternalList();
        internalListCopySerializer = std::make_shared<ListSerializer>(stateMetaInfo_->getStateSerializer());
        stateMetaInfo = stateMetaInfo_;
    }

    std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> getStateMetaInfo() {
        return stateMetaInfo;
    }

    void initInternalList() {
        internalList = std::make_shared<std::vector<S>>();
    }

    void setInternalList(const std::shared_ptr<std::vector<S>>& internalList_) {
        internalList = internalList_;
    }

    std::shared_ptr<std::vector<S>> getInternalList() {
        return internalList;
    }

    std::shared_ptr<ListSerializer> getInternalListCopySerializer() {
        return internalListCopySerializer;
    }

    std::shared_ptr<PartitionableListState<S>> deepCopy() {
        return std::make_shared<PartitionableListState<S>>(std::make_shared<RegisteredOperatorStateBackendMetaInfo>(*this->stateMetaInfo),
                                                           std::make_shared<std::vector<S>>(*this->internalList),
                                                           std::make_shared<ListSerializer>(*this->internalListCopySerializer));
    }

    void add(const S& value_) override {
        internalList->push_back(value_);
    }

    std::vector<long> write(DataOutputSerializer& out, long basePos = 0) {
        INFO_RELEASE("savepoint:  PartitionableListState::write");
        if (internalList == nullptr) {
            initInternalList();
            INFO_RELEASE("savepoint:  PartitionableListState::initInternalList");
        }
        std::vector<long> offsets(internalList->size());
        INFO_RELEASE("savepoint:  PartitionableListState::write internalList->size()= "<<internalList->size());
        for (size_t i = 0; i < internalList->size(); ++i) {
            offsets[i] = out.getPosition() + basePos;
        INFO_RELEASE("savepoint:  PartitionableListState::write i= "<<i<<"offsets[i]= "<< offsets[i]);
            auto element = (*internalList)[i];
            stateMetaInfo->getStateSerializer()->serialize(&element, out);
        }

        INFO_RELEASE("h30082497 PartitionableListState::write end");
        return offsets;
    }

    void update(const std::vector<S>& values_) override {
        INFO_RELEASE("savepoint:  PartitionableListState::update values_ size: "<<values_.size());
        internalList->clear();
        addAll(values_);
        INFO_RELEASE("savepoint:  PartitionableListState::update internalList->size()= "<<internalList->size());
    }

    std::vector<S>* get() override {
        return internalList.get();
    }

    void merge(const std::vector<S>& other_) override {
        if (other_.empty()) {
            return;
        }
        std::set<S> existSet(internalList->begin(), internalList->end());
        for (const S& element: other_) {
            if (existSet.find(element) == existSet.end()) {
                existSet.insert(element);
                internalList->push_back(element);
            }
        }
    }

    void addAll(const std::vector<S>& values_) override {
        if (values_.empty()) {
            return;
        }
        internalList->insert(internalList->end(), values_.begin(), values_.end());
    }

    void clear() override {
        internalList->clear();
    }

private:
    std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> stateMetaInfo;
    std::shared_ptr<std::vector<S>> internalList;
    std::shared_ptr<ListSerializer> internalListCopySerializer;
};

#endif //OMNISTREAM_PARTITIONABLELISTSTATE_H
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

#include "core/memory/DataOutputSerializer.h"
#include "core/api/common/variants/CustomVariant.h"
#include "core/api/common/state/ListState.h"
#include "core/typeutils/ListSerializer.h"

#include "RegisteredOperatorStateBackendMetaInfo.h"


template<typename S>
class PartitionableListState : public ListState<S> {
public:
    PartitionableListState(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo,
                           const std::shared_ptr<std::vector<S>>& internalList)
        : stateMetaInfo_(stateMetaInfo),
          internalList_(internalList),
          internalListCopySerializer_(std::make_shared<ListSerializer>(stateMetaInfo->getStateSerializer())) {}

    PartitionableListState(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo,
                           const std::shared_ptr<ListSerializer>& internalListCopySerializer)
        : stateMetaInfo_(stateMetaInfo),
          internalListCopySerializer_(internalListCopySerializer) {
        initInternalList();
    }

    PartitionableListState(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo)
        : PartitionableListState(stateMetaInfo, std::make_shared<ListSerializer>(stateMetaInfo->getStateSerializer())) {}

    void setStateMetaInfo(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo) {
        internalListCopySerializer_ = std::make_shared<ListSerializer>(stateMetaInfo->getStateSerializer());
        stateMetaInfo_ = stateMetaInfo;
    }

    std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> getStateMetaInfo() {
        return stateMetaInfo_;
    }

    void initInternalList() {
            INFO_RELEASE("h30082497 PartitionableListState::initInternalList");
        internalList_ = std::make_shared<std::vector<S>>();
    }

    void setInternalList(const std::shared_ptr<std::vector<S>>& internalList) {
        internalList_ = internalList;
    }

    std::shared_ptr<std::vector<S>> getInternalList() {
        return internalList_;
    }

    std::shared_ptr<ListSerializer> getInternalListCopySerializer() {
        return internalListCopySerializer_;
    }

    std::shared_ptr<PartitionableListState<S>> deepCopy() {
        return std::make_shared<PartitionableListState<S>>(std::make_shared<RegisteredOperatorStateBackendMetaInfo>(*this->stateMetaInfo_),
                                                           std::make_shared<std::vector<S>>(*this->internalList_),
                                                           std::make_shared<ListSerializer>(*this->internalListCopySerializer_));
    }

    void add(const S& value) override {
        INFO_RELEASE("h30082497 PartitionableListState::add size: b " + std::to_string(internalList_->size()));
        internalList_->push_back(value);
        INFO_RELEASE("h30082497 PartitionableListState::add size: a " + std::to_string(internalList_->size()));
    }

    std::vector<long> write(long startPos, DataOutputSerializer& out) {
        INFO_RELEASE("h30082497 PartitionableListState::write 1 internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(internalList_.get())));
        INFO_RELEASE("h30082497 PartitionableListState::write size: a " + std::to_string((*internalList_).size()));
        INFO_RELEASE("h30082497 PartitionableListState::write 2");
        std::vector<long> offsets;
        INFO_RELEASE("h30082497 PartitionableListState::write 3");

        for (size_t i = 0; i < internalList_->size(); i++) {
            INFO_RELEASE("h30082497 PartitionableListState::write 5 i = " + std::to_string(i));
            auto element = (*internalList_)[i];
            INFO_RELEASE("h30082497 PartitionableListState::write 5 element addr: " + std::to_string(reinterpret_cast<uintptr_t>(&element)));
            offsets.push_back(startPos + out.getPosition());
            INFO_RELEASE("h30082497 PartitionableListState::write 6");
            getStateMetaInfo()->getStateSerializer()->serialize(&element, out);
            INFO_RELEASE("h30082497 PartitionableListState::write 7");
        }

        INFO_RELEASE("h30082497 PartitionableListState::write end");
        return offsets;
    }

    void update(const std::vector<S>& values) override {
        INFO_RELEASE("h30082497 PartitionableListState::update");
        clear();
        addAll(values);
    }

    std::vector<S>* get() override {
        return internalList_.get();
    }

    void merge(const std::vector<S>& other) override {
        INFO_RELEASE("h30082497 PartitionableListState::merge");
        if (other.empty()) {
            INFO_RELEASE("h30082497 PartitionableListState::merge other is empty");
            return;
        }
        INFO_RELEASE("h30082497 PartitionableListState::merge other size: " + std::to_string(other.size()));
        INFO_RELEASE("h30082497 PartitionableListState::merge size: b " + std::to_string(internalList_->size()));
        std::set<S> existSet(internalList_->begin(), internalList_->end());
        for (const S& element: other) {
            if (existSet.find(element) == existSet.end()) {
                existSet.insert(element);
                internalList_->push_back(element);
            }
        }
        INFO_RELEASE("h30082497 PartitionableListState::merge size: a " + std::to_string(internalList_->size()));
    }

    void addAll(const std::vector<S>& values) override {
        INFO_RELEASE("h30082497 PartitionableListState::addAll");
        if (values.empty()) {
            return;
        }
        INFO_RELEASE("h30082497 PartitionableListState::addAll size: cc " + std::to_string(internalList_->size()));
        internalList_->reserve(internalList_->size() + values.size());
        INFO_RELEASE("h30082497 PartitionableListState::addAll values size: " + std::to_string(values.size()));
        INFO_RELEASE("h30082497 PartitionableListState::addAll size: b " + std::to_string(internalList_->size()));
        internalList_->insert(internalList_->end(), values.begin(), values.end());

        INFO_RELEASE("h30082497 PartitionableListState::addAll size: a " + std::to_string(internalList_->size()));
    }

    void clear() override {
        INFO_RELEASE("h30082497 PartitionableListState::clear");
        INFO_RELEASE("h30082497 PartitionableListState::clear size: b " + std::to_string(internalList_->size()));
        internalList_->clear();
        INFO_RELEASE("h30082497 PartitionableListState::clear size: a " + std::to_string(internalList_->size()));
    }

private:
    std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> stateMetaInfo_;
    std::shared_ptr<std::vector<S>> internalList_;
    std::shared_ptr<ListSerializer> internalListCopySerializer_;
};

#endif //OMNISTREAM_PARTITIONABLELISTSTATE_H
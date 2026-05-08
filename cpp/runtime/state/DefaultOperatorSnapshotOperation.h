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

#ifndef OMNISTREAM_DEFAULTOPERATORSNAPSHOTOPERATION_H
#define OMNISTREAM_DEFAULTOPERATORSNAPSHOTOPERATION_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

#include "core/include/common.h"

#include "CheckpointStateOutputStreamProxy.h"
#include "DefaultOperatorStateBackendSnapshotResources.h"
#include "SnapshotResult.h"

class DefaultOperatorSnapshotOperation : public SnapshotResultSupplier<OperatorStateHandle> {
public:
    DefaultOperatorSnapshotOperation(
        long checkpointId,
        CheckpointOptions* checkpointOptions,
        CheckpointStreamFactory* streamFactory,
        std::shared_ptr<DefaultOperatorStateBackendSnapshotResources> snapshotResources)
        : checkpointId_(checkpointId),
          checkpointOptions_(checkpointOptions),
          streamFactory_(streamFactory),
          snapshotResources_(std::move(snapshotResources)) {
        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation 2");
    }

    std::shared_ptr<SnapshotResult<OperatorStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) override {
        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 1");
        try {
            auto registeredOperatorStatesDeepCopies = snapshotResources_->getRegisteredOperatorStatesDeepCopies();
            auto registeredBroadcastStatesDeepCopies = snapshotResources_->getRegisteredBroadcastStatesDeepCopies();
            auto operatorStateMetaInfoSnapshots = snapshotResources_->getOperatorStateMetaInfoSnapshots();
            auto broadcastStateMetaInfoSnapshots = snapshotResources_->getBroadcastStateMetaInfoSnapshots();
            if (registeredOperatorStatesDeepCopies->empty() && registeredBroadcastStatesDeepCopies->empty()) {
                INFO_RELEASE("h30082497 DefaultOperatorStateBackendSnapshotStrategy::get is empty");
                return SnapshotResult<OperatorStateHandle>::Empty();
            }

            CheckpointStateOutputStreamProxy outputStreamProxy(bridge, checkpointId_, checkpointOptions_);

            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 2");
            outputStreamProxy.writeOperatorMetaData(operatorStateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots);

            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 3");
            int initialMapCapacity = registeredOperatorStatesDeepCopies->size() + registeredBroadcastStatesDeepCopies->size();
            auto writtenStatesMetaData = std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo>(initialMapCapacity);

            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4");
            for (auto& entry : *registeredOperatorStatesDeepCopies) {
                std::string name = entry.first;
        // INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 1 internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(entry.second.get())));
                    auto state = std::dynamic_pointer_cast<PartitionableListState<std::vector<uint8_t>>>(entry.second);
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates === name : " + name);
                if (state != nullptr) {
                    if (state->getInternalList() == nullptr) {
                        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if getInternalList is null");
                    }        
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(state->getInternalList().get())));

                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for size : " + std::to_string(state->getInternalList().get()->size()));
                    if (state->getInternalList()->size() == 0) {
                        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for internalList addr: " + std::to_string(state->getInternalList()->size()));

                        INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if getInternalList is empty");
                    }
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 1");
                    long startPos = outputStreamProxy.getPos();
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 2 === startPos : " + std::to_string(startPos));
                    DataOutputSerializer out;
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 3");
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 4");
                    auto offsets = state->write(startPos, out);
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 5 === out.length() : " + std::to_string(out.length()));
                    outputStreamProxy.writeBytes(out.getData(), out.length());
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 6");
                    auto distributionMode = state->getStateMetaInfo()->getAssignmentMode();
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if 7");
                    writtenStatesMetaData.emplace(name, OperatorStateHandle::StateMetaInfo(offsets, distributionMode));
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 4 for registeredOperatorStates if end");
                }
            }

            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5");
            for (auto& entry : *registeredBroadcastStatesDeepCopies) {
                std::string name = entry.first;
                auto state = std::dynamic_pointer_cast<HeapBroadcastState<std::vector<uint8_t>, std::vector<uint8_t>>>(entry.second);
                INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates === name : " + name);
                if (state != nullptr) {
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 1");
                    long startPos = outputStreamProxy.getPos();
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 2");
                    DataOutputSerializer out;
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 3");
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 4");
                    std::vector<long> offsets = {state->write(startPos, out)};
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 5");
                    outputStreamProxy.writeBytes(out.getData(), out.length());
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 6");
                    auto distributionMode = state->getStateMetaInfo()->getAssignmentMode();
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if 7");
                    writtenStatesMetaData.emplace(name, OperatorStateHandle::StateMetaInfo(offsets, distributionMode));
                    INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 5 for registeredBroadcastStates if end");
                }
            }

            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 6");
            auto handle = outputStreamProxy.close();
            if (handle) {
            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 7");
                auto jobManagerOwnedSnapshot = handle->GetJobManagerOwnedSnapshot();
            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 8");
                snapshotResources_->cleanup();
            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 9");
                auto stateHandle = std::make_shared<OperatorStreamStateHandle>(writtenStatesMetaData, jobManagerOwnedSnapshot);

                INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get end");
                return SnapshotResult<OperatorStateHandle>::WithLocalState(stateHandle, nullptr);
            }
            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 9");
            snapshotResources_->cleanup();
            INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get end");
                return SnapshotResult<OperatorStateHandle>::Empty();
        } catch (const std::exception &e) {
            snapshotResources_->cleanup();
            GErrorLog("DefaultOperatorSnapshotOperation get exception : " + std::string(e.what()));
            throw std::runtime_error("DefaultOperatorSnapshotOperation get failed.");
        }
    }

protected:
    long checkpointId_;
    CheckpointOptions* checkpointOptions_;
    CheckpointStreamFactory* streamFactory_;
    std::shared_ptr<DefaultOperatorStateBackendSnapshotResources> snapshotResources_;
};

#endif //OMNISTREAM_DEFAULTOPERATORSNAPSHOTOPERATION_H

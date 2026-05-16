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
    }

    std::shared_ptr<SnapshotResult<OperatorStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) override {
        try {
            auto registeredOperatorStatesDeepCopies = snapshotResources_->getRegisteredOperatorStatesDeepCopies();
            auto registeredBroadcastStatesDeepCopies = snapshotResources_->getRegisteredBroadcastStatesDeepCopies();
            auto operatorStateMetaInfoSnapshots = snapshotResources_->getOperatorStateMetaInfoSnapshots();
            auto broadcastStateMetaInfoSnapshots = snapshotResources_->getBroadcastStateMetaInfoSnapshots();
            if (registeredOperatorStatesDeepCopies->empty() && registeredBroadcastStatesDeepCopies->empty()) {
                return SnapshotResult<OperatorStateHandle>::Empty();
            }

            CheckpointStateOutputStreamProxy outputStreamProxy(bridge, checkpointId_, checkpointOptions_);

            outputStreamProxy.writeOperatorMetaData(operatorStateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots);

            int initialMapCapacity = registeredOperatorStatesDeepCopies->size() + registeredBroadcastStatesDeepCopies->size();
            auto writtenStatesMetaData = std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo>(initialMapCapacity);

            for (auto& entry : *registeredOperatorStatesDeepCopies) {
                std::string name = entry.first;
        // INFO_RELEASE("h30082497 DefaultOperatorSnapshotOperation::get 1 internalList addr: " + std::to_string(reinterpret_cast<uintptr_t>(entry.second.get())));
                    auto state = std::dynamic_pointer_cast<PartitionableListState<std::vector<uint8_t>>>(entry.second);
                if (state != nullptr) {
                    if (state->getInternalList() == nullptr) {
                    }        

                    if (state->getInternalList()->size() == 0) {

                    }
                    long startPos = outputStreamProxy.getPos();
                    DataOutputSerializer out;
                    auto offsets = state->write(startPos, out);
                    outputStreamProxy.writeBytes(out.getData(), out.length());
                    auto distributionMode = state->getStateMetaInfo()->getAssignmentMode();
                    writtenStatesMetaData.emplace(name, OperatorStateHandle::StateMetaInfo(offsets, distributionMode));
                }
            }

            for (auto& entry : *registeredBroadcastStatesDeepCopies) {
                std::string name = entry.first;
                auto state = std::dynamic_pointer_cast<HeapBroadcastState<std::vector<uint8_t>, std::vector<uint8_t>>>(entry.second);
                if (state != nullptr) {
                    long startPos = outputStreamProxy.getPos();
                    DataOutputSerializer out;
                    std::vector<long> offsets = {state->write(startPos, out)};
                    outputStreamProxy.writeBytes(out.getData(), out.length());
                    auto distributionMode = state->getStateMetaInfo()->getAssignmentMode();
                    writtenStatesMetaData.emplace(name, OperatorStateHandle::StateMetaInfo(offsets, distributionMode));
                }
            }

            auto handle = outputStreamProxy.close();
            if (handle) {
                auto jobManagerOwnedSnapshot = handle->GetJobManagerOwnedSnapshot();
                snapshotResources_->cleanup();
                auto stateHandle = std::make_shared<OperatorStreamStateHandle>(writtenStatesMetaData, jobManagerOwnedSnapshot);

                return SnapshotResult<OperatorStateHandle>::WithLocalState(stateHandle, nullptr);
            }
            snapshotResources_->cleanup();
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

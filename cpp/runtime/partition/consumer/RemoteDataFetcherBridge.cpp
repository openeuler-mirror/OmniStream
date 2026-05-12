/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#include "RemoteDataFetcherBridge.h"
#include "RemoteInputChannel.h"

namespace omnistream {
    void RemoteDataFetcherBridge::InitCppRemoteInputChannel(std::vector<std::shared_ptr<SingleInputGate> > inputGates)
    {
        for (const auto &inputGate: inputGates) {
            const auto &channelMap = inputGate->getInputChannels();
            for (const auto &pair: channelMap) {
                const auto &channel = pair.second;
                if (auto remoteChannel = std::dynamic_pointer_cast<RemoteInputChannel>(channel)) {
                    remoteChannel->SetRemoteDataFetcherBridge(shared_from_this());
                    remoteChannel->SetForwardResumeToJava(inputGate->GetForwardResumeToJava());
                    LOG("In RemoteDataFetcher Init, set RemoteDataFetcherBridge to RemoteInputChannel");
                } else {
                    LOG("In RemoteDataFetcher Init, this Channel is not a RemoteInputChannel");
                }
            }
        }
    }
}

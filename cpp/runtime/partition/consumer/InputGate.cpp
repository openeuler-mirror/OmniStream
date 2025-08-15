/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/1/25.
//

// InputGate.cpp
#include "InputGate.h"

namespace omnistream {


std::shared_ptr<CompletableFuture> InputGate::getAvailableFuture() {
    return availabilityHelper.getAvailableFuture();
}

std::vector<InputChannelInfo> InputGate::getChannelInfos() {
    std::vector<InputChannelInfo> channelInfos;
    for (int index = 0; index < getNumberOfInputChannels(); index++) {
        channelInfos.push_back(getChannel(index)->getChannelInfo());
    }
    return channelInfos;
}

std::shared_ptr<CompletableFuture>  InputGate::getPriorityEventAvailableFuture() {
    return priorityAvailabilityHelper.getAvailableFuture();
}

template <typename INPUT, typename DATA>
InputGate::InputWithData<INPUT, DATA>::InputWithData(std::shared_ptr<INPUT> input, std::shared_ptr<DATA> data, bool moreAvailable, bool morePriorityEvents)
    : input(input), data(data), moreAvailable(moreAvailable), morePriorityEvents(morePriorityEvents) {}
} // namespace omnistream
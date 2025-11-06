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

#include "InputGate.h"

namespace omnistream {
std::shared_ptr<CompletableFuture> InputGate::GetAvailableFuture()
{
    return availabilityHelper.GetAvailableFuture();
}

std::vector<InputChannelInfo> InputGate::getChannelInfos()
{
    std::vector<InputChannelInfo> channelInfos;
    for (int index = 0; index < GetNumberOfInputChannels(); index++) {
        channelInfos.push_back(getChannel(index)->getChannelInfo());
    }
    return channelInfos;
}

bool InputGate::fromOriginal()
{
    // Determine whether all InputChannelInfo have the omni type.
    // Once it exists, it indicates that the upstream task is an original Java task.
    bool res = false;
    for (int index = 0; index < GetNumberOfInputChannels(); index++) {
        if (getChannel(index)->getChannelInfo().getOmni()) {
            res = true;
            break;
        }
    }
    return res;
}

std::shared_ptr<CompletableFuture> InputGate::getPriorityEventAvailableFuture()
{
    return priorityAvailabilityHelper.GetAvailableFuture();
}

template <typename INPUT, typename DATA>
InputGate::InputWithData<INPUT, DATA>::InputWithData(std::shared_ptr<INPUT> input, std::shared_ptr<DATA> data, bool moreAvailable, bool morePriorityEvents)
    : input(input), data(data), moreAvailable(moreAvailable), morePriorityEvents(morePriorityEvents) {}
} // namespace omnistream
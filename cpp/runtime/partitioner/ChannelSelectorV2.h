/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// ChannelSelector.h
#ifndef CHANNELSELECTOR_V2_H
#define CHANNELSELECTOR_V2_H

#include <memory>
#include <unordered_map>

namespace omnistream {
    template <typename T>
    class ChannelSelectorV2 {
    public:
        virtual ~ChannelSelectorV2() {}

        virtual void setup(int numberOfChannels) = 0;
        virtual std::unordered_map<int, T*> selectChannel(T* record) = 0;
        [[nodiscard]] virtual bool isBroadcast() const = 0;
    };
}

#endif // CHANNELSELECTOR_H


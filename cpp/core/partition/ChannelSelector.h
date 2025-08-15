/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// ChannelSelector.h
#ifndef CHANNELSELECTOR_H
#define CHANNELSELECTOR_H


namespace omnistream::datastream {
    template <typename T>
    class ChannelSelector {
    public:
        virtual ~ChannelSelector() {}

        virtual void setup(int numberOfChannels) = 0;
        virtual int selectChannel(T *record) = 0;
        virtual bool isBroadcast() const = 0;
    };
}


#endif // CHANNELSELECTOR_H


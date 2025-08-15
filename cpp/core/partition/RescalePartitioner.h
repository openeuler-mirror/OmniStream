/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// RescalePartitioner.h
#ifndef RESCALEPARTITIONER_H
#define RESCALEPARTITIONER_H

#include "StreamPartitioner.h"

namespace omnistream::datastream {
    template<typename T>
    class RescalePartitioner : public StreamPartitioner<T> {
    public:
        RescalePartitioner() : nextChannelToSendTo(-1) {}

        int selectChannel(T* record) override
        {
            if (++nextChannelToSendTo >= this->numberOfChannels) {
                nextChannelToSendTo = 0;
            }
            return nextChannelToSendTo;
        }

        std::unique_ptr<StreamPartitioner<T>> copy() override
        {
            return std::make_unique<RescalePartitioner<T>>(*this);
        }

        bool isPointWise() const override
        {
            return true;
        }

        std::string toString() const override
        {
            return "RESCALE";
        }
    private:
        mutable int nextChannelToSendTo = -1;
    };
}

#endif // RESCALEPARTITIONER_H

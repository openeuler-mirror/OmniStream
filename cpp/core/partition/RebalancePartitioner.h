/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef REBALANCEPARTITIONER_H
#define REBALANCEPARTITIONER_H


#include <random>
#include "StreamPartitioner.h"

namespace omnistream::datastream {
    template<typename T>
    class RebalancePartitioner : public StreamPartitioner<T> {
    public:
        RebalancePartitioner() : randomEngine(std::random_device{}()) {}

        void setup(int numberOfChannels) override
        {
            StreamPartitioner<T>::setup(numberOfChannels);
            std::uniform_int_distribution<int> distribution(0, this->numberOfChannels - 1);
            nextChannelToSendTo = distribution(randomEngine);
        }

        int selectChannel(T* record) override
        {
            nextChannelToSendTo = (nextChannelToSendTo + 1) % this->numberOfChannels;
            return nextChannelToSendTo;
        }

        std::unique_ptr<StreamPartitioner<T>> copy() override
        {
            return std::make_unique<RebalancePartitioner<T>>(*this);
        }

        bool isPointWise() const override
        {
            return false;
        }

        [[nodiscard]] std::string toString() const override
        {
            return "REBALANCE";
        }
    private:
        int nextChannelToSendTo;
        std::default_random_engine randomEngine;
    };
}

#endif  //REBALANCEPARTITIONER_H

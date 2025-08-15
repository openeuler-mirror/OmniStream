/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/7/25.
//

#ifndef REBALANCEPARTITIONERV2_H
#define REBALANCEPARTITIONERV2_H

#include <random>

#include "table/vectorbatch/VectorBatch.h"
#include "StreamPartitionerV2.h"
#include "vector/vector_helper.h"

namespace omnistream {
    template<typename T>
    class RebalancePartitionerV2 : public StreamPartitionerV2<T> {
    public:
        RebalancePartitionerV2() : randomEngine(std::random_device{}()) {}

        void setup(int numberOfChannels) override
        {
            StreamPartitionerV2<T>::setup(numberOfChannels);
            std::uniform_int_distribution<int> distribution(0, this->numberOfChannels - 1);
            nextChannelToSendTo = distribution(randomEngine);
        }

        int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
        {
            nextChannelToSendTo = (nextChannelToSendTo + 1) % this->numberOfChannels;
            return nextChannelToSendTo;
        }

        int selectRowChannel(RowData* record) override
        {
            nextChannelToSendTo = (nextChannelToSendTo + 1) % this->numberOfChannels;
            return nextChannelToSendTo;
        }

        std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(RoundRobinMapper());
        }

        std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(ArbitraryMapper());
        }

        std::unique_ptr<StreamPartitionerV2<T>> copy() override
        {
            return std::make_unique<RebalancePartitionerV2<T>>(*this);
        }

        bool isPointwise() override
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


#endif //REBALANCEPARTITIONERV2_H

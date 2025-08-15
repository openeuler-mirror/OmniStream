/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/7/25.
//

#ifndef SHUFFLEPARTITIONERV2_H
#define SHUFFLEPARTITIONERV2_H

#include <random>
#include "StreamPartitionerV2.h"

namespace omnistream {
    template<typename T>
    class ShufflePartitionerV2 : public StreamPartitionerV2<T> {
    public:
        ShufflePartitionerV2() : randomEngine(std::random_device{}()) {}

        ShufflePartitionerV2(const ShufflePartitionerV2& other) : randomEngine(other.randomEngine) {}

        int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
        {
            std::uniform_int_distribution<int> distribution(0, this->numberOfChannels - 1);
            return distribution(randomEngine);
        }

        std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(RoundRobinMapper());
        }

        std::unique_ptr<StreamPartitionerV2<T>> copy() override
        {
            return std::make_unique<ShufflePartitionerV2<T>>(*this);
        }

        bool isPointwise() override
        {
            return false;
        }

        [[nodiscard]] std::string toString() const override
        {
            return "SHUFFLE";
        }

    private:
        std::default_random_engine randomEngine;
    };
}


#endif //SHUFFLEPARTITIONERV2_H

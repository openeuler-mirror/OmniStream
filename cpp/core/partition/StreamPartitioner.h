/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef STREAMPARTITIONER_H
#define STREAMPARTITIONER_H

#include <string>
#include "ChannelSelector.h"

namespace omnistream::datastream {
    template<typename T>
    class StreamPartitioner : public ChannelSelector<T> {
    public:
        StreamPartitioner() : numberOfChannels(0) {}

        virtual ~StreamPartitioner() {}

        void setup(int numberOfChannels) override
        {
            this->numberOfChannels = numberOfChannels;
        }

        bool isBroadcast() const override
        {
            return false;
        }

        virtual bool isPointWise() const = 0;
        virtual std::unique_ptr<StreamPartitioner<T>> copy() = 0;
        [[nodiscard]] virtual std::string toString() const = 0;
    protected:
        int numberOfChannels;
    };
}
#endif

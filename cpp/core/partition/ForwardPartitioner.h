/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// ForwardPartitioner.h
#ifndef FORWARDPARTITIONER_H
#define FORWARDPARTITIONER_H

#include "StreamPartitioner.h"

namespace omnistream::datastream {
    template <typename T>
    class ForwardPartitioner : public StreamPartitioner<T> {
    public:
        ForwardPartitioner() {}
        ~ForwardPartitioner() {}

        int selectChannel(T* record) override
        {
            return 0;
        }

        std::unique_ptr<StreamPartitioner<T>> copy() override
        {
            return std::make_unique<ForwardPartitioner<T>>(*this);
        }

        bool isPointWise() const override
        {
            return true;
        }

        [[nodiscard]] std::string toString() const override
        {
            return "FORWARD";
        }
    };
}


#endif // FORWARDPARTITIONER_H


/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNISTREAM_GLOBALPARTITIONER_H
#define OMNISTREAM_GLOBALPARTITIONER_H

#include "StreamPartitioner.h"
namespace omnistream::datastream {
    template <typename T>
    class GlobalPartitioner : public StreamPartitioner<T> {
    public:
        GlobalPartitioner()
        {
        };

        ~GlobalPartitioner()
        {
        };

        void setup(int numberOfChannels) override
        {
            StreamPartitioner<T>::setup(numberOfChannels);
        }

        int selectChannel(T* record) override
        {
            return 0;
        };

        bool isPointWise() const override
        {
            return true;
        };

        std::string toString() const
        {
            return "GLOBAL";
        };

        std::unique_ptr<StreamPartitioner<T>> copy() override
        {
            return std::make_unique<GlobalPartitioner<T>>(*this);
        }
    };
}
#endif // OMNISTREAM_GLOBALPARTITIONER_H

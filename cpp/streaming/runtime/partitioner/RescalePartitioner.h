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

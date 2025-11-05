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

#ifndef STREAMPARTITIONER_H
#define STREAMPARTITIONER_H

#include <string>
#include <memory>
#include "ChannelSelector.h"

namespace omnistream::datastream {
    template<typename T>
    class StreamPartitioner : public ChannelSelector<T> {
    public:
        StreamPartitioner() : numberOfChannels(0) {}

        virtual ~StreamPartitioner() {}

        void setup(int numberOfChannels_) override
        {
            this->numberOfChannels = numberOfChannels_;
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

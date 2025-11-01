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


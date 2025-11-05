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

#ifndef BUFFERLISTENER_H
#define BUFFERLISTENER_H

#include "Buffer.h"

namespace omnistream {

    class BufferListener {
    public:
        virtual ~BufferListener() = default;

        virtual bool notifyBufferAvailable(std::shared_ptr<Buffer> buffer) = 0;
        virtual void notifyBufferDestroyed() = 0;

        virtual std::string toString() const
        {
            return "BufferListener";
        }
    };
};

#endif // BUFFERLISTENER_H

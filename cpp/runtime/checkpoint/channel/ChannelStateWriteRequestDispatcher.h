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
#ifndef OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_H
#define OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_H

#include <exception>
#include "ChannelStateWriteRequest.h"

namespace omnistream {

    class ChannelStateWriteRequestDispatcher {
    public:
        virtual ~ChannelStateWriteRequestDispatcher() = default;

        virtual void dispatch(ChannelStateWriteRequest &request) = 0;
        virtual void fail(const std::exception_ptr &cause) = 0;
    };

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_H
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
#ifndef OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_EXECUTOR_H
#define OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_EXECUTOR_H

#include <exception>
#include <memory>
#include "runtime/jobgraph/JobVertexID.h"
#include "ChannelStateWriteRequest.h"

namespace omnistream {

    class ChannelStateWriteRequestExecutor {
    public:
        virtual ~ChannelStateWriteRequestExecutor() = default;

        virtual void start() = 0;
        virtual void submit(std::unique_ptr<ChannelStateWriteRequest> request) = 0;
        virtual void submitPriority(std::unique_ptr<ChannelStateWriteRequest> request) = 0;
        virtual void registerSubtask(const JobVertexID &jobVertexID, int subtaskIndex) = 0;
        virtual void releaseSubtask(const JobVertexID &jobVertexID, int subtaskIndex) = 0;
        virtual void shutdown() = 0;
    };

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_EXECUTOR_H
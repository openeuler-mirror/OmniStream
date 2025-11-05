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

#ifndef OMNISTREAM_RESULTSUBPARTITIONVIEW_H
#define OMNISTREAM_RESULTSUBPARTITIONVIEW_H

#include <memory>
#include <string>
#include <stdexcept>

#include "AvailabilityWithBacklog.h"
#include "BufferAndBacklog.h"

namespace omnistream {


class ResultSubpartitionView {
public:
    virtual ~ResultSubpartitionView() = default;

    virtual std::shared_ptr<BufferAndBacklog> getNextBuffer() = 0;
    virtual void notifyDataAvailable() = 0;
    virtual void notifyPriorityEvent(int priorityBufferNumber) {}
    virtual void releaseAllResources() = 0;
    virtual bool isReleased() = 0;
    virtual void resumeConsumption() = 0;
    virtual void acknowledgeAllDataProcessed() = 0;
    virtual std::shared_ptr<std::exception> getFailureCause() = 0;
    virtual AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) = 0;
    virtual int unsynchronizedGetNumberOfQueuedBuffers() = 0;
    virtual int getNumberOfQueuedBuffers() = 0;
    virtual void notifyNewBufferSize(int newBufferSize) = 0;
};

} // namespace omnistream

#endif // OMNISTREAM_RESULTSUBPARTITIONVIEW_H
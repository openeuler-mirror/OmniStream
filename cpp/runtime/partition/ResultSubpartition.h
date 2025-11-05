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

#ifndef OMNISTREAM_RESULTSUBPARTITION_H
#define OMNISTREAM_RESULTSUBPARTITION_H

#include <iostream>
#include <memory>
#include <string>

#include "BufferAvailabilityListener.h"
#include "ResultPartition.h"
#include "ResultSubpartitionInfoPOD.h"
#include "ResultSubpartitionView.h"

namespace omnistream {
    class ResultSubpartition {
    public:
        ResultSubpartition() {};
        ResultSubpartition(int index, std::shared_ptr<ResultPartition> parent);
        virtual ~ResultSubpartition() = 0;

        ResultSubpartitionInfoPOD getSubpartitionInfo() const
        {
            return this->subpartitionInfo;
        };
        int getSubPartitionIndex() const
        {
            return this->subpartitionInfo.getPartitionIdx();
        };
        void onConsumedSubpartition()
        {
            this->parent->OnConsumedSubpartition(getSubPartitionIndex());
        };

        // virtual int add(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength) = 0;
        virtual int add(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength) = 0;
        virtual void flush() = 0;
        virtual void finish() = 0;
        virtual void release() = 0;
        virtual bool isReleased() = 0;
        virtual std::shared_ptr<ResultSubpartitionView> createReadView(
            std::shared_ptr<BufferAvailabilityListener> availabilityListener) = 0;
        virtual int unsynchronizedGetNumberOfQueuedBuffers() = 0;
        virtual int getNumberOfQueuedBuffers() = 0;
        virtual void bufferSize(int desirableNewBufferSize) = 0;
        virtual long getTotalNumberOfBuffers() const = 0;
        virtual long getTotalNumberOfBytes() const = 0;
        virtual int getBuffersInBacklogUnsafe() const = 0;
        virtual std::string toString() = 0;

    protected:
        ResultSubpartitionInfoPOD subpartitionInfo;
        int index;
        std::shared_ptr<ResultPartition> parent;
    };
} // namespace omnistream

#endif // OMNISTREAM_RESULTSUBPARTITION_H

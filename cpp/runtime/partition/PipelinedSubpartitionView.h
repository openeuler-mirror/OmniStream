/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// PipelinedSubpartitionView.h
#ifndef PIPELINEDSUBPARTITIONVIEW_H
#define PIPELINEDSUBPARTITIONVIEW_H

#include <memory>
#include <atomic>
#include <string>

#include "AvailabilityWithBacklog.h"
#include "BufferAndBacklog.h"
#include "BufferAvailabilityListener.h"
#include "ResultSubpartitionView.h"

namespace omnistream {
    class PipelinedSubpartition;

    class PipelinedSubpartitionView: public   ResultSubpartitionView  {
    public:
        PipelinedSubpartitionView(std::shared_ptr<PipelinedSubpartition> parent, std::shared_ptr<BufferAvailabilityListener> listener);
        PipelinedSubpartitionView();
        ~PipelinedSubpartitionView() override;

        std::shared_ptr<BufferAndBacklog> getNextBuffer() override;
        void notifyDataAvailable() override;
        void notifyPriorityEvent(int priorityBufferNumber) override;
        void releaseAllResources() override;
        bool isReleased() override;
        void resumeConsumption() override;
        void acknowledgeAllDataProcessed() override;
        AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) override;
        std::shared_ptr<std::exception> getFailureCause() override;
        int unsynchronizedGetNumberOfQueuedBuffers() override;
        int getNumberOfQueuedBuffers() override;
        void notifyNewBufferSize(int newBufferSize) override;
        std::string toString();
        std::string getClassSimpleName();

    private:
        std::shared_ptr<PipelinedSubpartition> parent;
        std::shared_ptr<BufferAvailabilityListener> availabilityListener;
        std::atomic<bool> isReleased_;
    };

} // namespace omnistream

#endif // PIPELINEDSUBPARTITIONVIEW_H
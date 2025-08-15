/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// PipelinedSubpartitionView.cpp
#include "PipelinedSubpartitionView.h"
#include <sstream>
#include <stdexcept>
#include "PipelinedSubpartition.h"
#include "BufferAvailabilityListener.h"
#include "ResultSubpartition.h"
#include "AvailabilityWithBacklog.h"

namespace omnistream {

PipelinedSubpartitionView::PipelinedSubpartitionView(std::shared_ptr<PipelinedSubpartition> parent, std::shared_ptr<BufferAvailabilityListener> listener)
    : parent(parent), availabilityListener(listener), isReleased_(false)
{
    if (!parent || !listener) {
        throw std::invalid_argument("parent and listener cannot be null");
    }
}

PipelinedSubpartitionView::PipelinedSubpartitionView() : parent(nullptr), availabilityListener(nullptr), isReleased_(false) {}

PipelinedSubpartitionView::~PipelinedSubpartitionView() {}

std::shared_ptr<BufferAndBacklog> PipelinedSubpartitionView::getNextBuffer()
{
    // LOG_TRACE(">>> beginnning of get NextBuffer")
    return parent->pollBuffer();
}

void PipelinedSubpartitionView::notifyDataAvailable()
{
    availabilityListener->notifyDataAvailable();
}

void PipelinedSubpartitionView::notifyPriorityEvent(int priorityBufferNumber)
{
    availabilityListener->notifyPriorityEvent(priorityBufferNumber);
}

void PipelinedSubpartitionView::releaseAllResources()
{
    bool expected = false;
    bool desired = true;
    if (isReleased_.compare_exchange_strong(expected, desired)) {
        parent->onConsumedSubpartition();
    }
}

bool PipelinedSubpartitionView::isReleased()
{
    return isReleased_.load() || parent->isReleased();
}

void PipelinedSubpartitionView::resumeConsumption()
{
    parent->resumeConsumption();
}

void PipelinedSubpartitionView::acknowledgeAllDataProcessed()
{
    parent->acknowledgeAllDataProcessed();
}

AvailabilityWithBacklog PipelinedSubpartitionView::getAvailabilityAndBacklog(int numCreditsAvailable)
{
    return parent->getAvailabilityAndBacklog(numCreditsAvailable);
}

std::shared_ptr<std::exception> PipelinedSubpartitionView::getFailureCause() {
    return {};
}

int PipelinedSubpartitionView::unsynchronizedGetNumberOfQueuedBuffers()
{
    return parent->unsynchronizedGetNumberOfQueuedBuffers();
}

int PipelinedSubpartitionView::getNumberOfQueuedBuffers()
{
    return parent->getNumberOfQueuedBuffers();
}

void PipelinedSubpartitionView::notifyNewBufferSize(int newBufferSize)
{
    parent->bufferSize(newBufferSize);
}

std::string PipelinedSubpartitionView::toString()
{
    std::stringstream ss;
    ss << this->getClassSimpleName() << "(index: " << parent->getSubPartitionIndex() << ") of ResultPartition ";
    return ss.str();
}

std::string PipelinedSubpartitionView::getClassSimpleName()
{
    return "PipelinedSubpartitionView";
}

} // namespace omnistream
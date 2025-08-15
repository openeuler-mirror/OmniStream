/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BUFFERAVAILABILITYLISTENER_H_
#define OMNISTREAM_BUFFERAVAILABILITYLISTENER_H_

#include <memory>
#include <sstream>
#include <string>
#include "virtual_enable_shared_from_this_base.h"

namespace omnistream {

class BufferAvailabilityListener : public virtual_enable_shared_from_this<BufferAvailabilityListener> {
public:
    ~BufferAvailabilityListener() override = default;

    virtual void notifyDataAvailable() = 0;

    virtual void notifyPriorityEvent(int prioritySequenceNumber)
    {}

    virtual std::shared_ptr<BufferAvailabilityListener> shared_from_BAListener()
    {
        return shared_from_this();
    }

    virtual std::string toString() const
    {
        std::stringstream ss;
        ss << "BufferAvailabilityListener";
        return ss.str();
    }
};

class DefaultBufferAvailabilityListener : public BufferAvailabilityListener {
public:
    DefaultBufferAvailabilityListener() = default;
    ~DefaultBufferAvailabilityListener() override = default;

    void notifyDataAvailable() override
    {}

    void notifyPriorityEvent(int prioritySequenceNumber) override
    {}

    std::string toString() const override
    {
        std::stringstream ss;
        ss << "DefaultBufferAvailabilityListener";
        return ss.str();
    }
};

}  // namespace omnistream

#endif  // OMNISTREAM_BUFFERAVAILABILITYLISTENER_H_
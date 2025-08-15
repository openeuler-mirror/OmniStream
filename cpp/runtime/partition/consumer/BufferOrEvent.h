/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// BufferOrEvent.h
#ifndef OMNISTREAM_BUFFEROREVENT_H
#define OMNISTREAM_BUFFEROREVENT_H

#include <memory>
#include <string>
#include <buffer/ObjectBuffer.h>
#include <event/AbstractEvent.h>

#include "InputChannelInfo.h"

namespace omnistream {


    class BufferOrEvent {
    public:
        BufferOrEvent(std::shared_ptr<ObjectBuffer> buffer,InputChannelInfo channelInfo,
                      bool moreAvailable, bool morePriorityEvents);

        BufferOrEvent(int  event, bool hasPriority,
                      InputChannelInfo channelInfo, bool moreAvailable, int size,
                      bool morePriorityEvents);

        // Visible for testing
        BufferOrEvent(std::shared_ptr<ObjectBuffer> buffer, InputChannelInfo channelInfo);

        // Visible for testing
        BufferOrEvent(int event, InputChannelInfo channelInfo);

        bool isBuffer() const;
        bool isEvent() const;

        std::shared_ptr<ObjectBuffer> getBuffer() const;
        int  getEvent() const;
        InputChannelInfo getChannelInfo() const;

        void setChannelInfo(InputChannelInfo channelInfo);

        bool moreAvailable() const;
        bool morePriorityEvents() const;

        std::string toString() const;

        void setMoreAvailable(bool moreAvailable);

        int getSize() const;
        bool hasPriority() const;

    private:
        std::shared_ptr<ObjectBuffer> buffer_;
        int event_;
        bool hasPriority_;
        bool moreAvailable_;
        bool morePriorityEvents_;
        InputChannelInfo channelInfo_;
        int size_;
    };

} // namespace omnistream

#endif // OMNISTREAM_BUFFEROREVENT_H
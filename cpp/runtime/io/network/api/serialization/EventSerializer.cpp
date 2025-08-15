/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/10/25.
//

#include "EventSerializer.h"

#include <vector>
#include <cstring>
#include <buffer/EventBuffer.h>

namespace omnistream
{
    const int  EventSerializer::INVALID_EVENT = -1 ;
    const int EventSerializer::END_OF_PARTITION_EVENT = 0;
    const int EventSerializer::CHECKPOINT_BARRIER_EVENT = 1;
    const int EventSerializer::END_OF_SUPERSTEP_EVENT = 2;
    const int EventSerializer::OTHER_EVENT = 3;
    const int EventSerializer::CANCEL_CHECKPOINT_MARKER_EVENT = 4;
    const int EventSerializer::END_OF_CHANNEL_STATE_EVENT = 5;
    const int EventSerializer::ANNOUNCEMENT_EVENT = 6;
    const int EventSerializer::VIRTUAL_CHANNEL_SELECTOR_EVENT = 7;
    const int EventSerializer::END_OF_USER_RECORDS_EVENT = 8;
    const int EventSerializer::CHECKPOINT_TYPE_CHECKPOINT = 0;
    const int EventSerializer::CHECKPOINT_TYPE_SAVEPOINT = 1;
    const int EventSerializer::CHECKPOINT_TYPE_SAVEPOINT_SUSPEND = 2;
    const int EventSerializer::CHECKPOINT_TYPE_SAVEPOINT_TERMINATE = 3;

     std::shared_ptr<ObjectBuffer> EventSerializer::toBuffer(const int event, bool hasPriority)
     {
         return std::make_shared<EventBuffer>(event);
     }

     std::shared_ptr<ObjectBufferConsumer> EventSerializer::toBufferConsumer(const int event, bool hasPriority)
     {
         auto buffer = std::make_shared<VectorBatchBuffer>(event);
         return std::make_shared<ObjectBufferConsumer> ( buffer, 1);
     }

    std::shared_ptr<ObjectBufferConsumer> EventSerializer::ToBufferConsumer(std::shared_ptr<AbstractEvent> event,
        bool hasPriority)
    {
        if (event->GetEventClassID() == AbstractEvent::endOfData) {
         // at this time, ignoring the stop mode for simplication, assume it always the drain
            return toBufferConsumer(EventSerializer::END_OF_USER_RECORDS_EVENT, false);
        } else {
            NOT_IMPL_EXCEPTION
        }
    }


     int EventSerializer::fromBuffer(const std::shared_ptr<ObjectBuffer>& buffer)
     {
         auto vectorBatchBuffer = std::dynamic_pointer_cast<VectorBatchBuffer>(buffer);
         return vectorBatchBuffer->EventType();
     }
}

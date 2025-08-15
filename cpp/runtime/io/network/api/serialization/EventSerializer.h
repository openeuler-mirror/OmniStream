/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_EVENTSERIALIZER_H
#define OMNISTREAM_EVENTSERIALIZER_H

#include <memory>
#include <vector>
#include <cstdint>
#include <utility>
#include <buffer/ObjectBufferConsumer.h>

#include "event/AbstractEvent.h"


namespace omnistream {
    class ObjectBuffer;

    class EventSerializer {
    public:

    static std::shared_ptr<ObjectBuffer> toBuffer(const int event, bool hasPriority) ;
    static std::shared_ptr<ObjectBufferConsumer> toBufferConsumer(const int event, bool hasPriority) ;
    static std::shared_ptr<ObjectBufferConsumer> ToBufferConsumer(std::shared_ptr<AbstractEvent> event,
        bool hasPriority);
    static int fromBuffer(const std::shared_ptr<ObjectBuffer>& buffer);

    public:

        static const int INVALID_EVENT;;
    static const int END_OF_PARTITION_EVENT;
    static const int CHECKPOINT_BARRIER_EVENT;
    static const int END_OF_SUPERSTEP_EVENT;
    static const int OTHER_EVENT;
    static const int CANCEL_CHECKPOINT_MARKER_EVENT;
    static const int END_OF_CHANNEL_STATE_EVENT;
    static const int ANNOUNCEMENT_EVENT;
    static const int VIRTUAL_CHANNEL_SELECTOR_EVENT;
    static const int END_OF_USER_RECORDS_EVENT;

    static const int CHECKPOINT_TYPE_CHECKPOINT;
    static const int CHECKPOINT_TYPE_SAVEPOINT;
    static const int CHECKPOINT_TYPE_SAVEPOINT_SUSPEND;
    static const int CHECKPOINT_TYPE_SAVEPOINT_TERMINATE;
};

}

#endif // OMNISTREAM_EVENTSERIALIZER_H
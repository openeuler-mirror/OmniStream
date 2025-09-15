/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */


#ifndef OMNISTREAM_OBJECTBUFFERLISTENER_H
#define OMNISTREAM_OBJECTBUFFERLISTENER_H

#include <memory>
#include <string>

namespace omnistream {

    class ObjectBuffer;

    class ObjectBufferListener {
    public:
        virtual ~ObjectBufferListener() = default;

        virtual bool notifyBufferAvailable(std::shared_ptr<ObjectBuffer> buffer) = 0;
        virtual void notifyBufferDestroyed() = 0;

        virtual std::string toString() const {
            return "ObjectBufferListener";
        }
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERLISTENER_H

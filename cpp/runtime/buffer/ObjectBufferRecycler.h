/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#ifndef OMNISTREAM_OBJECTBUFFERRECYCLER_H
#define OMNISTREAM_OBJECTBUFFERRECYCLER_H

#include <memory>
#include <string>

#include "ObjectSegment.h"

namespace omnistream {

    class ObjectBufferRecycler {
    public:
        virtual ~ObjectBufferRecycler() = default;
        virtual void recycle(std::shared_ptr<ObjectSegment> objectBuffer) = 0;

        std::string toString() const
        {
            return "ObjectBufferRecycler";
        };
    };

    class DummyObjectBufferRecycler : public ObjectBufferRecycler {
    public:
        static std::shared_ptr<DummyObjectBufferRecycler> getInstance()
        {
            if (!instance) {
                instance = std::make_shared<DummyObjectBufferRecycler>();
            }
            return instance;
        };
        DummyObjectBufferRecycler() = default;
        ~DummyObjectBufferRecycler() override = default;
        void recycle(std::shared_ptr<ObjectSegment> objectBuffer) override {};
        std::string toString() const
        {
            return "DummyObjectBufferRecycler";
        };
    private:
        static std::shared_ptr<DummyObjectBufferRecycler> instance;
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERRECYCLER_H
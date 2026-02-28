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

#ifndef OMNISTREAM_OBJECTBUFFERRECYCLER_H
#define OMNISTREAM_OBJECTBUFFERRECYCLER_H

#include <memory>
#include <string>

#include "ObjectSegment.h"
#include "BufferRecycler.h"
#include "core/memory/Segment.h"

namespace omnistream {

    class ObjectBufferRecycler : public BufferRecycler {
    public:
        ~ObjectBufferRecycler() override = default;

        std::string toString() const override
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
        void recycle(Segment *objectBuffer) override
        {};

        std::string toString() const override
        {
            return "DummyObjectBufferRecycler";
        };
    private:
        static std::shared_ptr<DummyObjectBufferRecycler> instance;
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERRECYCLER_H
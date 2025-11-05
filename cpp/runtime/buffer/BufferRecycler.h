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

#ifndef BUFFERRECYCLER_H
#define BUFFERRECYCLER_H

#include <memory>
#include "core/memory/Segment.h"
#include <string>
#include <concepts>

namespace omnistream {

    class BufferRecycler {
    public:
        virtual ~BufferRecycler() = default;

        virtual void recycle(std::shared_ptr<Segment> objectBuffer) = 0;

        [[nodiscard]] virtual std::string toString() const = 0;
    };

    class DummyBufferRecycler : public BufferRecycler {
    public:
        static std::shared_ptr<DummyBufferRecycler> getInstance()
        {
            if (!instance) {
                instance = std::make_shared<DummyBufferRecycler>();
            }
            return instance;
        };
        DummyBufferRecycler() = default;
        ~DummyBufferRecycler() override = default;
        void recycle(std::shared_ptr<Segment> objectBuffer) override {
        };

        [[nodiscard]] std::string toString() const override
        {
            return "DummyBufferRecycler";
        };
    private:
        static std::shared_ptr<DummyBufferRecycler> instance;
    };

}

#endif // BUFFERRECYCLER_H

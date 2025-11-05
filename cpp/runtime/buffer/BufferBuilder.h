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

#ifndef BUFFERBUILDER_H
#define BUFFERBUILDER_H

#include <memory>
#include <string>
#include <atomic>

#include "Buffer.h"
#include "BufferConsumer.h"
#include "PositionMarker.h"

namespace omnistream {

    class BufferBuilder {
    public:
        virtual ~BufferBuilder() = default;
        explicit BufferBuilder(std::shared_ptr<Buffer> buffer);

        virtual std::shared_ptr<BufferConsumer> createBufferConsumerFromBeginning() = 0;
        virtual std::shared_ptr<BufferConsumer> createBufferConsumer(int currentReaderPosition) = 0;

        std::shared_ptr<BufferConsumer> createBufferConsumer();
        int appendAndCommit(void* source);
        virtual int append(void* source) = 0;
        void commit();
        int finish();
        bool isFinished();
        bool isFull();

        int getMaxCapacity();
        void trim(int newSize);
        void close();
        virtual std::string toString() = 0;

    protected:
        class SettablePositionMarker : public PositionMarker {
        public:
            SettablePositionMarker();
            int get() const override;
            bool isFinished();
            int getCached();
            int markFinished();
            void move(int offset);
            void set(int value);
            void commit();

        private:
            std::atomic<int> position;
            int cachedPosition;
        };

        std::shared_ptr<SettablePositionMarker> positionMarker;
        std::shared_ptr<Buffer> buffer;
        int maxCapacity;
    };

}


#endif // BUFFERBUILDER_H

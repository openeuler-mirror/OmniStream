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

#ifndef BUFFERCONSUMER_H
#define BUFFERCONSUMER_H

#include <memory>
#include <common.h>

#include "PositionMarker.h"
#include "ObjectBufferDataType.h"
#include "Buffer.h"

namespace omnistream {
    class BufferConsumer {
    public:

        class FixedSizePositionMarker : public PositionMarker {
        public:
            explicit FixedSizePositionMarker(int size) : size(size){};
            int get() const override
            {
                return size;
            };
        private:
            int size;
        };

        class CachedPositionMarker {
        public:
            CachedPositionMarker(const std::shared_ptr<PositionMarker>& positionMarker, int cachedPosition)
                : positionMarker(positionMarker),
                  cachedPosition(cachedPosition)
            {
                LOG_TRACE("consturctur")
            }

            explicit CachedPositionMarker(std::shared_ptr<PositionMarker> positionMarker) : positionMarker(
                positionMarker)
            {
                update();
                LOG_TRACE("consturctur2")
            }

            bool isFinished() const
            {
                return PositionMarker::isFinished(cachedPosition);
            }

            int getCached()
            {
                return PositionMarker::getAbsolute(cachedPosition);
            }

            int getLatest()
            {
                return PositionMarker::getAbsolute(positionMarker->get());
            }

            void update()
            {
                cachedPosition = positionMarker->get();
            }

            void selfCheck()
            {
                LOG_TRACE("selfCheck  "  << std::to_string(positionMarker.use_count()))
            }

            ~CachedPositionMarker()
            {
                LOG_TRACE("~CachedPositionMarker ")
                selfCheck();
            }
        private:
            std::shared_ptr<PositionMarker> positionMarker;
            int cachedPosition;
        };

        BufferConsumer(std::shared_ptr<Buffer> buffer_,
            std::shared_ptr<PositionMarker> writerPosition, int currentReaderPosition)
            : buffer(buffer_), writerPosition(writerPosition), currentReaderPosition(currentReaderPosition) {
        };

        virtual ~BufferConsumer() = default;

        bool isFinished() const;
        virtual std::shared_ptr<Buffer> build() = 0;
        void skip(int bytesToSkip);
        virtual std::shared_ptr<BufferConsumer> copy() = 0;
        virtual std::shared_ptr<BufferConsumer> copyWithReaderPosition(int readerPosition) = 0;
        bool isBuffer() const;
        ObjectBufferDataType getDataType() const;
        void close();
        bool isRecycled() const;
        int getWrittenBytes();
        int getCurrentReaderPosition() const;
        virtual bool isStartOfDataBuffer() const = 0;
        int getBufferSize() const;
        bool isDataAvailable();
        virtual std::string toDebugString(bool includeHash) = 0;
        virtual std::string toString() = 0;
        int getBufferType();

    protected:
        std::shared_ptr<Buffer> buffer;
        CachedPositionMarker writerPosition;
        int currentReaderPosition;
    };
}


#endif // BUFFERCONSUMER_H

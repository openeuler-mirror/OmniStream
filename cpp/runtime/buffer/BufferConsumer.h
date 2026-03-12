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
            explicit CachedPositionMarker(PositionMarker *positionMarker) : positionMarker(
                positionMarker)
            {
                update();
                LOG_TRACE("consturctur2")
            }

            inline bool isFinished() const
            {
                return PositionMarker::isFinished(cachedPosition);
            }

            inline int getCached()
            {
                return PositionMarker::getAbsolute(cachedPosition);
            }

            inline int getLatest()
            {
                return PositionMarker::getAbsolute(positionMarker->get());
            }

            inline void update()
            {
                cachedPosition = positionMarker->get();
            }

            inline void selfCheck()
            {
                LOG_TRACE("selfCheck  "  << std::to_string(positionMarker->get()))
            }

            ~CachedPositionMarker()
            {
                LOG_TRACE("~CachedPositionMarker ")
                selfCheck();
                if (positionMarker) {
                    positionMarker->release();
                    positionMarker = nullptr;
                }
            }

            inline PositionMarker *getInnerPositionMarker()
            {
                return positionMarker;
            }
        private:
            PositionMarker *positionMarker;
            int cachedPosition;
        };

        BufferConsumer(Buffer* buffer_,
                       PositionMarker *writerPosition, int currentReaderPosition)
            : buffer(buffer_), currentReaderPosition(currentReaderPosition) {
            this->writerPosition = new CachedPositionMarker(writerPosition);
        };

        // delete in BufferConsumerWithPartialRecordLength
        virtual ~BufferConsumer() {
            if (buffer->RefCount() == 0) {
                delete buffer;
            }
            delete writerPosition;
        }

        bool isFinished() const;
        virtual Buffer *build() = 0;

        Buffer* buildForPeek() {
            int oldReaderPos = currentReaderPosition;
            auto built = build();
            currentReaderPosition = oldReaderPos;
            return built;
        }

        void skip(int bytesToSkip);
        virtual std::shared_ptr<BufferConsumer> copy() = 0;
        virtual std::shared_ptr<BufferConsumer> copyWithReaderPosition(int readerPosition) = 0;
        bool isBuffer() const;
        ObjectBufferDataType getDataType() const;
        // Used by timeout->UC to convert a timeoutable aligned barrier into a priority event.
        void SetDataType(const ObjectBufferDataType& dataType);
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
        Buffer* buffer = nullptr;
        CachedPositionMarker *writerPosition = nullptr;
        int currentReaderPosition;
    };
}


#endif // BUFFERCONSUMER_H

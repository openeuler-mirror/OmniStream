/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BUFFERCONSUMER_H
#define OMNISTREAM_BUFFERCONSUMER_H

#include <memory>
#include <string>
#include "ObjectBuffer.h"
#include "ObjectBufferDataType.h"

#include "PositionMarker.h"
#include "VectorBatchBuffer.h"

namespace omnistream {

    class ObjectBufferConsumer {
    public:
        class CachedPositionMarker {
        public:
            CachedPositionMarker(const std::shared_ptr<PositionMarker>& positionMarker, int cachedPosition)
                : positionMarker(positionMarker),
                  cachedPosition(cachedPosition)
            {
                LOG_TRACE("consturctur")
            }

            explicit CachedPositionMarker(std::shared_ptr<PositionMarker> positionMarker) : positionMarker(
                positionMarker) {
                update();
                LOG_TRACE("consturctur2")
            }

            bool isFinished() const{
                return PositionMarker::isFinished(cachedPosition);
            }

            int getCached() {
                return PositionMarker::getAbsolute(cachedPosition);
            }

            int getLatest() {
                return PositionMarker::getAbsolute(positionMarker->get());
            }

            void update() {
                cachedPosition = positionMarker->get();
            }

            void selfCheck()
            {
                LOG_TRACE("selfCheck  "  << std::to_string(positionMarker.use_count()) )
            }

            ~CachedPositionMarker()
            {
                LOG_TRACE("~CachedPositionMarker " )
                selfCheck();
            }
        private:
            std::shared_ptr<PositionMarker> positionMarker;
            int cachedPosition;
        };

        ObjectBufferConsumer(std::shared_ptr<VectorBatchBuffer> buffer, int size);
        ObjectBufferConsumer(std::shared_ptr<VectorBatchBuffer> buffer, std::shared_ptr<PositionMarker> currentWriterPosition, int currentReaderPosition);

        ~ObjectBufferConsumer();

        bool isFinished() const;
        std::shared_ptr<VectorBatchBuffer> build();
        void skip(int bytesToSkip);
        std::shared_ptr<ObjectBufferConsumer> copy() ;
        std::shared_ptr<ObjectBufferConsumer> copyWithReaderPosition(int readerPosition) ;
        bool isBuffer() const;
        bool isEvent() const;
        ObjectBufferDataType getDataType() const;
        void close();
        bool isRecycled() const;
        int getWrittenBytes() ;
        int getCurrentReaderPosition() const;
        bool isStartOfDataBuffer() const;
        int getBufferSize() const;
        bool isDataAvailable() ;
        std::string toDebugString(bool includeHash) ;
        std::string    toString();
        int getBufferType();

    private:
        std::shared_ptr<VectorBatchBuffer> buffer;
        CachedPositionMarker writerPosition;
        int currentReaderPosition;
    };

} // namespace omnistream

#endif // OMNISTREAM_BUFFERCONSUMER_H
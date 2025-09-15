/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BUFFERBUILDER_H
#define OMNISTREAM_BUFFERBUILDER_H

#include <memory>
#include <string>
#include <vector>
#include <atomic>

#include "ObjectBuffer.h"
#include "ObjectBufferConsumer.h"
#include "ObjectBufferRecycler.h"
#include "ObjectSegment.h"
#include "PositionMarker.h"

namespace omnistream {

class ObjectBufferBuilder {
public:
    ObjectBufferBuilder(std::shared_ptr<ObjectSegment> objSegment, std::shared_ptr<ObjectBufferRecycler> recycler);
    ~ObjectBufferBuilder();

    std::shared_ptr<ObjectBufferConsumer> createBufferConsumer();
    std::shared_ptr<ObjectBufferConsumer> createBufferConsumerFromBeginning();
    int appendAndCommit(void* source);
    int append(void* source);
    void commit();
    int finish();
    bool isFinished();
    bool isFull();

    int getMaxCapacity();
    void trim(int newSize);

    void close();
    std::string toString() ;

    // for test
    StreamElement* getObject(int index);
private:

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

    std::shared_ptr<ObjectSegment> objSegment;
    std::shared_ptr<VectorBatchBuffer> buffer;
    int maxCapacity;
    bool bufferConsumerCreated;
    std::shared_ptr<SettablePositionMarker> positionMarker;

    std::shared_ptr<ObjectBufferConsumer> createBufferConsumer(int currentReaderPosition);
};

} // namespace omnistream

#endif // OMNISTREAM_BUFFERBUILDER_H
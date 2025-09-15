#ifndef FLINK_TNEL_MEMORYSEGMENTFACTORY_H
#define FLINK_TNEL_MEMORYSEGMENTFACTORY_H

#include "MemorySegment.h"

class MemorySegmentFactory {
public:

    static MemorySegment*  wrap(uint8_t *buffer, int size);
};


#endif //FLINK_TNEL_MEMORYSEGMENTFACTORY_H


//**
/*
 *   public static MemorySegment wrap(byte[] buffer) {
        return new MemorySegment(buffer, null);
    }
 */

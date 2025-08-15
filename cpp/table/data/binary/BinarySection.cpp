//
// Created by hudsonsheng on 9/4/24.
//

#include "BinarySection.h"
#include "core/include/common.h"
#include "core/task/StreamTask.h"

/**
 * public final void pointTo(MemorySegment segment, int offset_, int sizeInBytes) {
//        pointTo(new MemorySegment[] {segment}, offset_, sizeInBytes);
//    }
//
 * @param segment
 * @param offset
 * @param sizeInBytes
 */

// void BinarySection::pointTo(MemorySegment *segment, int offset, int sizeInBytes) {
//      // NOT_IMPL_EXCEPTION
//     //todo maybe need to be optimized later.
//     auto temp = new MemorySegment * [1];
//     temp[0] = segment;
//     pointTo(temp, 1, offset, sizeInBytes);
// }

// void BinarySection::pointTo(MemorySegment **segments, int numSegments, int offset, int sizeInBytes) {
//     segments_ = segments;
//     numSegments_ = numSegments;
//     offset_ = offset;
//     sizeInBytes_ = sizeInBytes;
// }

BinarySection::~BinarySection() {
    LOG("destructor  BinarySection");
    if (owner_ == 1 && memoryBuffer && bufferCapacity > 0) {
        delete[] memoryBuffer;
    }
}

void BinarySection::pointTo(uint8_t *segment, int offset, int sizeInBytes,int bufferCapacity) {
    memoryBuffer = segment;
    offset_ = offset;
    sizeInBytes_ = sizeInBytes;
    this->bufferCapacity = bufferCapacity;
}

void BinarySection::own(uint8_t *segment, int offset, int sizeInBytes,int bufferCapacity) {
    memoryBuffer = segment;
    offset_ = offset;
    sizeInBytes_ = sizeInBytes;
    this->bufferCapacity = bufferCapacity;
    owner_ = 1;
}

int BinarySection::getSizeInBytes() const {
    return sizeInBytes_;
}

// MemorySegment **BinarySection::getSegments() const {
//     return segments_;
// }

int BinarySection::getOffset() const {
    return offset_;
}

// int BinarySection::getNumSegments() const {
//     return numSegments_;
// }

uint8_t *BinarySection::getSegment() const {
    return memoryBuffer;
}

int BinarySection::getBufferCapacity() const {
    return bufferCapacity;
}
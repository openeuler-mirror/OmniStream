//
// Created by root on 9/15/24.
//

#include "BinaryRowWriter.h"
// #include "../binary/BinarySegmentUtils.h"
// #include "../../../core/memory/MemorySegmentFactory.h"
#include "../../../core/memory/MemorySegmentUtils.h"

BinaryRowWriter::BinaryRowWriter(BinaryRowData *row,  int initialSize) : row_(row) {
    nullBitsSizeInBytes_ = BinaryRowData::calculateBitSetWidthInBytes(row->getArity());
    fixedSize_ = row->getFixedLengthPartSize();
    cursor_ = fixedSize_;

    int buffer_size = fixedSize_ + initialSize;

    // segment_ = MemorySegmentFactory::wrap(new uint8_t [buffer_size], buffer_size);
    // row_ = row;
    memoryBuffer = new uint8_t[buffer_size];
    
    row_->pointTo(memoryBuffer, 0, buffer_size,buffer_size);
    // row_->pointTo(segment_, 0, segment_->GetSize());

}

BinaryRowWriter::BinaryRowWriter(BinaryRowData *row) : BinaryRowWriter(row, 0) {}

void BinaryRowWriter::writeLong(int pos, long value) {
    // row_->setLong(pos, value);
//    segment_->putLong(getFieldOffset(pos),value);
      MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), value);

}

void BinaryRowWriter::writeRowKind(RowKind kind) {

    // segment_->put(0, static_cast<std::uint8_t>(kind));
    row_->setRowKind(kind);
}

void BinaryRowWriter::writeInt(int pos, int value) {
    // segment_->putInt(getFieldOffset(pos), value);
    MemorySegmentUtils::putInt(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), value);
}
// void BinaryRowWriter::writeRowKind(RowKind *kind) {
//     row_->setRowKind(*kind);
//     // segment_->put(0, kind->toByteValue());
// }

void BinaryRowWriter::reset() {
    cursor_ = fixedSize_;
    for (int i = 0; i < nullBitsSizeInBytes_; i += 8) {
        row_->setLong(i, 0L);
        MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), i, 0L);
        // segment_->putLong(i, 0L);
    }
}

void BinaryRowWriter::setNullAt(int pos) {
    setNullBit(pos);
    // row_->setLong(pos, 0L);
    // segment_->putLong(getFieldOffset(pos), 0L);
    MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), 0L);
}

void BinaryRowWriter::setNullBit(int pos) {
    row_->setNullAt(pos);
    // BinarySegmentUtils::bitSet(segment_, 0, pos + BinaryRowData::HEADER_SIZE_IN_BITS);
}

int BinaryRowWriter::getFieldOffset(int pos) {
    return   nullBitsSizeInBytes_ + 8 * pos;
}

void BinaryRowWriter::complete() {
    row_->setSizeInBytes(cursor_);
}

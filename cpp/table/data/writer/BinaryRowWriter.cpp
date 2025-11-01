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

#include "BinaryRowWriter.h"
#include "../../../core/memory/MemorySegmentUtils.h"

BinaryRowWriter::BinaryRowWriter(BinaryRowData *row,  int initialSize) : row_(row)
{
    nullBitsSizeInBytes_ = BinaryRowData::calculateBitSetWidthInBytes(row->getArity());
    fixedSize_ = row->getFixedLengthPartSize();
    cursor_ = fixedSize_;

    int buffer_size = fixedSize_ + initialSize;

    memoryBuffer = new uint8_t[buffer_size];
    
    row_->pointTo(memoryBuffer, 0, buffer_size, buffer_size);
}

BinaryRowWriter::BinaryRowWriter(BinaryRowData *row) : BinaryRowWriter(row, 0) {}

void BinaryRowWriter::writeLong(int pos, long value)
{
    MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), value);
}

void BinaryRowWriter::writeRowKind(RowKind kind)
{
    row_->setRowKind(kind);
}

void BinaryRowWriter::writeInt(int pos, int value)
{
    MemorySegmentUtils::putInt(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), value);
}


void BinaryRowWriter::reset()
{
    cursor_ = fixedSize_;
    for (int i = 0; i < nullBitsSizeInBytes_; i += 8) {
        row_->setLong(i, 0L);
        MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), i, 0L);
    }
}

void BinaryRowWriter::setNullAt(int pos)
{
    setNullBit(pos);
    MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), 0L);
}

void BinaryRowWriter::setNullBit(int pos)
{
    row_->setNullAt(pos);
}

int BinaryRowWriter::getFieldOffset(int pos)
{
    return   nullBitsSizeInBytes_ + 8 * pos;
}

void BinaryRowWriter::complete()
{
    row_->setSizeInBytes(cursor_);
}

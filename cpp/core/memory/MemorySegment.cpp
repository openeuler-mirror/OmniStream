/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "MemorySegment.h"
#include <huawei_secure_c/include/securec.h>
#include <cstring>
#include <stdexcept>

#include "../include/common.h"

MemorySegment::MemorySegment(uint8_t* offHeapBuffer, int size) : offHeapBuffer_(offHeapBuffer), size_(size), owner_(nullptr) {
}

void MemorySegment::getResData(void* dst, void* src, size_t cur, int res)
{
    svbool_t pg = svwhilelt_b8(0, res);
    svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
    svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
}

MemorySegment::MemorySegment(int size) : size_(size)
{
    offHeapBuffer_ = new uint8_t[size];
}

uint8_t* MemorySegment::getAll()
{
    return offHeapBuffer_;
}

uint8_t MemorySegment::get(int index)
{
    if (index >= 0 && index < size_) {
        return *(offHeapBuffer_ + index);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

/**
 * Write a single `byte` to the memory segment at the specified `index`
 */
void MemorySegment::put(int index, uint8_t byte)
{
    if (index >= 0 && index < size_) {
        *(offHeapBuffer_ + index) = byte;
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

/**
 * Write a sequence of bytes `src` of `length` to the memory segment at the specified `index`
 */
void MemorySegment::put(int index, const uint8_t* src, int offset, int length)
{
    if (index >= 0 && index <= size_ - length) {
        void* pos = static_cast<void*>(offHeapBuffer_ + index);

        size_t skip_num = svcntb();
        size_t num = length / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, const_cast<uint8_t*>(src + offset) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(pos) + cur, s);
            cur += skip_num;
        }
        getResData(pos, const_cast<uint8_t*>(src + offset), cur, length - cur);

    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        throw std::out_of_range("Index out of bound");
    }
}

int MemorySegment::getSize() const
{
    return size_;
}

/**
 * Bulk get method. Copies a sequence of bytes of length `length` from the memory segment starting at the specified index to the destination memory `dst`, beginning at the given `offset`
 */
void MemorySegment::get(int index, uint8_t* dst, int offset, int length)
{
    if (index >= 0 && index <= size_ - length) {
        size_t skip_num = svcntb();
        size_t num = length / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, const_cast<uint8_t*>(offHeapBuffer_ + index) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(dst + offset) + cur, s);
            cur += skip_num;
        }
        getResData(dst + offset, const_cast<uint8_t*>(offHeapBuffer_ + index), cur, length - cur);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }

}

int MemorySegment::size()
{
    return size_;
}

long* MemorySegment::getLong(int index)
{

    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(long)) {
        return reinterpret_cast<long*>(offHeapBuffer_ + index);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }

}

void MemorySegment::putLong(int index, long value)
{
    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(long)) {
        size_t skip_num = svcntb();
        size_t num = sizeof(long) / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&value) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(offHeapBuffer_ + index) + cur, s);
            cur += skip_num;
        }
        getResData(offHeapBuffer_ + index, &value, cur, sizeof(long) - cur);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
    
}

int* MemorySegment::getInt(int index)
{
    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(int)) {
        return reinterpret_cast<int*>(offHeapBuffer_ + index);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
    
}

void MemorySegment::putInt(int index, int value)
{
    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(int)) {
        size_t len = sizeof(int);
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&value) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(offHeapBuffer_ + index) + cur, s);
            cur += skip_num;
        }
        getResData(offHeapBuffer_ + index, &value, cur, len - cur);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

/**
 * Equals two memory segment regions.
 *
 * @param seg2 Segment to equal this segment with
 * @param offset1 Offset of this segment to start equaling
 * @param offset2 Offset of seg2 to start equaling
 * @param length Length of the equaled memory region
 * @return true if equal, false otherwise
 */
bool MemorySegment::equalTo(MemorySegment seg2, int offset1, int offset2, int length)
{

    return (memcmp((offHeapBuffer_ + offset1), (seg2.offHeapBuffer_ + offset2), length) == 0);
}
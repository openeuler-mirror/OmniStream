#ifndef MEMORYSEGMENTUTILS_H
#define MEMORYSEGMENTUTILS_H
#include <huawei_secure_c/include/securec.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <arm_sve.h>

class MemorySegmentUtils
{
public:
    static const int ADDRESS_BITS_PER_WORD = 3;
    static const int BIT_BYTE_INDEX_MASK = 7;

    static void getResData(void* dst, void* src, size_t cur, int res)
    {
        svbool_t pg = svwhilelt_b8(0, res);
        svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
        svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
    }
    
    static inline uint8_t *getAll(uint8_t *offHeapBuffer)
    {
        return offHeapBuffer;
    }

    static inline uint8_t get(uint8_t *offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index < size)
        {
            return *(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void put(uint8_t *offHeapBuffer, int size, int index, uint8_t byte)
    {
        if (index >= 0 && index < size)
        {
            *(offHeapBuffer + index) = byte;
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void put(uint8_t *offHeapBuffer, int size, int index, const uint8_t *src, int offset, int length)
    {
        if (index >= 0 && index <= size - length)
        {
            void *pos = static_cast<void *>(offHeapBuffer + index);
            size_t len = length;
            size_t skip_num = svcntb();
            size_t num = len / skip_num;
            svbool_t pTrue = svptrue_b8();
            size_t cur = 0;
            for (size_t i = 0; i < num; i++)
            {
                svuint8_t s = svld1(pTrue, const_cast<uint8_t*>(src) + offset + cur);
                svst1(pTrue, reinterpret_cast<uint8_t*>(pos) + cur, s);
                cur += skip_num;
            }
            getResData(pos, const_cast<uint8_t*>(src) + offset, cur, len - cur);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::out_of_range("Index out of bound");
        }
    }

    static inline int getSize(int size)
    {
        return size;
    }

    static inline void get(uint8_t *offHeapBuffer, int size, int index, uint8_t *dst, int offset, int length)
    {
        if (index >= 0 && index <= size - length)
        {
            void *srcPos = static_cast<void *>(offHeapBuffer + index);
            void *dstPos = static_cast<void *>(dst + offset);

            size_t len = length;
            size_t skip_num = svcntb();
            size_t num = len / skip_num;
            svbool_t pTrue = svptrue_b8();
            size_t cur = 0;
            for (size_t i = 0; i < num; i++)
            {
                svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(srcPos) + cur);
                svst1(pTrue, reinterpret_cast<uint8_t*>(dstPos) + cur, s);
                cur += skip_num;
            }
            getResData(dstPos, srcPos, cur, len - cur);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::out_of_range("Index out of bound");
        }
    }

    static inline int size(int size)
    {
        return size;
    }

    static inline long *getLong(uint8_t *offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index <= size -  static_cast<int>(sizeof(long)))
        {
            return reinterpret_cast<long *>(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void putLong(uint8_t *offHeapBuffer, int size, int index, long value)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(long)))
        {
            size_t len = sizeof(long);
            size_t skip_num = svcntb();
            size_t num = len / skip_num;
            svbool_t pTrue = svptrue_b8();
            size_t cur = 0;
            for (size_t i = 0; i < num; i++)
            {
                svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&value) + cur);
                svst1(pTrue, reinterpret_cast<uint8_t*>(offHeapBuffer + index) + cur, s);
                cur += skip_num;
            }
            getResData(offHeapBuffer + index, &value, cur, len - cur);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline bool *getBool(uint8_t *offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(bool))) {
            return reinterpret_cast<bool *>(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void putBool(uint8_t *offHeapBuffer, int size, int index, bool value)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(bool))) {
            size_t len = sizeof(bool);
            size_t skip_num = svcntb();
            size_t num = len / skip_num;
            svbool_t pTrue = svptrue_b8();
            size_t cur = 0;
            for (size_t i = 0; i < num; i++)
            {
                svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&value) + cur);
                svst1(pTrue, reinterpret_cast<uint8_t*>(offHeapBuffer + index) + cur, s);
                cur += skip_num;
            }
            getResData(offHeapBuffer + index, &value, cur, len - cur);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }


    static inline int *getInt(uint8_t *offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(int)))
        {
            return reinterpret_cast<int *>(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void putInt(uint8_t *offHeapBuffer, int size, int index, int value)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(int)))
        {
            size_t len = sizeof(int);
            size_t skip_num = svcntb();
            size_t num = len / skip_num;
            svbool_t pTrue = svptrue_b8();
            size_t cur = 0;
            for (size_t i = 0; i < num; i++)
            {
                svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&value) + cur);
                svst1(pTrue, reinterpret_cast<uint8_t*>(offHeapBuffer + index) + cur, s);
                cur += skip_num;
            }
            getResData(offHeapBuffer + index, &value, cur, len - cur);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline bool equalTo(uint8_t *offHeapBuffer1, uint8_t *offHeapBuffer2, int offset1, int offset2, int length)
    {
        return (std::memcmp(offHeapBuffer1 + offset1, offHeapBuffer2 + offset2, length) == 0);
    }

    static inline int byteIndex(int bitIndex)
    {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

    static inline void bitUnSet(uint8_t *offHeapBuffer, int size, int baseOffset, int index)
    {
        int offset = baseOffset + byteIndex(index);
        uint8_t current = get(offHeapBuffer, size, offset);
        current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
        put(offHeapBuffer, size, offset, current);
    }

    static inline void bitSet(uint8_t *offHeapBuffer, int size, int baseOffset, int index)
    {
        int offset = baseOffset + byteIndex(index);
        uint8_t current = get(offHeapBuffer, size, offset);
        current |= (1 << (index & BIT_BYTE_INDEX_MASK));
        put(offHeapBuffer, size, offset, current);
    }
    
    static inline void copy(uint8_t *source, int source_offset, uint8_t * dest, int dest_offset, int copy_len)
    {
        size_t len = copy_len;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(source + source_offset) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(dest + dest_offset) + cur, s);
            cur += skip_num;
        }
        getResData(dest + dest_offset,source + source_offset, cur, len - cur);
    }
};

#endif // MEMORYSEGMENTUTILS_H
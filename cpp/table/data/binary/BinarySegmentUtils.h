#ifndef FLINK_TNEL_BINARYSEGMENTUTILS_H
#define FLINK_TNEL_BINARYSEGMENTUTILS_H


#include <iostream>
#include <sstream>
#include "MurmurHashUtils.h"
#include "../../../core/memory/MemorySegmentUtils.h"
#include <map>
#include "table/data/TimestampData.h"
#include "table/data/binary/BinaryStringData.h"

class BinarySegmentUtils {
public:


// static  std::map<int, uint8_t *> BYTES_LOCAL;
// static  std::map<int, int> BYTES_LOCAL_LENGTH;

//todo in java this a threadlocal thing,but here we use a map to simulate it. is it correct?
static inline uint8_t * allocateReuseBytes(int length)
{
    uint8_t *bytes = BYTES_LOCAL[0];

    if (bytes == nullptr)
    {
        if (length <= MAX_BYTES_LENGTH)
        {
            bytes = new uint8_t[MAX_BYTES_LENGTH];
            BYTES_LOCAL[0] = bytes;
            BYTES_LOCAL_LENGTH[0] = MAX_BYTES_LENGTH;
        }
        else
        {
            bytes = new uint8_t[length]; // TBD how to delete
            BYTES_LOCAL[0] = bytes;
            BYTES_LOCAL_LENGTH[0] = length;
        }
    }
    else if (MAX_BYTES_LENGTH < length)
    {

        bytes = new uint8_t[length]; // TBD how to delete
        BYTES_LOCAL[0] = bytes;
        BYTES_LOCAL_LENGTH[0] = length;
    } //  else TBD
    return bytes;
}

// TODO need to implement
static inline void copyToView(uint8_t *segment, int segmentSize, int offset, int sizeInBytes, DataOutputSerializer &target)
{

        target.write(segment, segmentSize, offset, sizeInBytes);

}

static inline void bitUnSet(uint8_t *offHeapBuffer, int baseOffset, int index, int size)
{
    int offset = baseOffset + byteIndex(index);
    uint8_t current = MemorySegmentUtils::get(offHeapBuffer, size, offset);
    current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
    MemorySegmentUtils::put(offHeapBuffer, size, offset, current);
}

static inline void bitSet(uint8_t *offHeapBuffer, int baseOffset, int index, int size)
{
    int offset = baseOffset + byteIndex(index);
    uint8_t current = MemorySegmentUtils::get(offHeapBuffer, size, offset);
    current |= (1 << (index & BIT_BYTE_INDEX_MASK));
     MemorySegmentUtils::put(offHeapBuffer, size, offset, current);
}


static inline  long getLong(uint8_t *segment, int segmentSize, int offset)
{
    return * MemorySegmentUtils::getLong(segment,segmentSize ,offset);
}


static inline void setLong(uint8_t *segments,int segmentSize, int offset, long value)
{
    MemorySegmentUtils::putLong(segments,segmentSize, offset, value);
}


static inline TimestampData * readTimestampData(uint8_t *segment,int sigmentSize, int baseOffset, long offsetAndNanos)
{
    int nanoOfMillisecond = (int)offsetAndNanos;
    int subOffset = (int)(offsetAndNanos >> 32);
    long millisecond = getLong(segment, sigmentSize,baseOffset + subOffset);
    return TimestampData::fromEpochMillis(millisecond, nanoOfMillisecond);
}


static inline  int byteIndex(int bitIndex)
{
    return bitIndex >> ADDRESS_BITS_PER_WORD;
}


static inline bool bitGet(uint8_t *segment, int segmentSize,int baseOffset, int index)
{
    int offset = baseOffset + byteIndex(index);
    uint8_t current = MemorySegmentUtils::get(segment,segmentSize,offset);
    return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
}

/**
 * Read string data from the given segment
 *
 * @param segment the memory segment
 * @param baseOffset the base offset of `BinaryRowData`
 * @param fieldOffset the field offset of the current column
 * @param variablePartOffsetAndLen
 *    - If `mark` is set, the lower 32 bits represent `subOffset` and the higher 32 bits represent `len`. E.g. `variablePartOffsetAndLen` with underlying memory `08 00 00 00 28 00 00 00`, has `subOffset` = 40 and `len` = 8
 *    - If `mark` is not set, the higher 56 bits represent the content and the lowest 4 bits represent `len`. Thus, the variable name "variablePartOffsetAndLen" is not accurate in this case
 *
 * Memory layout of VARCHAR column varies depending on the length of the content. Details of sees this [wiki](https://codehub-y.huawei.com/data-app-lab/OmniFlink/wiki?categoryId=149234&sn=WIKI202410104753613)
 *
 * Setter sees `setString` in "OmniFlink/cpp/table/data/binary/BinaryRowData.cpp"
 */

//todo need to implement
static inline  BinaryStringData * readStringData(uint8_t *segment, int baseOffset, int fieldOffset, int64_t variablePartOffsetAndLen)
{
    int64_t mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
    if (mark == 0)
    { // VARCHAR more than 7 characters
        int subOffset = static_cast<int>(variablePartOffsetAndLen >> 32);
        int len = static_cast<int>(variablePartOffsetAndLen);
        // return BinaryStringData::fromAddress(segment, baseOffset + subOffset, len);
        return BinaryStringData::fromBytes(segment, baseOffset + subOffset, len);
    }
    else
    { // VARCHAR less than or equal to 7 characters
        int len = static_cast<int>((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56);
        if (LITTLE_ENDIAN)
        {
            return BinaryStringData::fromBytes(segment, fieldOffset, len);
        }
        else
        {
            // fieldOffset + 1 to skip header.
            return BinaryStringData::fromBytes(segment, fieldOffset + 1, len);
        }
    }
}

static std::string_view readStringView(uint8_t *segment, int baseOffset, int fieldOffset,
    int64_t variablePartOffsetAndLen)
{
    int64_t mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
    if (mark == 0) {
        // VARCHAR more than 7 characters
        int subOffset = static_cast<int>(variablePartOffsetAndLen >> 32);
        int len = static_cast<int>(variablePartOffsetAndLen);
        // return BinaryStringData::fromAddress(segment, baseOffset + subOffset, len);
        return std::string_view(reinterpret_cast<const char *>(segment) + baseOffset + subOffset, len);
    } else {
        // VARCHAR less than or equal to 7 characters
        int len = static_cast<int>((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56);
        if (LITTLE_ENDIAN) {
            return std::string_view(reinterpret_cast<const char *>(segment) + fieldOffset, len);
        } else {
            // fieldOffset + 1 to skip header.
            return std::string_view(reinterpret_cast<const char *>(segment) + fieldOffset + 1, len);
        }
    }
}

/**
 * hash segments to int, numBytes must be aligned to 4 bytes.
 *
 * @param segments Source segments.
 * @param offset Source segments offset.
 * @param numBytes the number bytes to hash.
 */

static inline int hashByWords(uint8_t *segment, int offset, int numBytes)
{
        return MurmurHashUtils::hashBytesByWords(segment, offset, numBytes);

}

static inline int hashBytes(uint8_t *segment, int offset, int numBytes)
{
    return MurmurHashUtils::hashBytes(segment, offset, numBytes);

}



/**
 * Equals two memory segments regions.
 *
 * @param segments1 Segments 1
 * @param offset1 Offset of segments1 to start equaling
 * @param segments2 Segments 2
 * @param offset2 Offset of segments2 to start equaling
 * @param len Length of the equaled memory region
 * @return true if equal, false otherwise
 */
static inline bool equals(uint8_t *segments1, int offset1, uint8_t *segments2, int offset2, int len)
{
    return std::memcmp(segments1 + offset1, segments2 + offset2, len) == 0;
}


private:
    static std::map<int, uint8_t *> BYTES_LOCAL;
    static std::map<int, int> BYTES_LOCAL_LENGTH;
    static const int MAX_BYTES_LENGTH = 1024 * 64;
    static const int BIT_BYTE_INDEX_MASK = 7;
    static const int ADDRESS_BITS_PER_WORD = 3;
    static const int64_t HIGHEST_FIRST_BIT = 0x80L << 56;
    static const int64_t HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;
    // TODO: Shouldnt this be 0 since we use a raw array
    static const int BYTE_ARRAY_BASE_OFFSET = 0;

    static int hashMultiSegByWords(uint8_t *segments, int numSegments, int offset, int numBytes);
    static bool inFirstSegment(uint8_t *segments, int offset, int numBytes);
};


#endif //FLINK_TNEL_BINARYSEGMENTUTILS_H

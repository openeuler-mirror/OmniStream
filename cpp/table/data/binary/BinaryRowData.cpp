//
// Created by root on 9/5/24.
//
#include <iostream>
#include "BinaryRowData.h"
#include "core/memory/MemorySegmentUtils.h"
#include "core/task/StreamTask.h"

BinaryRowData::BinaryRowData(int arity) : RowData(RowData::BinaryRowDataID), BinarySection(), arity_(arity)

{
    nullBitsSizeInBytes_ = calculateBitSetWidthInBytes(arity);
    types.resize(arity);
}

BinaryRowData::BinaryRowData(int arity, int length) : RowData(RowData::BinaryRowDataID), BinarySection(), arity_(arity)

{
    memoryBuffer = new uint8_t[length]();
    nullBitsSizeInBytes_ = calculateBitSetWidthInBytes(arity);
    types.resize(arity);
    bufferCapacity = length;
    sizeInBytes_ = length;
}

BinaryRowData::~BinaryRowData()
{
    LOG("destructor  BinaryRowData");
    // memoryBuffer is freed by ~BinarySection()
}

int BinaryRowData::getArity()
{
    return arity_;
}

void BinaryRowData::setRowKind(RowKind kind)
{
   
    MemorySegmentUtils::put(memoryBuffer, bufferCapacity, offset_, static_cast<std::uint8_t>(kind));
    // segments_[0]->put(offset_, kind.toByteValue());
}

bool BinaryRowData::isNullAt(int pos)
{
    return BinarySegmentUtils::bitGet(memoryBuffer, bufferCapacity, offset_, pos + HEADER_SIZE_IN_BITS);
}

long *BinaryRowData::getLong(int pos)
{
    return MemorySegmentUtils::getLong(memoryBuffer, bufferCapacity, getFieldOffset(pos));
}

bool *BinaryRowData::getBool(int pos)
{
    return MemorySegmentUtils::getBool(memoryBuffer, bufferCapacity, getFieldOffset(pos));
}

int BinaryRowData::calculateBitSetWidthInBytes(int arity)
{
    // return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
    return ((arity + 63 + HEADER_SIZE_IN_BITS) >>6) <<3;
}

int BinaryRowData::calculateFixPartSizeInBytes(int arity)
{
    // return calculateBitSetWidthInBytes(arity) + 8 * arity;
    return calculateBitSetWidthInBytes(arity) +  (arity<<3);
}

int BinaryRowData::getFixedLengthPartSize() const
{
    // return nullBitsSizeInBytes_ + 8 * arity_;
    return nullBitsSizeInBytes_ + (arity_ <<3 );
}

RowKind BinaryRowData::getRowKind()
{
    // uint8_t kindValue = segments_[0]->get(offset_);
    return static_cast<RowKind>(MemorySegmentUtils::get(memoryBuffer, bufferCapacity, offset_));

}

void BinaryRowData::setSizeInBytes(int sizeInBytes)
{
    sizeInBytes_ = sizeInBytes;
}

int BinaryRowData::getFieldOffset(int pos)
{
    // return offset_ + nullBitsSizeInBytes_ + pos * 8;
    return offset_ + nullBitsSizeInBytes_ + (pos << 3);
}

void BinaryRowData::setNotNullAt(int i)
{
    MemorySegmentUtils::bitUnSet(memoryBuffer,bufferCapacity, offset_, i + HEADER_SIZE_IN_BITS);
}

BinaryRowData *BinaryRowData::createBinaryRowDataWithMem(int arity)
{
    int length = calculateFixPartSizeInBytes(arity); // 1 is for the header byte
    auto binRow = new BinaryRowData(arity);
    auto *bytes = new uint8_t[length]();
    binRow->memoryBuffer = bytes;
    binRow->bufferCapacity = length;
    binRow->sizeInBytes_ = length;
    return binRow;
}

BinaryRowData *BinaryRowData::createRowFromSubJoinedRows(BinaryRowData * row1, BinaryRowData * row2 )
{
    int arity = row1->getArity() + row2->getArity();
    int length = calculateFixPartSizeInBytes(arity);
    auto binRow = new BinaryRowData(arity, length);
    for (int pos = 0; pos < arity; pos++) {
        BinaryRowData *subRow = pos < row1->getArity() ? row1 : row2;
        int posInSubRow = pos < row1->getArity() ? pos : pos - row1->getArity();
        bool isNull = subRow->isNullAt(posInSubRow);
        if (isNull) {
            binRow->setNullAt(pos);
        } else if (subRow->types[posInSubRow] < 2) {
            binRow->setLong(pos, subRow->getLong(posInSubRow));
            binRow->types[pos] = 1;
        } else {
            std::string_view sv = subRow->getStringView(posInSubRow);
            binRow->setStringView(pos, sv);
            binRow->types[pos] = 2;
        }
    }
    return binRow;
}

void BinaryRowData::setLong(int pos, long value)
{
    setNotNullAt(pos);
    MemorySegmentUtils::putLong(memoryBuffer, bufferCapacity, getFieldOffset(pos), value);
    types[pos] = 1;
    // segments_[0]->putLong(getFieldOffset(pos), value);
}

void BinaryRowData::setLong(int pos, long *value)
{
    if (value == nullptr)
    {
        setNullAt(pos);
    }
    else
    {
        setNotNullAt(pos);
        MemorySegmentUtils::putLong(memoryBuffer, bufferCapacity, getFieldOffset(pos), *value);
        types[pos] = 1;
    }
}

void BinaryRowData::setBool(int pos, bool value)
{
    setNotNullAt(pos);
    MemorySegmentUtils::putBool(memoryBuffer, bufferCapacity, getFieldOffset(pos), value);
    types[pos] = 1;
}

void BinaryRowData::setBool(int pos, bool *value)
{
    if (value == nullptr) {
        setNullAt(pos);
    } else {
        setNotNullAt(pos);
        MemorySegmentUtils::putBool(memoryBuffer, bufferCapacity, getFieldOffset(pos), *value);
        types[pos] = 1;
    }
}

void BinaryRowData::setNullAt(int pos)
{
    MemorySegmentUtils::bitSet(memoryBuffer,bufferCapacity, offset_, pos + HEADER_SIZE_IN_BITS);
}

TimestampData *BinaryRowData::getTimestamp(int pos)
{
    return TimestampData::fromEpochMillis(*(MemorySegmentUtils::getLong(memoryBuffer, bufferCapacity, getFieldOffset(pos))));
}

TimestampData *BinaryRowData::getTimestampPrecise(int pos)
{
    return TimestampData::fromEpochMillis(*(MemorySegmentUtils::getLong(memoryBuffer, bufferCapacity, getFieldOffset(pos))),
                *(MemorySegmentUtils::getInt(memoryBuffer, bufferCapacity, getFieldOffset(pos + 1))));
}


void BinaryRowData::setTimestamp(int pos, TimestampData &value, int precision)
{
    if (TimestampData::isCompact(precision))
    {
        setLong(pos, value.getMillisecond());
        types[pos] = 1;
    }
    else
    {
        setLong(pos, value.getMillisecond());
        types[pos] = 1;
        setInt(pos + 1, value.getNanoOfMillisecond());
        types[pos+1] = 1;
    }
}

int *BinaryRowData::getInt(int pos)
{
    return MemorySegmentUtils::getInt(memoryBuffer, bufferCapacity, getFieldOffset(pos));
}

void BinaryRowData::setInt(int pos, int value)
{
    setNotNullAt(pos);
    MemorySegmentUtils::putInt(memoryBuffer, bufferCapacity, getFieldOffset(pos), value);
    types[pos] = 1;
}

BinaryStringData *BinaryRowData::getString(int pos)
{
    int fieldOffset = getFieldOffset(pos);
    long offsetAndLen = *(MemorySegmentUtils::getLong(memoryBuffer, bufferCapacity, fieldOffset));
    return BinarySegmentUtils::readStringData(memoryBuffer, offset_, fieldOffset, offsetAndLen);
}

/**
 * Write VARCHAR column
 *
 * Memory layout of VARCHAR column varies depending on the length of the content. Details of sees this [wiki](https://codehub-y.huawei.com/data-app-lab/OmniFlink/wiki?categoryId=149234&sn=WIKI202410104753613)
 *
 * Implementation of this function is based on implementation of `writeString` in "/flink/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/data/writer/AbstractBinaryWriter.java". Implementation of the following utility functions are also based on java implementation in `AbstractBinaryWriter` class
 *
 * Getter sees `readStringData` in "OmniFlink/cpp/table/data/binary/BinarySegmentUtils.cpp"
 */
void BinaryRowData::setString(int pos, BinaryStringData *value)
{
    setNotNullAt(pos);
    int len = value->getSizeInBytes();
    if (len <= 7)
    {
        writeFixLenVarchar(getFieldOffset(pos), value->toBytes(), len);
        types[pos] = 1;
    }
    else
    {
        writeVarLenVarchar(getFieldOffset(pos), value->toBytes(), len);
        types[pos] = 2;
    }
}
std::string_view BinaryRowData::getStringView(int pos)
{
    int fieldOffset = getFieldOffset(pos);
    long offsetAndLen = *(MemorySegmentUtils::getLong(memoryBuffer, bufferCapacity, fieldOffset));
    return BinarySegmentUtils::readStringView(memoryBuffer, offset_, fieldOffset, offsetAndLen);
}
void BinaryRowData::setStringView(int pos, std::string_view value)
{
    static int varcharTypeFlag = 2;
    setNotNullAt(pos);
    auto len = value.size();
    if (len <= sizeof(int64_t) - 1) {
        writeFixLenVarchar(getFieldOffset(pos), reinterpret_cast<const uint8_t *>(value.data()), len);
        types[pos] = 1;
    } else {
        writeVarLenVarchar(getFieldOffset(pos), reinterpret_cast<const uint8_t *>(value.data()), len);
        types[pos] = varcharTypeFlag;
    }
}

/**
 * Write VARCHAR column that is less than or equal to 7 bytes
 */
void BinaryRowData::writeFixLenVarchar(int fieldOffset, const uint8_t *bytes, int len)
{
    uint64_t firstByte = len | 0x80; // first bit is 1, other bits is `len`, `len`'s first bit is never set
    uint64_t sevenBytes = 0L;        // real data
    if (LITTLE_ENDIAN)
    {
        for (int i = 0; i < len; i++)
        {
            sevenBytes |= ((0x00000000000000FFL & bytes[i]) << (i * 8L));
        }
    }
    else
    {
        for (int i = 0; i < len; i++)
        {
            sevenBytes |= ((0x00000000000000FFL & bytes[i]) << ((6 - i) * 8L));
        }
    }

    uint64_t offsetAndSize = (firstByte << 56) | sevenBytes;

    MemorySegmentUtils::putLong(memoryBuffer, bufferCapacity, fieldOffset, offsetAndSize);
}

/**
 * Write VARCHAR column that is longer than 7 bytes
 */
void BinaryRowData::writeVarLenVarchar(int fieldOffset, const uint8_t *bytes, int len)
{
    int roundedSize = getNumberOfBytesToNearestWord(len);
    int segmentSize = this->getSizeInBytes();

    // Write header
    setOffsetAndSize(fieldOffset, segmentSize, len);

    // Calculate the new buffer size
    int newBufferCapacity = bufferCapacity + roundedSize;

    // Allocate a new buffer with the increased size
    auto *newBuffer = new uint8_t[newBufferCapacity]();

    // Copy existing data to the new buffer
    auto ret = memcpy_s(newBuffer, newBufferCapacity, memoryBuffer, bufferCapacity);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }

    // Update the memoryBuffer pointer and bufferCapacity
    delete[] memoryBuffer;    // Free the old buffer

    memoryBuffer = newBuffer;
    bufferCapacity = newBufferCapacity;
    sizeInBytes_ = newBufferCapacity; // what is the difference between sizeInBytes_ and bufferCapacity?

    // Write the variable length portion
    MemorySegmentUtils::put(memoryBuffer, bufferCapacity, segmentSize, bytes, 0, len);
    // segments_[0]->put(segmentSize, bytes, 0, len);
    zeroOutPaddingBytes(segmentSize, len);

    //this->setSizeInBytes(segmentSize + len);
}

/**
 * Round the number of bytes to the nearest word (8 bytes)
 * @param numBytes number of bytes
 */
int BinaryRowData::getNumberOfBytesToNearestWord(int numBytes)
{
    int remainder = numBytes & 0x07;
    if (remainder == 0)
    {
        return numBytes;
    }
    else
    {
        return numBytes + (8 - remainder);
    }
}

/**
 * Zero out padding bytes to avoid random trailing data
 * @param fieldOffset offset of the variable length portion
 * @param numBytes number of bytes written to the variable length portion
 */
void BinaryRowData::zeroOutPaddingBytes(int fieldOffset, int numBytes)
{
    int remainder = numBytes & 0x07;
    if (remainder > 0)
    { // `numBytes` not a multiple of 8, need padding
        int paddingBytes = 8 - remainder;
        for (int i = 0; i < paddingBytes; i++)
        {
            MemorySegmentUtils::put(memoryBuffer, bufferCapacity, fieldOffset + numBytes + i, (uint8_t)0);
        }
    }
}

/**
 * @param headerOffset offset to write the header
 * @param varcharOffset offset of variable length VARCHAR that is written to header at `offset`
 * @param len length of variable length VARCHAR content that is written to header at `offset`
 */
void BinaryRowData::setOffsetAndSize(int headerOffset, int varcharOffset, int len)
{
    long offsetAndSize = (long)varcharOffset << 32 | (long)len;
    MemorySegmentUtils::putLong(memoryBuffer, bufferCapacity, headerOffset, offsetAndSize);
    // segments_[0]->putLong(headerOffset, offsetAndSize);
}

/**
 * @param other instance to compare to
 */

bool BinaryRowData::operator==(const RowData &other) const
{
    // Check for self-reference
    if (this == &other)
    {
        return true;
    }

    auto castedOther = static_cast<const BinaryRowData *>(&other);
    if (castedOther == nullptr)
    {
        // If it is not compared to a BinaryRowData return false directly
        return false;
    }
    // TODO: Need to add "&& typeid(other) != typeid(NestedRowData))"
    if (typeid(other) != typeid(BinaryRowData))
    {
        return false;
    }
    // if (this->hashCode() != castedOther->hashCode())
    // {
    //     return false;
    // }

    return sizeInBytes_ == castedOther->sizeInBytes_ &&
           BinarySegmentUtils::equals(memoryBuffer, offset_,
                                      castedOther->memoryBuffer, castedOther->offset_,
                                      sizeInBytes_);
}

/**
 * hash segments to int, numBytes must be aligned to 4 bytes.
 */
int BinaryRowData::hashCode() const {
    return BinarySegmentUtils::hashBytes(memoryBuffer, offset_, sizeInBytes_);
}

// for debug
void BinaryRowData::printSegInBinary() const
{
    // auto segPtr = segments_[0];
    auto bytePtr = memoryBuffer + offset_;
    for (int i = 0; i < sizeInBytes_; i++)
    {
        std::bitset<8> binary(*(bytePtr));
        std::cout << ":" << binary << " | ";
        std::cout << std::endl;
    }
}

RowData *BinaryRowData::copy()
{
    LOG("copy()  BinaryRowData");
    auto *newRow = new BinaryRowData(arity_);
    auto *bytes = new uint8_t[sizeInBytes_];
    MemorySegmentUtils::copy(memoryBuffer, offset_, bytes, 0, sizeInBytes_);
    
    newRow->own(bytes, 0, sizeInBytes_, bufferCapacity);
    newRow->types = types;
    return newRow;
}

int BinaryRowData::hashCodeFast() const {
    uint32_t hash = 42;
    for (int i = 0; i < sizeInBytes_; ++i) {
        hash ^= memoryBuffer[offset_ + i];
        hash *= 16777619u;
    }
    return hash;
}

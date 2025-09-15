#ifndef FLINK_TNEL_MEMORYSEGMENT_H
#define FLINK_TNEL_MEMORYSEGMENT_H

#include <cstdint>
#include <iostream>
#include <arm_sve.h>

// MemorySegment is a segment of mem.
// in cpp, MemorySegment may not be useful as in java.
// At this time, for convenience of migrating java code to cpp, MemorySegment is helpful
// Later when we  optimize the code, there is possibility to remove this class.
// ByteBuffer (core/utils/ByteBuffer) has very similar functionality. maybe MemorySegment can be alias of bytebuffer.

/**
 * Notice, MemorySegment always wrap the existing mem
 *
 */
class MemorySegment {
public:
    // wrap the existing mem
    MemorySegment(uint8_t* offHeapBuffer, int size);
    explicit MemorySegment(int size);

    uint8_t* getAll();

    // get byte at the index
    uint8_t get(int index);

    void put(int index, uint8_t b);
    void put(int index, const uint8_t* src, int offset, int length);

    int getSize() const;

    /**
     * Writes the given long value (64bit, 8 bytes) to the given position in the system's native
     * byte order.
     */
    void putLong(int index, long value);

    /**
     * Reads a long value (64bit, 8 bytes) from the given position, in the system's native byte
     * order.
     */
    long* getLong(int index);

    void putInt(int index, int value);
    int* getInt(int index);

    /**
     * Bulk get method. Copies length memory from the specified position to the destination memory,
     * beginning at the given offset.
     */
    void get(int index, uint8_t* dst, int offset, int length);

    int size();

    bool equalTo(MemorySegment seg2, int offset1, int offset2, int length);

    void getResData(void* dst, void* src, size_t cur, int res);

private:
    // so far, assume we only run with LITTLE ENDIAN cpu architecture
    static const bool cLittleEndian = true;

    uint8_t* offHeapBuffer_;
    int size_;

    // if the owner is null, the object itself own the offHeapBuffer, otherwise the owner_ own the offHeapBuffer.
    void* owner_;
};


#endif  // FLINK_TNEL_MEMORYSEGMENT_H

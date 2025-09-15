#ifndef FLINK_TNEL_ABSTRACTBINARYWRITER_H
#define FLINK_TNEL_ABSTRACTBINARYWRITER_H


#include "BinaryWriter.h"
// #include "../../../core/memory/MemorySegment.h"

class AbstractBinaryWriter : public BinaryWriter {
public:
    AbstractBinaryWriter() = default;
    virtual ~AbstractBinaryWriter() = default;
protected:

    virtual void setNullBit(int ordinal) = 0;
    virtual int getFieldOffset(int pos) = 0;


    // MemorySegment * segment_{};
    int cursor_{};
    
    uint8_t * memoryBuffer;

    //DataOutputViewStreamWrapper outputView;

};


#endif //FLINK_TNEL_ABSTRACTBINARYWRITER_H

#ifndef OMNIFLINK_LAZYBINARYFORMAT_H
#define OMNIFLINK_LAZYBINARYFORMAT_H

#include "BinarySection.h"
#include "../../../core/typeutils/TypeSerializer.h"

template <typename T>
class LazyBinaryFormat
{
protected:
    T *object;
    BinarySection *binarySection;
    bool materialized = false;

    // Materializer
    virtual BinarySection *materialize(TypeSerializer *serializer) = 0;

public:
    LazyBinaryFormat() {};
    LazyBinaryFormat(T *object) : object(object) {};
    // LazyBinaryFormat(MemorySegment *segments[], int offset, int sizeInBytes) : materialized(true)
    // {
    //     binarySection = new BinarySection(segments, 1, offset, sizeInBytes);
    //     object = new T();
    // };
    // LazyBinaryFormat(MemorySegment *segments[], int offset, int sizeInBytes, T *object) : object(object), materialized(true) { binarySection = new BinarySection(segments, 1, offset, sizeInBytes); };

    // todo  why we need to new A T here? 
    LazyBinaryFormat(uint8_t *bytes, int offset, int sizeInBytes) : materialized(true)
    {
        binarySection = new BinarySection(bytes, offset, sizeInBytes);
        object = new T();
    };

    // Getters
    T *getObject() { return object; };
    BinarySection *getBinarySection() { return binarySection; };
    // MemorySegment **getSegments()
    // {
    //     if (materialized == false)
    //     {
    //         // TODO
    //         // Change error type?
    //         THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized")
    //     }
    //     // Temp impl, should return all segments if we decide to split data up into multiple segments
    //     return (binarySection->segments_);
    // };
    int getOffset()
    {
        if (materialized == false)
        {
            // TODO
            // Change error type?
            THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized");
        }
        return binarySection->offset_;
    };
    int getSizeInBytes()
    {
        if (materialized == false)
        {
            // TODO
            // Change error type?
            THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized");
        }
        return binarySection->sizeInBytes_;
    };
    // int getNumSegments()
    // {
    //     if (materialized == false)
    //     {
    //         // TODO
    //         // Change error type?
    //         THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized")
    //     }
    //     return binarySection->numSegments_;
    // };

    // Materialization
    void ensureMaterialized();

    uint8_t *getSegment()
    {
        if (materialized == false)
        {
            // TODO
            // Change error type?
            THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized");
        }
        return binarySection->getSegment();
    };
};

#endif // OMNIFLINK_LAZYBINARYFORMAT_H

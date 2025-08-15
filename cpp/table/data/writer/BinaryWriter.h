//
// Created by root on 9/15/24.
//

#ifndef FLINK_TNEL_BINARYWRITER_H
#define FLINK_TNEL_BINARYWRITER_H


#include "../../types/logical/LogicalType.h"
#include "../../../core/typeutils/TypeSerializer.h"

class BinaryWriter {

public :
    virtual void writeLong(int pos, long value) = 0;
    virtual void reset() =0;
    virtual void setNullAt(int pos) = 0;
    virtual void complete() = 0;
    virtual void writeInt(int pos, int value) = 0;

    static void write(
            BinaryWriter*  writer,
            int pos,
            void  * object,
            LogicalType* type,
            TypeSerializer* serializer);
};


#endif //FLINK_TNEL_BINARYWRITER_H

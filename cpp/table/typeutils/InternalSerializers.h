//
// Created by root on 9/12/24.
//

#ifndef FLINK_TNEL_INTERNALSERIALIZERS_H
#define FLINK_TNEL_INTERNALSERIALIZERS_H

#include "../types/logical/LogicalType.h"
#include "../../core/typeutils/TypeSerializer.h"

class InternalSerializers {
public:
    static TypeSerializer *  create(LogicalType* type);

private:
    static TypeSerializer *  createInternal(LogicalType* type);

};



#endif //FLINK_TNEL_INTERNALSERIALIZERS_H

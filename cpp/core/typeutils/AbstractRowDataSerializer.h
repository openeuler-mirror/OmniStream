/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef ABSTRACTROWDATASERIALIZER_H
#define ABSTRACTROWDATASERIALIZER_H

#include "core/typeutils/PagedTypeSerializer.h"
#include "table/data/binary/BinaryRowData.h"

template <typename T>
class AbstractRowDataSerializer: public PagedTypeSerializer<T>{
public:
    virtual int getArity() = 0;
    virtual BinaryRowData *toBinaryRow(T rowdata) = 0;
};

#endif //ABSTRACTROWDATASERIALIZER_H

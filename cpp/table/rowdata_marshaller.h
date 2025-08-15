/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Row buffer Header
 */

#ifndef OMNISTREAM_ROWDATA_MARSHALLER_H
#define OMNISTREAM_ROWDATA_MARSHALLER_H
#include "table/data/binary/BinaryRowData.h"
#include "table/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/type/string_ref.h"
using namespace omniruntime;

using VBToRowSerializer = void (*)(vec::BaseVector *baseVector, int32_t rowIdx, BinaryRowData *result, int32_t pos);

using RowToVBDeSerializer = void (*)(vec::BaseVector *baseVector, int32_t rowIdx, BinaryRowData *input, int32_t pos);

extern std::vector<VBToRowSerializer> rowSerializerCenter;
extern std::vector<RowToVBDeSerializer> rowDeserializerCenter;


#endif // OMNISTREAM_ROWDATA_MARSHALLER_H

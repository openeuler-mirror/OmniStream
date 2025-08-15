/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

//
// Created by t00510106 on 2025/7/4.
//

#ifndef OMNISTREAM_VECTORBATCHUTILS_H
#define OMNISTREAM_VECTORBATCHUTILS_H


#include "vectorbatch/VectorBatch.h"

class VectorBatchUtils {
public:
    static void AppendLongVectorForInt64(omnistream::VectorBatch* outputVB, std::vector<RowData*> collectedRows,
                                         int numRowsPerVB, int colIndex);
    static void AppendLongVectorForDouble(omnistream::VectorBatch* outputVB, std::vector<RowData*> collectedRows,
                                          int numRowsPerVB, int colIndex);
    static void AppendIntVector(omnistream::VectorBatch* outputVB, std::vector<RowData*> collectedRows,
                                int numRowsPerVB, int colIndex);
    static void AppendIntVectorForBool(omnistream::VectorBatch* outputVB, std::vector<RowData*> collectedRows,
                                       int numRowsPerVB, int colIndex);
    static void AppendStringVector(omnistream::VectorBatch* outputVB, std::vector<RowData*> collectedRows,
                                   int numRowsPerVB, int colIndex);
};


#endif // OMNISTREAM_VECTORBATCHUTILS_H

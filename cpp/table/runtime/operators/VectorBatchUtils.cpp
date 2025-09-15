/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "VectorBatchUtils.h"

void VectorBatchUtils::AppendLongVectorForInt64(omnistream::VectorBatch *outputVB, std::vector<RowData *> collectedRows,
                                                int numRowsPerVB, int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<int64_t>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendLongVectorForDouble(omnistream::VectorBatch *outputVB, std::vector<RowData *> collectedRows,
                                                 int numRowsPerVB, int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<double>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendIntVector(omnistream::VectorBatch *outputVB, std::vector<RowData *> collectedRows,
                                       int numRowsPerVB, int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<int32_t>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendIntVectorForBool(omnistream::VectorBatch* outputVB, std::vector<RowData*> collectedRows,
                                              int numRowsPerVB, int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<bool>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendStringVector(omnistream::VectorBatch* outputVB, std::vector<RowData*> collectedRows,
                                          int numRowsPerVB, int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        std::string_view strView = collectedRows[rowIndex]->getStringView(colIndex);
        vector->SetValue(rowIndex, strView);
    }
    outputVB->Append(vector);
}
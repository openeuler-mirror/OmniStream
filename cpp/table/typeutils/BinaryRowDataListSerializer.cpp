/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by c00572813 on 2025/5/13.
//

#include "BinaryRowDataListSerializer.h"
#include "core/io/DataInputDeserializer.h"

void BinaryRowDataListSerializer::serialize(void *row, DataOutputSerializer &target)
{
    LOG(">>>>")
    std::vector<RowData*>* castedVectorRow = reinterpret_cast<std::vector<RowData*>*>(row);
    target.writeInt(castedVectorRow->size());
    for (auto &item: *castedVectorRow) {
        binaryRowDataSerializer->serialize(item, target);
    }
}

void* BinaryRowDataListSerializer::deserialize(DataInputView &source)
{
    int size = source.readInt();
    std::vector<RowData*>* result = new std::vector<RowData*>();
    for (int i = 0; i < size; ++i) {
        RowData* resPtr = reinterpret_cast<RowData*>(binaryRowDataSerializer->deserialize(source));
        result->push_back(resPtr->copy());
    }
    return result;
}

const char *BinaryRowDataListSerializer::getName() const
{
    return "BinaryRowDataListSerializer";
}

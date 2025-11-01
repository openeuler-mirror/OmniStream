/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "BinaryRowDataListSerializer.h"
#include "core/memory/DataInputDeserializer.h"

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

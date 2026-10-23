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

#include <stdexcept>

#include "core/memory/DataInputDeserializer.h"

BinaryRowDataListSerializer::BinaryRowDataListSerializer(int numFields, TypeSerializer* elementSerializer)
    : binaryRowDataSerializer(new BinaryRowDataSerializer(numFields)),
      elementSerializer(elementSerializer)
{
}

BinaryRowDataListSerializer::~BinaryRowDataListSerializer()
{
    delete binaryRowDataSerializer;
    delete elementSerializer;
}

std::string BinaryRowDataListSerializer::toJson()
{
    TypeSerializer* nested = elementSerializer != nullptr ? elementSerializer : binaryRowDataSerializer;
    SerializerJsonInfo typeJson = {SerializerType::LIST, "", nullptr, nested};
    return typeJson.toJson();
}

void BinaryRowDataListSerializer::serialize(void* row, DataOutputSerializer& target)
{
    LOG(">>>>");
    std::vector<RowData*>* castedVectorRow = reinterpret_cast<std::vector<RowData*>*>(row);
    target.writeInt(castedVectorRow->size());
    for (auto& item : *castedVectorRow) {
        binaryRowDataSerializer->serialize(item, target);
    }
}

void* BinaryRowDataListSerializer::deserialize(DataInputView& source)
{
    int size = source.readInt();
    std::vector<RowData*>* result = new std::vector<RowData*>();
    for (int i = 0; i < size; ++i) {
        RowData* resPtr = reinterpret_cast<RowData*>(binaryRowDataSerializer->deserialize(source));
        result->push_back(resPtr->copy());
    }
    return result;
}

void BinaryRowDataListSerializer::indexSerializedRows(
    const uint8_t* data, int length, std::vector<SerializedRowSpan>& spans)
{
    DataInputDeserializer input(data, length, 0);
    int count = input.readInt();
    if (count < 0) {
        throw std::runtime_error("BinaryRowDataListSerializer row count is negative");
    }
    spans.resize(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        int start = static_cast<int>(input.getPosition());
        int payload = input.readInt();
        if (payload < 0 || static_cast<int64_t>(input.getPosition()) + payload > length) {
            throw std::runtime_error("BinaryRowDataListSerializer row payload is out of range");
        }
        input.setPosition(input.getPosition() + static_cast<size_t>(payload));
        spans[i].offset = start;
        spans[i].bytes = static_cast<int>(input.getPosition()) - start;
    }
}

RowData* BinaryRowDataListSerializer::copySerializedRow(const uint8_t* data, const SerializedRowSpan& span)
{
    DataInputDeserializer input(data + span.offset, span.bytes, 0);
    auto* reused = reinterpret_cast<RowData*>(binaryRowDataSerializer->deserialize(input));
    return reused->copy();
}

void BinaryRowDataListSerializer::appendSerializedRow(RowData* row, DataOutputSerializer& target)
{
    binaryRowDataSerializer->serialize(row, target);
}

const char* BinaryRowDataListSerializer::getName() const
{
    return "BinaryRowDataListSerializer";
}

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

#include "RowDataSerializer.h"
#include "InternalSerializers.h"
#include "BinaryRowDataSerializer.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"


RowDataSerializer::RowDataSerializer(omnistream::RowType *rowType) :binarySerializer_(static_cast<int>(rowType->getChildren().size())), reuseRow_(nullptr), reuseWriter_(nullptr)
{
    auto types = rowType->getChildren();
    types_ = types;

    for (size_t i = 0; i<types.size(); i++) {
        LogicalType* fieldType = types_[i];
        TypeSerializer* serializer =  InternalSerializers::create(fieldType);
        fieldSerializers_.push_back(serializer);
        fieldGetters_.push_back(RowData::createFieldGetter(fieldType, i));
    }
}

RowDataSerializer::~RowDataSerializer()
{
    delete reuseRow_;
    delete reuseWriter_;
}

void *RowDataSerializer::deserialize(DataInputView &source)
{
    LOG(">>>>")
    return  binarySerializer_.deserialize(source);
}


void RowDataSerializer::serialize(void *rowData, DataOutputSerializer &target)
{
    LOG(">>>>")
    binarySerializer_.serialize(toBinaryRow(static_cast<RowData *>(rowData)), target);
}

BinaryRowData *RowDataSerializer::toBinaryRow(RowData *row)
{
    LOG(">>>>")

    if (row->getRowDataTypeId() == RowData::BinaryRowDataID) {
        LOG(">>>>")
        return static_cast<BinaryRowData *>(row);
    }

    if (reuseRow_ == nullptr) {
        reuseRow_ = new BinaryRowData(types_.size());
        reuseWriter_ = new BinaryRowWriter(reuseRow_);
    }

    reuseWriter_->reset();
    reuseWriter_->writeRowKind(row->getRowKind());
    for (size_t i = 0; i < types_.size(); i++) {
        if (row->isNullAt(i)) {
            reuseWriter_->setNullAt(i);
        } else {
            BinaryWriter::write(
                reuseWriter_, i, (fieldGetters_)[i]->getFieldOrNull(row), types_[i],
                (fieldSerializers_)[i]);
        }
    }
    reuseWriter_->complete();
    return reuseRow_;
}

const char *RowDataSerializer::getName() const
{
    return "RowDataSerializer";
}

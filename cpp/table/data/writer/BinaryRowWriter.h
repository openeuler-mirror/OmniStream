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

#ifndef FLINK_TNEL_BINARYROWWRITER_H
#define FLINK_TNEL_BINARYROWWRITER_H


#include "AbstractBinaryWriter.h"
#include "table/data/binary/BinaryRowData.h"

class BinaryRowWriter : public AbstractBinaryWriter {
public:
    explicit BinaryRowWriter(BinaryRowData *row);

    BinaryRowWriter(BinaryRowData *row,  int initialSize);
    ~BinaryRowWriter() override = default;

    // virtual
    void writeLong(int pos, long value) override;
    void writeInt(int pos, int value) override;

    void reset() override;

    void setNullAt(int pos) override;

    void complete() override;

    // non-virtual
    void writeRowKind(RowKind kind);

protected:
    int getFieldOffset(int pos) override;

protected:
    void setNullBit(int ordinal) override;

private:

    int nullBitsSizeInBytes_{};
    BinaryRowData* row_{};
    int fixedSize_{};
};


#endif

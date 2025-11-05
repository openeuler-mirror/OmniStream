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

#ifndef FLINK_TNEL_BINARYWRITER_H
#define FLINK_TNEL_BINARYWRITER_H


#include "../../types/logical/LogicalType.h"
#include "../../../core/typeutils/TypeSerializer.h"

class BinaryWriter {
public:
    virtual void writeLong(int pos, long value) = 0;
    virtual void reset() =0;
    virtual void setNullAt(int pos) = 0;
    virtual void complete() = 0;
    virtual void writeInt(int pos, int value) = 0;

    static void write(
            BinaryWriter*  writer,
            int pos,
            void* object,
            LogicalType* type,
            TypeSerializer* serializer);
};


#endif // FLINK_TNEL_BINARYWRITER_H

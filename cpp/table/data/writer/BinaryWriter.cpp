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

#include "BinaryWriter.h"
#include "table/data/TimestampData.h"

using namespace omniruntime::type;

void BinaryWriter::write(BinaryWriter *writer, int pos, void *object, LogicalType* type, TypeSerializer *serializer)
{
    switch (type->getTypeId()) {
        case DataTypeId::OMNI_LONG:
            writer->writeLong(pos, *(reinterpret_cast<long *>(object)));
            break;
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            writer->writeLong(pos, reinterpret_cast<TimestampData *>(object)->getMillisecond());
            break;
        default:
            THROW_LOGIC_EXCEPTION("Unknown type" + std::to_string(type->getTypeId()));
    }
}

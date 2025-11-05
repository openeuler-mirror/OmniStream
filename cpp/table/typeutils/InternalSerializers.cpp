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

#include "InternalSerializers.h"

#include "../../core/typeutils/LongSerializer.h"
#include "../types/logical/RowType.h"
#include "RowDataSerializer.h"
#include "StringDataSerializer.h"

using namespace omniruntime::type;

TypeSerializer *InternalSerializers::create(LogicalType* type)
{
    return createInternal(type);
}

TypeSerializer *InternalSerializers::createInternal(LogicalType* type)
{
    switch (type->getTypeId()) {
        case DataTypeId::OMNI_CONTAINER:
            return new RowDataSerializer(static_cast<omnistream::RowType *>(type));
        case DataTypeId::OMNI_LONG:
            return LongSerializer::INSTANCE; // `LongSerializer` is currently dummy, we use `RowDataSerializer`'s `serialize` and `deserialize` for now
        case DataTypeId::OMNI_INT:
            return LongSerializer::INSTANCE; // `LongSerializer` is currently dummy, we use `RowDataSerializer`'s `serialize` and `deserialize` for now
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_VARCHAR:
            return StringDataSerializer::INSTANCE;
        default:
            THROW_LOGIC_EXCEPTION("Unknown type" + std::to_string(type->getTypeId()));
    }
}

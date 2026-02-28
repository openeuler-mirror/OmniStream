/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * @Description: Tuple Type Info for DataStream
 */
#include "TupleTypeInfo.h"
#include "core/typeutils/TupleSerializer.h"

std::string TupleTypeInfo::name()
{
    std::stringstream ss;
    ss << "TupleTypeInfo: type id" << "tes";
    ss << "typeSerializer: " << typeSerializer->getName();
    return ss.str();
}

TypeSerializer *TupleTypeInfo::createTypeSerializer()
{
    typeSerializer = new Tuple2Serializer(types);
    return typeSerializer;
}

TupleTypeInfo *TupleTypeInfo::of(const json &type)
{
    auto *serializer = new Tuple2Serializer(const_cast<json &>(type));
    serializer->setSelfBufferReusable(true);
    return new TupleTypeInfo(serializer);
}

BackendDataType TupleTypeInfo::getBackendId() const
{
    return typeSerializer->getBackendId();
}

TypeSerializer *TupleTypeInfo::getTypeSerializer()
{
    return typeSerializer;
}

/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

TypeSerializer *TupleTypeInfo::createTypeSerializer(std::string)
{
    return typeSerializer;
}

TupleTypeInfo *TupleTypeInfo::of(const json &type)
{
    auto *serializer = new Tuple2Serializer(const_cast<json &>(type));
    return new TupleTypeInfo(serializer);
}

BackendDataType TupleTypeInfo::getBackendId() const
{
    return typeSerializer->getBackendId();
}

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/15/24.
//

#ifndef FLINK_TNEL_STRINGTYPEINFO_H
#define FLINK_TNEL_STRINGTYPEINFO_H

#include "../typeutils/TypeSerializer.h"
#include "TypeInformation.h"
#include "typeconstants.h"

class StringTypeInfo : public TypeInformation {
public:
    explicit StringTypeInfo(const char *name);

    TypeSerializer *createTypeSerializer(const std::string config) override;
    std::string name() override;

    BackendDataType getBackendId() const override { return BackendDataType::VARCHAR_BK; };

private:
    const char* name_ = TYPE_NAME_STRING;

    // u32string is std string for utf32 which is equivalent to java char utf32.
    const char* native_type = "std::u32string";
};


#endif //FLINK_TNEL_STRINGTYPEINFO_H

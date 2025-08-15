/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Long Type Info for DataStream
 */

#ifndef OMNISTREAM_LONGTYPEINFO_H
#define OMNISTREAM_LONGTYPEINFO_H

#include "../typeutils/TypeSerializer.h"
#include "TypeInformation.h"
#include "typeconstants.h"

class LongTypeInfo : public TypeInformation {
public:
    explicit LongTypeInfo(const char *name);

    TypeSerializer *createTypeSerializer(const std::string config) override;
    std::string name() override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::LONG_BK;
    };

private:
    const char* name_ = TYPE_NAME_LONG;
};


#endif  //OMNISTREAM_LONGTYPEINFO_H

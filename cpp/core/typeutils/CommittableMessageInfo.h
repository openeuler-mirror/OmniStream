/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_COMMITTABLEMESSAGEINFO_H
#define FLINK_TNEL_COMMITTABLEMESSAGEINFO_H

#include "../../core/typeinfo/TypeInformation.h"
#include "CommittableMessageSerializer.h"

class CommittableMessageInfo : public TypeInformation {
public:
    explicit CommittableMessageInfo()
    {
        typeSerializer = new CommittableMessageSerializer();
    };

    TypeSerializer *createTypeSerializer(const std::string config) override
    {
        return typeSerializer;
    };

    std::string name() override
    {
        return typeSerializer->getName();
    };

    BackendDataType getBackendId() const override
    {
        return typeSerializer->getBackendId();
    }

    ~CommittableMessageInfo() override = default;

private:
    TypeSerializer *typeSerializer;
};

#endif //FLINK_TNEL_COMMITTABLEMESSAGEINFO_H

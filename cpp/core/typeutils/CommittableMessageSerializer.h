/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_COMMITTABLEMESSAGESERIALIZER_H
#define FLINK_TNEL_COMMITTABLEMESSAGESERIALIZER_H

#include "../../core/typeutils/TypeSerializer.h"
class CommittableMessageSerializer :  public TypeSerializer {
    const char* getName() const override
    {
        return "CommittableMessageSerializer";
    }

    void* deserialize(DataInputView& source)
    {
        return nullptr;
    }
    void serialize(void * record, DataOutputSerializer& target) {
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::BIGINT_BK;
    };
};
#endif //FLINK_TNEL_COMMITTABLEMESSAGESERIALIZER_H

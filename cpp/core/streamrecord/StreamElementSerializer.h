/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/13/24.
//

#ifndef FLINK_TNEL_STREAMELEMENTSERIALIZER_H
#define FLINK_TNEL_STREAMELEMENTSERIALIZER_H

#include "../typeutils/TypeSerializer.h"
#include "../include/common.h"
#include "functions/StreamElement.h"
#include "StreamRecord.h"
#include "functions/Watermark.h"

namespace omnistream::datastream {
    class StreamElementSerializer : public TypeSerializer {
    public:
        explicit StreamElementSerializer(TypeSerializer *typeSerializer);

        ~StreamElementSerializer()
        {
            delete reUsableRecord_;
            delete reUsableWatermark_;
        }

        void *deserialize(DataInputView& source) override;
        void serialize(void * record, DataOutputSerializer& target) override {}

        void serialize(Object *record, DataOutputSerializer &target) override;

        const char *getName() const override;

        BackendDataType getBackendId() const override
        {
            return BackendDataType::BIGINT_BK;
        };

    private:
        TypeSerializer *typeSerializer_;
        StreamRecord *reUsableRecord_{};
        Watermark *reUsableWatermark_{};
    };
}
#endif  //FLINK_TNEL_STREAMELEMENTSERIALIZER_H

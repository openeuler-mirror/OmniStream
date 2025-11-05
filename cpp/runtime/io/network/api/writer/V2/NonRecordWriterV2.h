/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef NONRECORDWRITERV2_H
#define NONRECORDWRITERV2_H

#include "RecordWriterDelegateV2.h"

namespace omnistream {
    class NonRecordWriterV2 : public RecordWriterDelegateV2 {

    public:
        NonRecordWriterV2() = default;

        RecordWriterV2* getRecordWriter(int outputIndex) override
        {
            throw std::invalid_argument("NonRecordWriterV2 unsupport");
        }

        void cancel() override {}
        void close() override {}
        void broadcastEvent(std::shared_ptr<AbstractEvent> event) override {}
    };
}


#endif

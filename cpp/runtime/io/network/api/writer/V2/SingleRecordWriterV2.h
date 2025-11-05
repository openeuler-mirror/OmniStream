/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SINGLERECORDWRITER_V2_H
#define FLINK_TNEL_SINGLERECORDWRITER_V2_H

#include "../include/common.h"
#include "RecordWriterDelegateV2.h"
namespace omnistream {
    class SingleRecordWriterV2 : public RecordWriterDelegateV2 {
    public:
        explicit SingleRecordWriterV2(RecordWriterV2* recordWriter);

        RecordWriterV2* getRecordWriter(int outputIndex) override;

        ~SingleRecordWriterV2() override = default;

        void cancel() override;
        void close() override;

        void broadcastEvent(std::shared_ptr<AbstractEvent> event) override
        {
            recordWriter_->broadcastEvent(event);
        }
    private:
        RecordWriterV2* recordWriter_;
    };
}


#endif

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/15/24.
//

#ifndef FLINK_TNEL_SINGLERECORDWRITER_H
#define FLINK_TNEL_SINGLERECORDWRITER_H

#include "../include/common.h"
#include "RecordWriterDelegate.h"

namespace omnistream::datastream {
    class SingleRecordWriter : public RecordWriterDelegate {
    public:
        explicit SingleRecordWriter(RecordWriter* recordWriter);

        RecordWriter* getRecordWriter(int outputIndex) override;

        ~SingleRecordWriter() override = default;

    private:
        RecordWriter* recordWriter_;
    };
}
#endif //FLINK_TNEL_SINGLERECORDWRITER_H

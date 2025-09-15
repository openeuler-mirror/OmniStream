/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RECORDWRITERDELEGATE_H
#define FLINK_TNEL_RECORDWRITERDELEGATE_H

#include "RecordWriter.h"
namespace omnistream::datastream {
    class RecordWriterDelegate {
    public:
        virtual RecordWriter*  getRecordWriter(int outputIndex) = 0;

        virtual ~RecordWriterDelegate() = default;
    };
}
#endif  //FLINK_TNEL_RECORDWRITERDELEGATE_H

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SingleRecordWriter.h"
namespace omnistream::datastream {
    SingleRecordWriter::SingleRecordWriter(RecordWriter* recordWriter) : recordWriter_(recordWriter) {}

    RecordWriter* SingleRecordWriter::getRecordWriter(int outputIndex)
    {
        return recordWriter_;
    }
}


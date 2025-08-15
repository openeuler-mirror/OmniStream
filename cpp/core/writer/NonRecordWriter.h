/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/26/25.
//

#ifndef OMNISTREAM_NONRECORDWRITER_H
#define OMNISTREAM_NONRECORDWRITER_H

#include "RecordWriter.h"
#include "RecordWriterDelegate.h"

namespace omnistream::datastream {
    class NonRecordWriter : public RecordWriterDelegate {

    public:
        NonRecordWriter() = default;

        RecordWriter* getRecordWriter(int outputIndex) override
        {
            return nullptr;
        }
    };
}


#endif  //OMNISTREAM_NONRECORDWRITER_H

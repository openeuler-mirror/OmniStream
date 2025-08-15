/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/26/25.
//

#ifndef OMNISTREAM_MULTIRECORDWRITERS_H
#define OMNISTREAM_MULTIRECORDWRITERS_H

#include "RecordWriterDelegate.h"

namespace omnistream::datastream {
    class MultipleRecordWriters : public RecordWriterDelegate {
    public:
        explicit MultipleRecordWriters(std::vector<RecordWriter*>& recordWriters);

        RecordWriter* getRecordWriter(int outputIndex) override;

        ~MultipleRecordWriters() override = default;
        
    private:
        std::vector<RecordWriter*> recordWriters;
    };
}

#endif  //OMNISTREAM_MULTIRECORDWRITERS_H

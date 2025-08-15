/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/26/25.
//

#include "MultipleRecordWriters.h"
namespace omnistream::datastream {

    MultipleRecordWriters::MultipleRecordWriters(std::vector<RecordWriter*>& recordWriters) : recordWriters(recordWriters) {}


    RecordWriter* MultipleRecordWriters::getRecordWriter(int outputIndex)
    {
        return recordWriters[outputIndex];
    }
}
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/19/24.
//

#ifndef FLINK_TNEL_OUTPUTBUFFER_H
#define FLINK_TNEL_OUTPUTBUFFER_H

#include <cstdint>


struct OutputBufferStatus {
        // uint8_t raw[32]; // To access the raw bytes
        uintptr_t outputBuffer_;
        int32_t capacity_;
        int32_t outputSize; // byte size
        int32_t numberElement; // number of element_ of outputToOut
        int32_t ownership;  // 0 outputBuffer owned by java, 1 stands  owned by cpp native
        OutputBufferStatus() : outputBuffer_(0), capacity_(0), ownership(1) {}
};

#endif  //FLINK_TNEL_OUTPUTBUFFER_H

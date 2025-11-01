/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

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

#endif

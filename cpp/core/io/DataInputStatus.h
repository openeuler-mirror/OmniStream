/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/11/24.
//

#ifndef FLINK_TNEL_DATAINPUTSTATUS_H
#define FLINK_TNEL_DATAINPUTSTATUS_H


#include <cstdint>

// DataInputStatus is to provide the constant definition for OmniDataInputStatus which is an uint32_t;
//  lowest 8 bits:  uint_8 DataInputStatus
//  second lowest 8 bits: boolean isBufferConsumed
//  3rd lowest 8 bits: boolean isFullRecord
//  higest 8 bits: breakBatchEmitting


class OmniDataInputStatus {
public:
    // DataInputStatus see java org.apache.flink.streaming.runtime.io.DataInputStatus
    static constexpr uint8_t DataInputStatus_NOT_PROCESSED = 0;
    static constexpr uint8_t DataInputStatus_MORE_AVAILABLE = 1;
    static constexpr uint8_t DataInputStatus_NOTHING_AVAILABLE = 2;
    static constexpr uint8_t DataInputStatus_END_OF_RECOVERY = 3;
    static constexpr uint8_t DataInputStatus_STOPPED = 4;
    static constexpr uint8_t DataInputStatus_END_OF_DATA = 5;
    static constexpr uint8_t DataInputStatus_END_OF_INPUT = 6;

    // mask
    static constexpr uint32_t  MASK_DataInputStatus = 0xFF;
    static constexpr uint32_t  MASK_BUFFER_CONSUMED = 0xFF00;
    static constexpr uint32_t  MASK_FULL_RECORD = 0xFF0000;
    static constexpr uint32_t  MASK_BREAK_BATCH_EMITTING = 0xFF000000;

    static constexpr uint32_t BUFFER_CONSUMED_TRUE = 0x0100;
    static constexpr uint32_t FULL_RECORD_TRUE = 0x010000;
    static constexpr uint32_t BREAK_BATCH_EMITTING_TRUE = 0x01000000;
    static constexpr uint32_t AT_LEAST_ONE_FULL_RECORD_CONSUMED = 8;
};
 
enum class DataInputStatus : int {
    NOT_PROCESSED = -1,
    /**
     * Indicator that more data is available and the input can be called immediately again to
     * produce more data.
     */
    MORE_AVAILABLE = 0,

    /**
     * Indicator that no data is currently available, but more data will be available in the future
     * again.
     */
    NOTHING_AVAILABLE,

    /** Indicator that all persisted data of the data exchange has been successfully restored. */
    END_OF_RECOVERY,

    /** Indicator that the input was stopped because of stop-with-savepoint without drain. */
    STOPPED,

    /** Indicator that the input has reached the end_ of data. */
    END_OF_DATA,

    /**
     * Indicator that the input has reached the end_ of data and control events. The input is about
     * to close.
     */
    END_OF_INPUT
};

class DataInputStatusHelper {
public:
    static int mapToInt(const DataInputStatus& inputStatus)
    {
        switch (inputStatus) {
            case DataInputStatus::NOT_PROCESSED:
                return -1;
            case DataInputStatus::MORE_AVAILABLE:
                return 0;
            case DataInputStatus::NOTHING_AVAILABLE:
                return 1;
            case DataInputStatus::END_OF_RECOVERY:
                return 2;
            case DataInputStatus::STOPPED:
                return 3;
            case DataInputStatus::END_OF_DATA:
                return 4;
            case DataInputStatus::END_OF_INPUT:
                return 5;
        }
        return -2; // unknow
    }
};


#endif  //FLINK_TNEL_DATAINPUTSTATUS_H

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_RESULTPARTITIONTYPE_H
#define OMNISTREAM_RESULTPARTITIONTYPE_H

#include <stdexcept>

namespace omnistream {

class ResultPartitionType {
public:
    // Constants for bit masks
    static const int PIPELINED_MASK = 0x01;
    static const int BACK_PRESSURE_MASK = 0x02;
    static const int BOUNDED_MASK = 0x04;
    static const int PERSISTENT_MASK = 0x08;
    static const int RECONNECTABLE_MASK = 0x10;

    static const int PIPELINED;
    static const int PIPELINED_BOUNDED;
    static const int PIPELINED_APPROXIMATE;
    static const int BLOCKING_PERSISTENT;
    static const int BLOCKING;

    /**
     * Converts a set of boolean flags representing result partition type features into an integer representation.
     * Each boolean flag corresponds to a specific bit in the resulting integer.
     *
     * @param isPipelined     Indicates whether pipelining is enabled.
     * @param hasBackPressure Indicates whether back pressure is enabled.
     * @param isBounded       Indicates whether the partition is bounded.
     * @param isPersistent    Indicates whether the partition is persistent.
     * @param isReconnectable Indicates whether the partition is reconnectable.
     * @return An integer representing the combined state of the result partition type features.
     * Each bit corresponds to a specific flag (0: false, 1: true).
     */
    static int encode(bool isPipelined, bool hasBackPressure, bool isBounded, bool isPersistent, bool isReconnectable);
    static std::string getNameByType(int type);
};

} // namespace omnistream

#endif // OMNISTREAM_RESULTPARTITIONTYPE_H
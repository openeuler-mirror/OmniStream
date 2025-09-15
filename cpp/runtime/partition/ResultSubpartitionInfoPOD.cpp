/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ResultSubpartitionInfoPOD.h"
#include <sstream>

namespace omnistream {

    ResultSubpartitionInfoPOD::ResultSubpartitionInfoPOD() : partitionIdx(0), subPartitionIdx(0) {}

    ResultSubpartitionInfoPOD::ResultSubpartitionInfoPOD(int partitionIdx, int subPartitionIdx)
        : partitionIdx(partitionIdx), subPartitionIdx(subPartitionIdx) {}

    ResultSubpartitionInfoPOD::ResultSubpartitionInfoPOD(const ResultSubpartitionInfoPOD& other)
        : partitionIdx(other.partitionIdx), subPartitionIdx(other.subPartitionIdx) {}

    ResultSubpartitionInfoPOD::~ResultSubpartitionInfoPOD() {}

    int ResultSubpartitionInfoPOD::getPartitionIdx() const {
        return partitionIdx;
    }

    void ResultSubpartitionInfoPOD::setPartitionIdx(int partitionIdx) {
        this->partitionIdx = partitionIdx;
    }

    int ResultSubpartitionInfoPOD::getSubPartitionIdx() const {
        return subPartitionIdx;
    }

    void ResultSubpartitionInfoPOD::setSubPartitionIdx(int subPartitionIdx) {
        this->subPartitionIdx = subPartitionIdx;
    }

    bool ResultSubpartitionInfoPOD::operator==(const ResultSubpartitionInfoPOD& other) const {
        return partitionIdx == other.partitionIdx && subPartitionIdx == other.subPartitionIdx;
    }

    bool ResultSubpartitionInfoPOD::operator!=(const ResultSubpartitionInfoPOD& other) const {
        return !(*this == other);
    }

    std::string ResultSubpartitionInfoPOD::toString() const {
        std::stringstream ss;
        ss << "ResultSubpartitionInfoPOD{partitionIdx=" << partitionIdx
           << ", subPartitionIdx=" << subPartitionIdx << "}";
        return ss.str();
    }

} // namespace omnistream


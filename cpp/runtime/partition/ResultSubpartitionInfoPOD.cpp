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

#include "ResultSubpartitionInfoPOD.h"
#include <sstream>

namespace omnistream {

    ResultSubpartitionInfoPOD::ResultSubpartitionInfoPOD() : partitionIdx(0), subPartitionIdx(0) {}

    ResultSubpartitionInfoPOD::ResultSubpartitionInfoPOD(int partitionIdx, int subPartitionIdx)
        : partitionIdx(partitionIdx), subPartitionIdx(subPartitionIdx) {}

    ResultSubpartitionInfoPOD::ResultSubpartitionInfoPOD(const ResultSubpartitionInfoPOD& other)
        : partitionIdx(other.partitionIdx), subPartitionIdx(other.subPartitionIdx) {}

    ResultSubpartitionInfoPOD::~ResultSubpartitionInfoPOD() {}

    int ResultSubpartitionInfoPOD::getPartitionIdx() const
    {
        return partitionIdx;
    }

    void ResultSubpartitionInfoPOD::setPartitionIdx(int partitionIdx_)
    {
        this->partitionIdx = partitionIdx_;
    }

    int ResultSubpartitionInfoPOD::getSubPartitionIdx() const
    {
        return subPartitionIdx;
    }

    void ResultSubpartitionInfoPOD::setSubPartitionIdx(int subPartitionIdx_)
    {
        this->subPartitionIdx = subPartitionIdx_;
    }

    bool ResultSubpartitionInfoPOD::operator==(const ResultSubpartitionInfoPOD& other) const
    {
        return partitionIdx == other.partitionIdx && subPartitionIdx == other.subPartitionIdx;
    }

    bool ResultSubpartitionInfoPOD::operator!=(const ResultSubpartitionInfoPOD& other) const
    {
        return !(*this == other);
    }

    bool ResultSubpartitionInfoPOD::operator<(const ResultSubpartitionInfoPOD& other) const
    {
        if (partitionIdx != other.partitionIdx) {
            return partitionIdx < other.partitionIdx;
        }
        return subPartitionIdx < other.subPartitionIdx;
    }

    std::string ResultSubpartitionInfoPOD::toString() const
    {
        std::stringstream ss;
        ss << "ResultSubpartitionInfoPOD{partitionIdx=" << partitionIdx
           << ", subPartitionIdx=" << subPartitionIdx << "}";
        return ss.str();
    }

} // namespace omnistream


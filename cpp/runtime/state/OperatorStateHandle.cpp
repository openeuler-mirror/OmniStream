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
#include "OperatorStateHandle.h"

OperatorStateHandle::StateMetaInfo::StateMetaInfo(
    std::vector<long> offsets,
    OperatorStateHandle::Mode distributionMode)
    : offsets_(offsets),
      distributionMode_(distributionMode)
{
}

std::vector<long> OperatorStateHandle::StateMetaInfo::getOffsets()
{
    return offsets_;
}

OperatorStateHandle::Mode OperatorStateHandle::StateMetaInfo::getDistributionMode()
{
    return distributionMode_;
}

bool OperatorStateHandle::StateMetaInfo::operator==(const StateMetaInfo &other) const
{
    return this->distributionMode_ == other.distributionMode_
        && this->offsets_ == other.offsets_;
}

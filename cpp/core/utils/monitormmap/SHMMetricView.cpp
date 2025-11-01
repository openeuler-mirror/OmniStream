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

#include "SHMMetricView.h"

long int omnistream::SHMMetricView::GetThreadID()
{
    return this->valuePtr_[threadIDOffset];
}


long int omnistream::SHMMetricView::GetProbeID()
{
    return this->valuePtr_[probeIDOffset];
}


long int omnistream::SHMMetricView::GetMilliSeconds()
{
    return this->valuePtr_[millisecondsOffset];
}

long int omnistream::SHMMetricView::GetCount()
{
    return this->valuePtr_[countOffset];
}

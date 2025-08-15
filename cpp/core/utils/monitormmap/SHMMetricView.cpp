/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

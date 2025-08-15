/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "DescriptiveStatisticsHistogram.h"

namespace omnistream {
    DescriptiveStatisticsHistogram::DescriptiveStatisticsHistogram(int windowSize)
        : descriptiveStatistics(windowSize)
    {
    }

    void DescriptiveStatisticsHistogram::Update(long value)
    {
        descriptiveStatistics.AddValue(static_cast<double>(value));
    }

    long DescriptiveStatisticsHistogram::GetCount()
    {
        return descriptiveStatistics.GetElementsSeen();
    }

    HistogramStatistics* DescriptiveStatisticsHistogram::GetStatistics()
    {
        return new DescriptiveStatisticsHistogramStatistics(&descriptiveStatistics);
    }
}

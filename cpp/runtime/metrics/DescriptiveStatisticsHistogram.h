/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef DESCRIPTIVESTATISTICSHISTOGRAM_H
#define DESCRIPTIVESTATISTICSHISTOGRAM_H
#include "Histogram.h"
#include "CircularDoubleArray.h"
#include "DescriptiveStatisticsHistogramStatistics.h"
#include "HistogramStatistics.h"

namespace omnistream {
    class DescriptiveStatisticsHistogram : public Histogram {
    public:
        explicit DescriptiveStatisticsHistogram(int windowSize);
        void Update(long value) override;
        long GetCount() override;
        HistogramStatistics* GetStatistics() override;

    private:
        CircularDoubleArray descriptiveStatistics;
    };
}
#endif // DESCRIPTIVESTATISTICSHISTOGRAM_H

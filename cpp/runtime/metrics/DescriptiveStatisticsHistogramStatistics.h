/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef DESCRIPTIVESTATISTICSHISTOGRAMSTATISTICS_H
#define DESCRIPTIVESTATISTICSHISTOGRAMSTATISTICS_H
#include <vector>
#include <string>
#include "HistogramStatistics.h"
#include "CircularDoubleArray.h"

namespace omnistream {
    class DescriptiveStatisticsHistogramStatistics : public HistogramStatistics {
    public:
        explicit DescriptiveStatisticsHistogramStatistics(CircularDoubleArray* cda);
        ~DescriptiveStatisticsHistogramStatistics() override;
        double GetQuantile(double quantile) override;
        std::vector<long> GetValues() override;
        int Size() override;
        double GetMean() override;
        double GetStdDev() override;
        long GetMax() override;
        long GetMin() override;

    private:
        CircularDoubleArray* circularArray;
        std::vector<double> GetData() const; // Helper to retrieve current data
    };
} // namespace omnistream
#endif // DESCRIPTIVESTATISTICSHISTOGRAMSTATISTICS_H

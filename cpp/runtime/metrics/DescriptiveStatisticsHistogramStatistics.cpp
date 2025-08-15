/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "DescriptiveStatisticsHistogramStatistics.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <stdexcept>
#include <sstream>

namespace omnistream {
    DescriptiveStatisticsHistogramStatistics::DescriptiveStatisticsHistogramStatistics(CircularDoubleArray* cda)
        : circularArray(cda)
    {
    }

    DescriptiveStatisticsHistogramStatistics::~DescriptiveStatisticsHistogramStatistics()
    {
    }

    std::vector<double> DescriptiveStatisticsHistogramStatistics::GetData() const
    {
        return circularArray->ToUnsortedArray();
    }

    int DescriptiveStatisticsHistogramStatistics::Size()
    {
        return circularArray->GetSize();
    }

    double DescriptiveStatisticsHistogramStatistics::GetMean()
    {
        auto data = GetData();
        if (data.empty()) { return 0.0; }
        double sum = std::accumulate(data.begin(), data.end(), 0.0);
        return sum / data.size();
    }

    double DescriptiveStatisticsHistogramStatistics::GetStdDev()
    {
        auto data = GetData();
        int n = data.size();
        if (n == 0) { return 0.0; }
        double mean = GetMean();
        double sqSum = std::inner_product(data.begin(), data.end(), data.begin(), 0.0,
                                          std::plus<double>(),
                                          [mean](double a, double b) { return (a - mean) * (b - mean); });
        return std::sqrt(sqSum / n);
    }

    long DescriptiveStatisticsHistogramStatistics::GetMax()
    {
        auto data = GetData();
        if (data.empty()) { return 0; }
        return static_cast<long>(*std::max_element(data.begin(), data.end()));
    }

    long DescriptiveStatisticsHistogramStatistics::GetMin()
    {
        auto data = GetData();
        if (data.empty()) { return 0; }
        return static_cast<long>(*std::min_element(data.begin(), data.end()));
    }

    std::vector<long> DescriptiveStatisticsHistogramStatistics::GetValues()
    {
        auto data = GetData();
        std::vector<long> values;
        for (auto d : data) {
            values.push_back(static_cast<long>(d));
        }
        return values;
    }

    double DescriptiveStatisticsHistogramStatistics::GetQuantile(double quantile)
    {
        auto data = GetData();
        if (data.empty()) {
            throw std::runtime_error("No data available");
        }
        std::sort(data.begin(), data.end());
        double pos = quantile * (data.size() - 1);
        size_t idx = static_cast<size_t>(pos);
        if (idx >= data.size() - 1) { return data.back(); }
        double fraction = pos - idx;
        return data[idx] * (1.0 - fraction) + data[idx + 1] * fraction;
    }
} // namespace omnistream

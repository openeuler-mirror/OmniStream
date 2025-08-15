/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef HISTOGRAMSTATISTICS_H
#define HISTOGRAMSTATISTICS_H
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <stdexcept>

namespace omnistream {
    class HistogramStatistics {
    public:
        HistogramStatistics()
        {
        }

        virtual ~HistogramStatistics()
        {
        }

        virtual double GetQuantile(double var1) = 0;
        virtual std::vector<long> GetValues() = 0;
        virtual int Size() = 0;
        virtual double GetMean() = 0;
        virtual double GetStdDev() = 0;
        virtual long GetMax() = 0;
        virtual long GetMin() = 0;

        virtual std::string ToString()
        {
            std::ostringstream oss;
            oss << "{";
            oss << "\"mean\": " << GetMean() << ", ";
            oss << "\"stdDev\": " << GetStdDev() << ", ";
            oss << "\"max\": " << GetMax() << ", ";
            oss << "\"min\": " << GetMin() << ", ";
            oss << "\"size\": " << Size() << ", ";
            oss << "\"values\": [";
            const auto& values = GetValues();
            for (size_t i = 0; i < values.size(); ++i) {
                oss << values[i];
                if (i < values.size() - 1) {
                    oss << ", ";
                }
            }
            oss << "]";
            oss << "}";
            return oss.str();
        }
    };
}
#endif // HISTOGRAMSTATISTICS_H

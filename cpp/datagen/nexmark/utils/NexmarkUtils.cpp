/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "NexmarkUtils.h"
std::vector<long> NexmarkUtils::RateShape::interEventDelayUs(int firstRate, int nextRate,
                                                             const RateUnit &unit, int numGenerators) const
{
    if (firstRate == nextRate) {
        std::vector<long> interEventDelayUs(1);
        interEventDelayUs[0] = unit.rateToPeriodUs(firstRate) * numGenerators;
        return interEventDelayUs;
    }
    switch (type) {
        case SQUARE: {
            std::vector<long> interEventDelayUs(2);
            interEventDelayUs[0] = unit.rateToPeriodUs(firstRate) * numGenerators;
            interEventDelayUs[1] = unit.rateToPeriodUs(nextRate) * numGenerators;
            return interEventDelayUs;
        }
        case SINE: {
            double mid = (firstRate + nextRate) / 2.0;
            double amp = (firstRate - nextRate) / 2.0; // may be -ve
            std::vector<long> interEventDelayUs(N);
            for (int i = 0; i < N; i++) {
                double r = (2.0 * M_PI * i) / N;
                double rate = mid + amp * std::cos(r);
                long rounded = static_cast<long>(std::round(rate));
                interEventDelayUs[i] = unit.rateToPeriodUs(rounded) * numGenerators;
            }
            return interEventDelayUs;
        }
    }
    throw std::runtime_error("Switch should be exhaustive");
}


// Definition of static members of RateUnit.
const NexmarkUtils::RateUnit NexmarkUtils::RateUnit::PER_SECOND(1000000L);
const NexmarkUtils::RateUnit NexmarkUtils::RateUnit::PER_MINUTE(60000000L);

// Definition of static instances of RateShape.
const NexmarkUtils::RateShape NexmarkUtils::RateShape::SQUARE_SHAPE(NexmarkUtils::RateShape::SQUARE);
const NexmarkUtils::RateShape NexmarkUtils::RateShape::SINE_SHAPE(NexmarkUtils::RateShape::SINE);
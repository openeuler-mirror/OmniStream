/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_NEXMARKUTILS_H
#define OMNISTREAM_NEXMARKUTILS_H


#include <iostream>
#include <string>
#include <stdexcept>
#include <vector>
#include <cmath>

class NexmarkUtils {
public:
    class RateUnit {
    public:
        // Public method: Number of microseconds between events at given rate.
        long rateToPeriodUs(long rate) const
        {
            return (usPerUnit + rate / 2) / rate;
        }

        // Static members for RateUnit.
        static const RateUnit PER_SECOND;
        static const RateUnit PER_MINUTE;

    private:
        // Number of microseconds per unit.
        long usPerUnit;

        // Private constructor to initialize RateUnit.
        explicit RateUnit(long usPerUnit) : usPerUnit(usPerUnit) {}

        // Grant NexmarkUtils access if needed.
        friend class NexmarkUtils;
    };

    //
    // Shape of event rate.
    //
    class RateShape {
    public:
        // Return inter-event delay, in microseconds, for each generator to follow in order to achieve
        // rate at unit using numGenerators.
        long interEventDelayUs(int rate, const RateUnit &unit, int numGenerators) const
        {
            return unit.rateToPeriodUs(rate) * numGenerators;
        }

        // Return array of successive inter-event delays, in microseconds, for each generator to follow
        // in order to achieve this shape with firstRate/nextRate at unit using numGenerators.
        std::vector<long>
        interEventDelayUs(int firstRate, int nextRate, const RateUnit &unit, int numGenerators) const;

        // Return delay between steps, in seconds, for result of interEventDelayUs, so as to
        // cycle through the entire sequence every ratePeriodSec.
        int stepLengthSec(int ratePeriodSec) const
        {
            int n = 0;
            switch (type) {
                case SQUARE:
                    n = 2;
                    break;
                case SINE:
                    n = N;
                    break;
            }
            return (ratePeriodSec + n - 1) / n;
        }

        // Public enum values for RateShape.
        enum Type {
            SQUARE,
            SINE
        };

        // Static instances for the shapes.
        static const RateShape SQUARE_SHAPE;
        static const RateShape SINE_SHAPE;

    public:
        Type type;
        // Number of steps used to approximate sine wave.
        static const int N = 10;

        // Private constructor.
        RateShape(Type type) : type(type) {}
    };
};

#endif // OMNISTREAM_NEXMARKUTILS_H

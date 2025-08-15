/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef CIRCULARDOUBLEARRAY_H
#define CIRCULARDOUBLEARRAY_H
#include <mutex>
#include <vector>

namespace omnistream {
    class CircularDoubleArray {
    public:
        explicit CircularDoubleArray(int windowSize);
        void AddValue(double value);
        std::vector<double> ToUnsortedArray();
        int GetSize();
        long GetElementsSeen();

    private:
        std::vector<double> backingArray;
        int nextPos;
        bool fullSize;
        long elementsSeen;
        std::mutex mtx;
    };
} // namespace omnistream
#endif // CIRCULARDOUBLEARRAY_H

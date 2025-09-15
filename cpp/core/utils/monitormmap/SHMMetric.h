/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef SHMMETRIC_H
#define SHMMETRIC_H

namespace omnistream {

    class SHMMetric {
    public:
        SHMMetric(long int threadID, long int* valuePtr, long int probeID, long int count)
            : threadID_(threadID),
              valuePtr_(valuePtr),
              probeID_(probeID),
              count_(count)
        {
            valuePtr[threadIDOffset] = threadID_;
            valuePtr[probeIDOffset] = probeID_;
        }

        void SetCount(long int count)
        {
            count_ = count;
        }

        void UpdateMetric(long count);

    private:
        const int threadIDOffset = 0;
        const int probeIDOffset = 1;
        const int millisecondsOffset = 2;
        const int countOffset = 3;

        long int threadID_;
        long int *valuePtr_;
        long int probeID_;
        long int count_;
    };
}


#endif // SHMMETRIC_H

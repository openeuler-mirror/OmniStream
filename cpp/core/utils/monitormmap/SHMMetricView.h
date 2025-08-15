/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/21/25.
//

#ifndef SHMMETRICVIEW_H
#define SHMMETRICVIEW_H


namespace omnistream {
    class SHMMetricView {
    public:
        explicit SHMMetricView(long int* valuePtr)
            : valuePtr_(valuePtr)
        {
        }

        ~SHMMetricView();

        long int GetThreadID();
        long int GetProbeID();
        long int GetMilliSeconds();
        long int GetCount();

    private:
        const int threadIDOffset = 0;
        const int probeIDOffset = 1;
        const int millisecondsOffset = 2;
        const int countOffset = 3;
        long int *valuePtr_;
    };
}

#endif // SHMMETRICVIEW_H
